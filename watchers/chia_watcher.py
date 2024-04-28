import asyncio
import aiohttp
from typing import List
from config import Network
from sqlalchemy import and_
from chia.types.coin_record import CoinRecord
from chia.types.blockchain_format.coin import Coin
from chia.consensus.block_record import BlockRecord
from chia.types.condition_opcodes import ConditionOpcode
from chia.types.blockchain_format.program import Program
from chia.rpc.full_node_rpc_client import FullNodeRpcClient
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.blockchain_format.program import INFINITE_COST
from chia.util.condition_tools import conditions_dict_for_solution
from db import join_message_contents, Message, MessageStatus, setup_database

class HTTPFullNodeRpcClient(FullNodeRpcClient):
    def __init__(self, base_url: str):
        super().__init__(None, None, None, None, None, None)
        self.session = aiohttp.ClientSession()
        self.closing_task = None
        self.base_url = base_url

    async def fetch(self, path, request_json):
        async with self.session.post(f"{self.base_url}/{path}", json=request_json) as response:
            response.raise_for_status()

            res_json = await response.json()
            if not res_json["success"]:
                raise ValueError(res_json)
            return res_json

    def close(self):
        self.closing_task = self.session.close()

    async def await_closed(self):
        await self.closing_task
        

class ChiaWatcher:
    network_id: str
    rpc_url: str
    portal_launcher_id: bytes32
    bridging_puzzle_hash: bytes32
    min_height: int
    message_toll: int
    tasks: list
    nodes: List[HTTPFullNodeRpcClient]

    def __init__(self, network: Network):
        self.network_id = network.id
        self.rpc_url = network.rpc_url
        self.min_height = network.min_height
        self.message_toll = network.message_toll
        self.portal_launcher_id = bytes.fromhex(network.portal_launcher_id)
        self.bridging_puzzle_hash = bytes.fromhex(network.bridging_puzzle_hash)
        self.tasks = []
        self.nodes = []

    def log(self, message):
        print(f"[{self.network_id} watcher] {message}")

    def getNode(self):
        node = HTTPFullNodeRpcClient(self.rpc_url)
        self.nodes.append(node)
        return node
    
    def getDb(self):
        return setup_database()

    def createMessageFromMemo(
            self,
            db,
            nonce: bytes,
            source: bytes,
            created_height: int,
            created_timestamp: int,
            memo: Program
    ):
        try:
            destination_chain = memo.first().as_atom()
            destination = memo.rest().first().as_atom()
            contents_unparsed = memo.rest().rest()
        except:
            self.log(f"Coin {self.network_id}-{nonce.hex()}: error when parsing memo; skipping")
            return
        
        contents = []
        for content in contents_unparsed.as_iter():
            c = content.as_atom()
            if len(c) < 32:
                c = b'\x00' * (32 - len(c)) + c
            if len(c) > 32:
                c = c[:32]
            contents.append(c)
        
        msg_in_db = db.query(Message).filter(and_(
            Message.nonce == nonce,
            Message.source_chain == self.network_id.encode()
        )).first()
        if msg_in_db is not None:
            self.log(f"Coin {self.network_id}-{nonce.hex()}: message already in db; skipping")
            return
        
        msg = Message(
            nonce=nonce,
            source_chain=self.network_id.encode(),
            source=source,
            destination_chain=destination_chain,
            destination=destination,
            contents=join_message_contents(contents),
            block_number=created_height,
            timestamp = created_timestamp,
            transaction_hash = nonce,
            status = MessageStatus.SENT
        )
        db.add(msg)
        db.commit()
        self.log(f"Message {self.network_id}-{nonce.hex()} added to db.")


    async def processCoinRecord(self, db, node: FullNodeRpcClient, coin_record: CoinRecord):
        parent_record = await node.get_coin_record_by_name(coin_record.coin.parent_coin_info)
        parent_spend = await node.get_puzzle_and_solution(
            coin_record.coin.parent_coin_info,
            parent_record.spent_block_index
        )

        try:
            _, output = parent_spend.puzzle_reveal.run_with_cost(INFINITE_COST, parent_spend.solution)
            for condition in output.as_iter():
                if condition.first().as_int() == 51: # CREATE_COIN
                    created_ph = condition.at('rf').as_atom()
                    created_amount = condition.at('rrf').as_int()

                    if created_ph == self.bridging_puzzle_hash and created_amount >= self.message_toll:
                        coin = Coin(parent_record.coin.name(), created_ph, created_amount)
                        try:
                            memo = condition.at('rrrf')
                        except:
                            self.log(f"ERROR! - Coin {self.network_id}-{coin.name().hex()} - error when parsing memo; skipping")
                            continue

                        try:
                            self.createMessageFromMemo(
                                db,
                                coin.name(),
                                parent_record.coin.puzzle_hash,
                                parent_record.spent_block_index,
                                parent_record.timestamp,
                                memo
                            )
                        except Exception as e:
                            self.log(f"ERROR - Coin {self.network_id}-{coin.name().hex()} - error when parsing memo to create message; skipping even though we shouldn't")
                            print(e)
        except Exception:
            self.log(f"ERROR - Coin {self.network_id}-{coin_record.coin.name().hex()} - error when parsing output; skipping")


    async def get_current_height(self, node: FullNodeRpcClient) -> int:
        try:
            return (await node.get_blockchain_state())["peak"].height
        except Exception as e:
            self.log(f"Error getting current height; sleeping 5s and trying again")
            await asyncio.sleep(5)
            return await self.get_current_height(node)



    async def sentMessageWatcher(self):
        node = self.getNode()
        db = self.getDb()

        while True:
            last_synced_height = db.query(Message.block_number).filter(
                Message.source_chain == self.network_id.encode()
            ).order_by(Message.block_number.desc()).first()

            if last_synced_height is None:
                last_synced_height = self.min_height
            else:
                last_synced_height = last_synced_height[0]

            unfiltered_coin_records = await node.get_coin_records_by_puzzle_hash(
                self.bridging_puzzle_hash,
                include_spent_coins=True,
                start_height=last_synced_height - 1
            )
            if unfiltered_coin_records is None:
                self.log("No new coin records; checking again in 30s...")
                await asyncio.sleep(30)
                continue

            # because get_coin_records_by_puzzle_hash can be quite resource exensive, we'll process all results
            # instead of only one and calling again
            skip_coin_ids = []
            reorg = False
            while not reorg:
                earliest_unprocessed_coin_record = None
                for coin_record in unfiltered_coin_records:
                    nonce = coin_record.coin.name()
                    if nonce in skip_coin_ids:
                        continue

                    if coin_record.coin.amount < self.message_toll:
                        skip_coin_ids.append(nonce)
                        continue

                    message_in_db = db.query(Message).filter(and_(
                        Message.nonce == nonce,
                        Message.source_chain == self.network_id.encode()
                    )).first()
                    if message_in_db is not None:
                        skip_coin_ids.append(nonce)
                        continue

                    if earliest_unprocessed_coin_record is None or coin_record.confirmed_block_index < earliest_unprocessed_coin_record.confirmed_block_index:
                        earliest_unprocessed_coin_record = coin_record

                if earliest_unprocessed_coin_record is None:
                    break

                # wait for this to actually be confirmed :)
                # 4 blocks for explorer
                while earliest_unprocessed_coin_record.confirmed_block_index + 4 > (await self.get_current_height(node)):
                    await asyncio.sleep(5)

                coin_record_copy = await node.get_coin_record_by_name(earliest_unprocessed_coin_record.coin.name())
                if coin_record_copy is None or coin_record_copy.confirmed_block_index != earliest_unprocessed_coin_record.confirmed_block_index:
                    self.log(f"Coin {self.network_id}-0x{earliest_unprocessed_coin_record.coin.name().hex()}: possible reorg; re-processing")
                    reorg = True
                    break

                await self.processCoinRecord(db, node, earliest_unprocessed_coin_record)

                skip_coin_ids.append(nonce)

            if not reorg:
                self.log("Processed all coin records; checking again in 30s...")
                await asyncio.sleep(30)

            
    async def receivedMessageWatcher(self):
        node = self.getNode()
        while True:
            await asyncio.sleep(5)

    def start(self, loop):
      self.log("Starting...")

      self.tasks.append(
          loop.create_task(self.sentMessageWatcher())
      )
      self.tasks.append(
          loop.create_task(self.receivedMessageWatcher())
      )

    def stop(self):
        self.log(f"Stopping...")

        for node in self.nodes:
            node.close()

    async def await_stopped(self):
        for node in self.nodes:
            await node.await_closed()
    
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks)

        self.tasks = []
        self.nodes = []
