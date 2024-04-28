import asyncio
import aiohttp
import logging
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
from db import join_message_contents, Message, MessageStatus, setup_database, ChiaPortalState

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
        logging.info(f"[{self.network_id} watcher] - {message}")

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
            source_block_number=created_height,
            source_timestamp = created_timestamp,
            destination_block_number = None,
            destination_timestamp = None,
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
                    else:
                        self.log(f"Coin {self.network_id}-{coin_record.coin.name().hex()}: toll not high enough; skipping")
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
            last_synced_height = db.query(Message.source_block_number).filter(
                Message.source_chain == self.network_id.encode()
            ).order_by(Message.source_block_number.desc()).first()

            if last_synced_height is None:
                last_synced_height = self.min_height
            else:
                last_synced_height = last_synced_height[0] + 1

            unfiltered_coin_records = await node.get_coin_records_by_puzzle_hash(
                self.bridging_puzzle_hash,
                include_spent_coins=True,
                start_height=last_synced_height
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
                        self.log(f"Skipping nonce {nonce.hex()} because toll is not high enough")
                        skip_coin_ids.append(nonce)
                        continue

                    message_in_db = db.query(Message).filter(and_(
                        Message.nonce == nonce,
                        Message.source_chain == self.network_id.encode()
                    )).first()
                    if message_in_db is not None:
                        self.log(f"Nonce {nonce.hex()} already in db; skipping")
                        skip_coin_ids.append(nonce)
                        continue

                    if earliest_unprocessed_coin_record is None or coin_record.confirmed_block_index < earliest_unprocessed_coin_record.confirmed_block_index:
                        earliest_unprocessed_coin_record = coin_record

                if earliest_unprocessed_coin_record is None:
                    break

                # wait for this to actually be confirmed :)
                # 4 blocks for explorer
                while earliest_unprocessed_coin_record.confirmed_block_index + 3 > (await self.get_current_height(node)):
                    self.log(f"Coin {self.network_id}-0x{earliest_unprocessed_coin_record.coin.name().hex()}: does not yet have 3 confs; retrying in 15s...")
                    await asyncio.sleep(15)

                coin_record_copy = await node.get_coin_record_by_name(earliest_unprocessed_coin_record.coin.name())
                if coin_record_copy is None or coin_record_copy.confirmed_block_index != earliest_unprocessed_coin_record.confirmed_block_index:
                    self.log(f"Coin {self.network_id}-0x{earliest_unprocessed_coin_record.coin.name().hex()}: possible reorg; re-processing")
                    reorg = True
                    break

                self.log(f"Processing coin record for nonce {earliest_unprocessed_coin_record.coin.name().hex()}...")
                await self.processCoinRecord(db, node, earliest_unprocessed_coin_record)

                nonce = earliest_unprocessed_coin_record.coin.name()
                self.log(f"Marking {nonce.hex()} as processed")
                skip_coin_ids.append(nonce)

            if not reorg:
                self.log("Processed all coin records; checking again in 30s...")
                await asyncio.sleep(30)

    async def syncPortal(
        self,
        db,
        node: FullNodeRpcClient,
        last_synced_portal: ChiaPortalState
    ) -> ChiaPortalState:
        coin_record = await node.get_coin_record_by_name(last_synced_portal.coin_id)
        if coin_record is None:
            for i in range(6):
                logging.info(f"Coin {self.network_id}-0x{last_synced_portal.coin_id.hex()}: not found; will remove from db if not found in {6-i} tries")
                coin_record = await node.get_coin_record_by_name(last_synced_portal.coin_id)
                await asyncio.sleep(10)

            db.delete(last_synced_portal)
            new_last_synced_portal = db.query(ChiaPortalState).filter(
                ChiaPortalState.chain_id == self.network_id.encode()
            ).order_by(ChiaPortalState.height.desc()).first()
            return new_last_synced_portal

        if coin_record.spent_block_index == 0:
            parent_coin_record = await node.get_coin_record_by_name(last_synced_portal.parent_id)
            if parent_coin_record.spent_block_index == 0:
                self.log(f"Portal coin {self.network_id}-0x{last_synced_portal.coin_id.hex()}: parent is unspent; reverting.")
                parent_state = db.query(ChiaPortalState).filter(
                    ChiaPortalState.coin_id == last_synced_portal.parent_id
                ).first()
                db.delete(last_synced_portal)
                db.commit()
                return parent_state
            
            self.log("Portal coin state is up to date.")
            await asyncio.sleep(10)
            return last_synced_portal

        self.log("Parsing new portal coin spend")
        # spent!
        spend = await node.get_puzzle_and_solution(last_synced_portal.coin_id, coin_record.spent_block_index)
        while spend is None:
            spend = await node.get_puzzle_and_solution(last_synced_portal.coin_id, coin_record.spent_block_index)
            
        conds = conditions_dict_for_solution(spend.puzzle_reveal, spend.solution, INFINITE_COST)
        create_coins = conds[ConditionOpcode.CREATE_COIN]
        new_ph = None
        for cond in create_coins:
            if cond.vars[1] == b'\x01':
                new_ph = cond.vars[0]
                break
        if new_ph is None:
            self.log(f"Portal coin {self.network_id}-0x{last_synced_portal.coin_id.hex()}: no singleton found in spend; reverting.")
            parent_state = db.query(ChiaPortalState).filter(
                ChiaPortalState.coin_id == last_synced_portal.parent_id
            ).first()
            db.delete(last_synced_portal)
            db.commit()
            return parent_state

        inner_solution: Program = Program.from_bytes(bytes(spend.solution)).at("rrf")
        update_package = inner_solution.at("f")

        chains_and_nonces = inner_solution.at("rf").as_iter() if bytes(update_package) == bytes(Program.to(0)) else []
        for cn in chains_and_nonces:
            source_chain = cn.first().as_atom()
            nonce = cn.rest().as_atom()

            msg = db.query(Message).filter(and_(
                Message.source_chain == source_chain,
                Message.nonce == nonce,
                Message.destination_chain == self.network_id.encode()
            )).first()
            while msg is None:
                self.log(f"Message {source_chain.decode()}-{nonce.hex()} not found in db; waiting 10s for other threads to catch up")
                await asyncio.sleep(10)
                msg = db.query(Message).filter(and_(
                    Message.source_chain == source_chain,
                    Message.nonce == nonce,
                    Message.destination_chain == self.network_id.encode()
                )).first()
            
            self.log(f"Updating status of message {source_chain.decode()}-{nonce.hex()} to 'RECEIVED'")
            msg.destination_block_number = coin_record.spent_block_index
            msg.destination_timestamp = coin_record.timestamp
            msg.status = MessageStatus.RECEIVED
            db.commit()
       
        new_singleton = Coin(
            last_synced_portal.coin_id,
            new_ph,
            1
        )

        new_synced_portal = ChiaPortalState(
            chain_id=self.network_id.encode(),
            coin_id=new_singleton.name(),
            parent_id=new_singleton.parent_coin_info,
            height=coin_record.spent_block_index,
        )
        db.add(new_synced_portal)
        db.commit()

        self.log(f"New portal coin: {self.network_id}-0x{new_synced_portal.coin_id.hex()}")

        return new_synced_portal


    async def receivedMessageWatcher(self):
        node = self.getNode()
        db = self.getDb()

        last_synced_portal = db.query(ChiaPortalState).filter(
            ChiaPortalState.chain_id == self.network_id.encode()
        ).order_by(ChiaPortalState.height.desc()).first()

        if last_synced_portal is None:
            self.log("No last synced portal found, using launcher...")
            launcher_coin_record = await node.get_coin_record_by_name(self.portal_launcher_id)
            assert launcher_coin_record.spent_block_index > 0

            launcher_spend = await node.get_puzzle_and_solution(self.portal_launcher_id, launcher_coin_record.spent_block_index)
            conds = conditions_dict_for_solution(launcher_spend.puzzle_reveal, launcher_spend.solution, INFINITE_COST)
            create_coins = conds[ConditionOpcode.CREATE_COIN]
            assert len(create_coins) == 1 and create_coins[0].vars[1] == b'\x01'

            singleton_full_puzzle_hash = create_coins[0].vars[0]
            first_singleton = Coin(
                self.portal_launcher_id,
                singleton_full_puzzle_hash,
                1
            )

            last_synced_portal = ChiaPortalState(
                chain_id=self.network_id.encode(),
                coin_id=first_singleton.name(),
                parent_id=self.portal_launcher_id,
                height=launcher_coin_record.spent_block_index,
            )
            db.add(last_synced_portal)
            db.commit()

        self.log(f"Latest portal coin: {self.network_id}-0x{last_synced_portal.coin_id.hex()}")

        while True:
            last_synced_portal = await self.syncPortal(db, node, last_synced_portal)


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
