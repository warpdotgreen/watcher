import asyncio
from web3 import Web3
from typing import Tuple
from config import Network
from sqlalchemy import and_
from db import setup_database, Message, MessageStatus, join_message_contents

class EVMWatcher:
    network_id: str
    rpc_url: str
    min_height: int
    max_block_range: int
    portal_address: str
    tasks: list

    def __init__(self, network: Network):
      self.network_id = network.id
      self.rpc_url = network.rpc_url
      self.min_height = network.min_height
      self.max_block_range = network.max_block_range
      self.portal_address = network.portal_address
      self.tasks = []

    def getWeb3(self):
        return Web3(Web3.HTTPProvider(self.rpc_url))
    
    def getDb(self):
        return setup_database()
    
    def log(self, message):
        print(f"[{self.network_id} watcher] {message}")
    
    def nonceIntToBytes(self, nonceInt: int) -> bytes:
      s = hex(nonceInt)[2:]
      return (64 - len(s)) * "0" + s


    def syncSentMessageEvents(self, web3, db, contract, nonce: int, start_height: int) -> Tuple[int, int] | None: # new nonce, new start height
        query_start_height = start_height
        query_end_height = query_start_height + self.max_block_range - 1

        time_to_stop = False
        nonce_hex = "0x" + self.nonceIntToBytes(nonce)
        logs = []
        while not time_to_stop:
            current_block_height = web3.eth.block_number
            if query_end_height > current_block_height:
                query_end_height = current_block_height
                time_to_stop = True

            if not time_to_stop:
                self.log(f"Long query for {self.network_id}-{nonce} from {query_start_height} to {query_end_height} (normal if catching up)")

            logs = contract.events.MessageSent().get_logs(
                fromBlock=query_start_height,
                toBlock=query_end_height,
                argument_filters={"nonce": nonce_hex},
            )

            logs = [_ for _ in logs]
            if len(logs) > 0:
                break
        
            query_start_height = query_end_height
            query_end_height = query_start_height + self.max_block_range - 1
      
        # query_start_height will be the last used query_end_height
        new_min_height = query_start_height - self.max_block_range * 2 // 3

        if len(logs) == 0:
            return None
      
        event = logs[0]
        new_message = Message(
            nonce=event.args.nonce,
            source_chain=self.network_id.encode(),
            source=bytes.fromhex(event.args.source.replace("0x", "")),
            destination_chain=event.args.destination_chain,
            destination=event.args.destination,
            contents=join_message_contents(event.args.contents),
            block_number=event.blockNumber,
            timestamp=web3.eth.get_block(event.blockNumber).timestamp,
            transaction_hash=event.transactionHash,
            status=MessageStatus.SENT
        )

        self.log(f"Found new sent message {self.network_id}-{nonce} at block {event.blockNumber}")
        existing_message = db.query(Message).filter(and_(
            Message.nonce == new_message.nonce,
            Message.source_chain == new_message.source_chain
        )).first()
        if existing_message is None:
            db.add(new_message)
            db.commit()
        else:
            self.log(f"Message {self.network_id}-{nonce} already exists in the database")

        return nonce + 1, new_min_height

    async def sentMessageWatcher(self):
        web3 = self.getWeb3()
        db = self.getDb()

        latest_message_in_db = db.query(Message).filter(
            Message.source_chain == self.network_id.encode()
        ).order_by(Message.nonce.desc()).first()

        next_nonce: int = int(latest_message_in_db.nonce.hex(), 16) + 1 if latest_message_in_db is not None else 1
        start_height: int = latest_message_in_db.block_number - 1 if latest_message_in_db is not None else self.min_height

        portal = web3.eth.contract(address=self.portal_address, abi=PORTAL_EVENTS_ABI)

        while True:
            self.log(f"Checking for sent message with id {next_nonce}...")
            resp = self.syncSentMessageEvents(web3, db, portal, next_nonce, start_height)

            if resp is None:
                await asyncio.sleep(60)
                continue 

            next_nonce, start_height = resp


    async def receivedMessageWatcher(self):
        web3 = self.getWeb3()
        db = self.getDb()

        portal = web3.eth.contract(address=self.portal_address, abi=PORTAL_EVENTS_ABI)

        while True:
            await asyncio.sleep(5)


    def start(self, loop):
      self.log(f"Starting...")

      self.tasks.append(
          loop.create_task(self.sentMessageWatcher())
      )
      self.tasks.append(
          loop.create_task(self.receivedMessageWatcher())
      )

    def stop(self):
        self.log(f"Stopping...")

        for task in self.tasks:
            task.cancel()

    async def await_stopped(self):
        await asyncio.gather(*self.tasks)

        self.tasks = []


PORTAL_EVENTS_ABI = [
    {
      "anonymous": False,
      "inputs": [
        {
          "indexed": True,
          "internalType": "bytes32",
          "name": "nonce",
          "type": "bytes32"
        },
        {
          "indexed": False,
          "internalType": "bytes3",
          "name": "source_chain",
          "type": "bytes3"
        },
        {
          "indexed": False,
          "internalType": "bytes32",
          "name": "source",
          "type": "bytes32"
        },
        {
          "indexed": False,
          "internalType": "address",
          "name": "destination",
          "type": "address"
        },
        {
          "indexed": False,
          "internalType": "bytes32[]",
          "name": "contents",
          "type": "bytes32[]"
        }
      ],
      "name": "MessageReceived",
      "type": "event"
    },
    {
      "anonymous": False,
      "inputs": [
        {
          "indexed": True,
          "internalType": "bytes32",
          "name": "nonce",
          "type": "bytes32"
        },
        {
          "indexed": False,
          "internalType": "address",
          "name": "source",
          "type": "address"
        },
        {
          "indexed": False,
          "internalType": "bytes3",
          "name": "destination_chain",
          "type": "bytes3"
        },
        {
          "indexed": False,
          "internalType": "bytes32",
          "name": "destination",
          "type": "bytes32"
        },
        {
          "indexed": False,
          "internalType": "bytes32[]",
          "name": "contents",
          "type": "bytes32[]"
        }
      ],
      "name": "MessageSent",
      "type": "event"
    }
]
