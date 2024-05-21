import asyncio
import logging
from web3 import Web3
from typing import Tuple
from config import Network
from sqlalchemy import and_
from db import setup_database, Message, MessageStatus, join_message_contents

class EVMWatcher:
    parse_message_func: any
    network_id: str
    rpc_url: str
    min_height: int
    max_block_range: int
    portal_address: str
    tasks: list

    def __init__(self, network: Network, parse_message_func: any):
      self.parse_message_func = parse_message_func
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
        logging.info(f"[{self.network_id} watcher] - {message}")
    
    def nonceIntToBytes(self, nonceInt: int) -> bytes:
      s = hex(nonceInt)[2:]
      return (64 - len(s)) * "0" + s


    async def syncSentMessageEvents(self, web3, db, contract, nonce: int, start_height: int) -> Tuple[int, int] | None: # new nonce, new start height
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

            if query_start_height > query_end_height:
                query_start_height = query_end_height
                break

            if time_to_stop:
                self.log(f"Querying blocks {query_start_height}-{query_end_height} for {self.network_id}-{nonce}")
            else:
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
      
        if len(logs) == 0:
            await asyncio.sleep(60)
            return nonce, query_end_height - self.max_block_range * 2 // 3
      
        event = logs[0]
        new_message = Message(
            nonce=event.args.nonce,
            source_chain=self.network_id.encode(),
            source=bytes.fromhex(event.args.source.replace("0x", "")),
            destination_chain=event.args.destination_chain,
            destination=event.args.destination,
            contents=join_message_contents(event.args.contents),
            source_block_number=event.blockNumber,
            source_timestamp=int(web3.eth.get_block(event.blockNumber).timestamp),
            source_transaction_hash=event.transactionHash,
            destination_block_number=None,
            destination_timestamp=None,
            destination_transaction_hash=None,
            status=MessageStatus.SENT
        )

        self.log(f"Found new sent message {self.network_id}-{nonce} at block {event.blockNumber}")
        existing_message = db.query(Message).filter(and_(
            Message.nonce == new_message.nonce,
            Message.source_chain == new_message.source_chain
        )).first()
        if existing_message is None:
            self.log(f"Adding message {self.network_id}-{nonce} to the database...")
            new_message = self.parse_message_func(db, new_message)
            db.add(new_message)
            db.commit()
        else:
            self.log(f"Message {self.network_id}-{nonce} already exists in the database")

        return nonce + 1, event.blockNumber - 1


    async def sentMessageWatcher(self):
        web3 = self.getWeb3()
        db = self.getDb()

        latest_message_in_db: Message | None = db.query(Message).filter(
            Message.source_chain == self.network_id.encode()
        ).order_by(Message.nonce.desc()).first()

        next_nonce: int = int(latest_message_in_db.nonce.hex(), 16) + 1 if latest_message_in_db is not None else 1
        start_height: int = latest_message_in_db.source_block_number - 1 if latest_message_in_db is not None else self.min_height

        portal = web3.eth.contract(address=self.portal_address, abi=PORTAL_EVENTS_ABI)

        while True:
            self.log(f"Checking for sent message with id {next_nonce}...")
            next_nonce, start_height = await self.syncSentMessageEvents(web3, db, portal, next_nonce, start_height)


    async def syncReceivedMessageEvents(self, web3, db, contract, start_height: int) -> int | None: # new start height
        query_start_height = start_height
        query_end_height = query_start_height + self.max_block_range - 1

        time_to_stop = False
        logs = []
        while not time_to_stop:
            current_block_height = web3.eth.block_number
            if query_end_height > current_block_height:
                query_end_height = current_block_height
                time_to_stop = True

            if query_start_height > query_end_height:
                query_start_height = query_end_height
                break

            if time_to_stop:
                self.log(f"Querying blocks {query_start_height}-{query_end_height} (ReceivedMessage)")
            else:
                self.log(f"Long query for {self.network_id} from {query_start_height} to {query_end_height} (normal if catching up)")

            logs = contract.events.MessageReceived().get_logs(
                fromBlock=query_start_height,
                toBlock=query_end_height,
            )

            logs = [_ for _ in logs]
            if len(logs) > 0:
                break
        
            query_start_height = query_end_height
            query_end_height = query_start_height + self.max_block_range - 1
      
        new_min_height = query_start_height 

        if len(logs) == 0:
            await asyncio.sleep(60)
            return query_end_height - self.max_block_range * 2 // 3
      
        for event in logs:
            message_nonce = event.args.nonce
            self.log(f"Found new received message {event.args.source_chain.decode()}-{message_nonce.hex()} at block {event.blockNumber}")

            msg: Message | None = db.query(Message).filter(and_(
                Message.nonce == message_nonce,
                Message.source_chain == event.args.source_chain,
                Message.destination_chain == self.network_id.encode()
            )).first()

            if msg is None:
                self.log(f"Message {event.args.source_chain.decode()}-{message_nonce.hex()} not found in the database; waiting for listener on the other side to add it...")
            while msg is None:
                msg = db.query(Message).filter(and_(
                    Message.nonce == message_nonce,
                    Message.source_chain == event.args.source_chain,
                    Message.destination_chain == self.network_id.encode()
                )).first()
                await asyncio.sleep(5)

            new_min_height = max(new_min_height, event.blockNumber + 1)

            if msg.status == MessageStatus.RECEIVED:
                self.log(f"Message {event.args.source_chain.decode()}-{message_nonce.hex()} already marked as 'RECEIVED' in the database.")
                continue

            self.log(f"Marking message {event.args.source_chain.decode()}-{message_nonce.hex()} as 'RECEIVED' in the database.")
            msg.status = MessageStatus.RECEIVED
            msg.destination_block_number = event.blockNumber
            msg.destination_timestamp = web3.eth.get_block(event.blockNumber).timestamp
            msg.destination_transaction_hash = event.transactionHash
            db.commit()

        return new_min_height
    

    async def receivedMessageWatcher(self):
        web3 = self.getWeb3()
        db = self.getDb()

        latest_message_in_db: Message | None = db.query(Message).filter(and_(
            Message.destination_chain == self.network_id.encode(),
            Message.destination_block_number != None
        )).order_by(Message.destination_block_number.desc()).first()

        start_height: int = latest_message_in_db.destination_block_number + 1 if latest_message_in_db is not None else self.min_height

        portal = web3.eth.contract(address=self.portal_address, abi=PORTAL_EVENTS_ABI)

        while True:
            self.log(f"Checking for received message from height {start_height}...")
            start_height = await self.syncReceivedMessageEvents(web3, db, portal, start_height)


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
