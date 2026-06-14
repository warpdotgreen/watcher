import asyncio
import json
import logging
import os
import re
from aiohttp import ClientTimeout
from web3 import AsyncWeb3, Web3
from web3.providers import WebsocketProviderV2, AsyncHTTPProvider
from typing import Any
from config import Network
from sqlalchemy import and_
from db import setup_database, Message, MessageStatus, join_message_contents, increment_key_value

# Blocks rescanned on catch-up so a short reorg cannot leave a gap.
REORG_SAFETY_MARGIN = 64
RPC_REQUEST_TIMEOUT = 120

MESSAGE_SENT_TOPIC = Web3.keccak(
    text="MessageSent(bytes32,address,bytes3,bytes32,bytes32[])"
).hex()
MESSAGE_RECEIVED_TOPIC = Web3.keccak(
    text="MessageReceived(bytes32,bytes3,bytes32,address,bytes32[])"
).hex()


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
        self.max_block_range = network.max_block_range or 1000
        self.portal_address = Web3.to_checksum_address(network.portal_address)
        self.tasks = []
        self.next_nonce = 1
        self.sent_scan_height = network.min_height
        self.received_scan_height = network.min_height
        ws_env = f"{network.id.upper()}_WS_RPC_URL"
        self.ws_rpc_url = os.environ.get(ws_env)

    def getWebsocketUrl(self) -> str:
        if self.ws_rpc_url:
            return self.ws_rpc_url

        url = self.rpc_url
        if url.startswith("https://"):
            url = "wss://" + url[len("https://"):]
        elif url.startswith("http://"):
            url = "ws://" + url[len("http://"):]

        # Infura requires /ws/ in the path: ...infura.io/ws/v3/<key>
        if "infura.io" in url and "/ws/" not in url:
            url = url.replace(".infura.io/", ".infura.io/ws/", 1)

        return url

    def getHttpProvider(self) -> AsyncHTTPProvider:
        return AsyncHTTPProvider(
            self.rpc_url,
            request_kwargs={"timeout": ClientTimeout(total=RPC_REQUEST_TIMEOUT)},
        )

    def format_error(self, error: Exception) -> str:
        msg = repr(error)
        for secret in (self.rpc_url, self.ws_rpc_url, self.getWebsocketUrl()):
            if secret:
                msg = msg.replace(secret, "[redacted]")
        msg = re.sub(r"/v3/[a-zA-Z0-9]+", "/v3/[redacted]", msg)
        msg = re.sub(r"/v2/[a-zA-Z0-9]+", "/v2/[redacted]", msg)
        return msg

    def getDb(self):
        return setup_database()

    def log(self, message):
        logging.info(f"[{self.network_id} watcher] - {message}")

    def nonceIntToBytes(self, nonceInt: int) -> str:
        s = hex(nonceInt)[2:]
        return "0x" + (64 - len(s)) * "0" + s

    def advance_cursor(self, scanned_to: int) -> int:
        return max(self.min_height, scanned_to - REORG_SAFETY_MARGIN)

    def log_bytes(self, value) -> bytes:
        if isinstance(value, bytes):
            return value
        if hasattr(value, "hex"):
            return bytes(value)
        return bytes.fromhex(str(value).replace("0x", ""))

    def log_nonce(self, raw_log: dict) -> bytes:
        return self.log_bytes(raw_log["topics"][1])

    def is_head_block_error(self, error: Exception) -> bool:
        parts = [str(error), repr(error)]
        for arg in error.args:
            parts.append(str(arg))
            if isinstance(arg, dict):
                parts.append(str(arg.get("message", "")))
        text = " ".join(parts).lower()
        return "beyond current head" in text or "head block" in text

    def is_block_range_too_large_error(self, error: Exception) -> bool:
        text = " ".join([str(error), repr(error)] + [str(a) for a in error.args]).lower()
        return any(
            phrase in text
            for phrase in (
                "block range",
                "too many",
                "exceed",
                "max block",
                "10,000",
                "10000",
            )
        ) and not self.is_head_block_error(error)

    def is_transient_rpc_error(self, error: Exception) -> bool:
        text = self.format_error(error).lower()
        return any(
            phrase in text
            for phrase in (
                "timeout",
                "timed out",
                "429",
                "too many requests",
                "connection",
                "reset",
                "503",
                "502",
                "504",
                "rate limit",
                "temporarily unavailable",
                "cannot connect",
                "disconnected",
            )
        )

    def safe_db_rollback(self, db) -> None:
        try:
            db.rollback()
        except Exception:
            pass

    async def rpc_with_retry(self, fn):
        delay = 1
        while True:
            try:
                return await fn()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not self.is_transient_rpc_error(e):
                    raise
                self.log(
                    f"Transient RPC error: {self.format_error(e)}; "
                    f"retrying in {delay}s..."
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)

    async def get_head_block(self, w3) -> int:
        return await self.rpc_with_retry(lambda: w3.eth.block_number)

    async def get_block_timestamp(self, w3, block_number: int) -> int:
        block = await self.rpc_with_retry(lambda bn=block_number: w3.eth.get_block(bn))
        return int(block["timestamp"])

    async def query_event_logs(
        self,
        w3,
        event,
        from_block: int,
        to_block: int,
        argument_filters: dict | None = None,
    ) -> list:
        while True:
            head = await self.get_head_block(w3)
            to_block = min(to_block, head)
            if from_block > to_block:
                return []

            kwargs = {"fromBlock": from_block, "toBlock": to_block}
            if argument_filters:
                kwargs["argument_filters"] = argument_filters

            try:
                return await self.rpc_with_retry(lambda: event.get_logs(**kwargs))
            except Exception as e:
                if self.is_head_block_error(e):
                    await asyncio.sleep(0.5)
                    continue
                if from_block < to_block and self.is_block_range_too_large_error(e):
                    mid = (from_block + to_block) // 2
                    self.log(
                        f"Block range {from_block}-{to_block} too large; "
                        f"splitting at {mid}"
                    )
                    left = await self.query_event_logs(
                        w3, event, from_block, mid, argument_filters
                    )
                    right = await self.query_event_logs(
                        w3, event, mid + 1, to_block, argument_filters
                    )
                    return left + right
                raise

    def reverseParserSideEffects(self, db, message: Message) -> None:
        if not message.parsed or message.source_chain.decode() != self.network_id:
            return

        parsed = json.loads(message.parsed)
        msg_type = parsed.get("type")
        if msg_type not in ("erc20_bridge", "cat_bridge"):
            return

        token_symbol = parsed["token_symbol"]
        amount = parsed["amount_mojo"]
        increment_key_value(db, f"{token_symbol}_total_volume", -amount)

        if msg_type == "erc20_bridge":
            increment_key_value(db, f"{token_symbol}_locked", -amount)
        else:
            increment_key_value(db, f"{token_symbol}_locked", amount)

    async def handleSentReorg(self, w3, db, raw_log: dict) -> None:
        nonce = self.log_nonce(raw_log)
        block_number = raw_log["blockNumber"]
        tx_hash = self.log_bytes(raw_log["transactionHash"])
        nonce_int = int(nonce.hex(), 16)

        msg = db.query(Message).filter(and_(
            Message.nonce == nonce,
            Message.source_chain == self.network_id.encode(),
            Message.source_block_number == block_number,
            Message.source_transaction_hash == tx_hash,
        )).first()

        if msg is None:
            self.log(
                f"MessageSent reorg at block {block_number} for nonce {nonce_int}: "
                f"no matching DB row"
            )
            self.next_nonce = min(self.next_nonce, nonce_int)
            self.sent_scan_height = min(self.sent_scan_height, self.advance_cursor(block_number))
            return

        if msg.status == MessageStatus.RECEIVED:
            self.log(
                f"Reorg: removing MessageSent {self.network_id}-{nonce_int} that was "
                f"already marked RECEIVED on {msg.destination_chain.decode()}"
            )
        else:
            self.log(
                f"Reorg: removing MessageSent {self.network_id}-{nonce_int} "
                f"from block {block_number}"
            )

        self.reverseParserSideEffects(db, msg)
        db.delete(msg)

        # Later nonces mined on the orphaned fork may also be invalid.
        fork_messages = db.query(Message).filter(and_(
            Message.source_chain == self.network_id.encode(),
            Message.source_block_number >= block_number,
        )).all()
        for fork_msg in fork_messages:
            fork_nonce = int(fork_msg.nonce.hex(), 16)
            if fork_nonce <= nonce_int:
                continue
            self.log(
                f"Reorg: removing orphaned fork message {self.network_id}-{fork_nonce} "
                f"from block {fork_msg.source_block_number}"
            )
            self.reverseParserSideEffects(db, fork_msg)
            db.delete(fork_msg)

        db.commit()
        self.next_nonce = nonce_int
        self.sent_scan_height = self.advance_cursor(block_number)

    async def handleReceivedReorg(self, w3, db, portal, raw_log: dict) -> None:
        event = portal.events.MessageReceived().process_log(raw_log)
        block_number = event.blockNumber
        tx_hash = self.log_bytes(event.transactionHash)
        nonce_hex = event.args.nonce.hex()

        msg = db.query(Message).filter(and_(
            Message.nonce == event.args.nonce,
            Message.source_chain == event.args.source_chain,
            Message.destination_chain == self.network_id.encode(),
            Message.destination_block_number == block_number,
            Message.destination_transaction_hash == tx_hash,
        )).first()

        if msg is None:
            self.log(
                f"MessageReceived reorg at block {block_number} for "
                f"{event.args.source_chain.decode()}-{nonce_hex}: no matching DB row"
            )
            return

        if msg.status != MessageStatus.RECEIVED:
            self.log(
                f"MessageReceived reorg at block {block_number} for "
                f"{event.args.source_chain.decode()}-{nonce_hex}: "
                f"message is {msg.status.name}, skipping"
            )
            return

        self.log(
            f"Reorg: reverting MessageReceived {event.args.source_chain.decode()}-"
            f"{nonce_hex} at block {block_number} back to SENT"
        )
        msg.status = MessageStatus.SENT
        msg.destination_block_number = None
        msg.destination_timestamp = None
        msg.destination_transaction_hash = None
        db.commit()

        self.received_scan_height = min(
            self.received_scan_height,
            self.advance_cursor(block_number),
        )

    def loadStateFromDb(self, db):
        latest_sent: Message | None = db.query(Message).filter(
            Message.source_chain == self.network_id.encode()
        ).order_by(Message.nonce.desc()).first()

        if latest_sent is None:
            self.next_nonce = 1
            self.sent_scan_height = self.min_height
        else:
            self.next_nonce = int(latest_sent.nonce.hex(), 16) + 1
            self.sent_scan_height = self.advance_cursor(latest_sent.source_block_number)

        latest_received: Message | None = db.query(Message).filter(and_(
            Message.destination_chain == self.network_id.encode(),
            Message.destination_block_number != None
        )).order_by(Message.destination_block_number.desc()).first()

        self.received_scan_height = (
            self.advance_cursor(latest_received.destination_block_number)
            if latest_received is not None
            else self.min_height
        )

    async def fetchSentEvent(
        self, w3, portal, nonce: int, start_height: int
    ) -> tuple[Any | None, int]:
        query_start_height = start_height
        query_end_height = query_start_height + self.max_block_range - 1
        nonce_hex = self.nonceIntToBytes(nonce)
        highest_block_scanned = start_height
        sent_event = portal.events.MessageSent()

        while True:
            head = await self.get_head_block(w3)
            query_end_height = min(query_end_height, head)

            if query_start_height > query_end_height:
                return None, self.advance_cursor(highest_block_scanned)

            self.log(
                f"Catching up MessageSent {self.network_id}-{nonce} "
                f"in blocks {query_start_height}-{query_end_height}"
            )
            highest_block_scanned = query_end_height

            logs = await self.query_event_logs(
                w3,
                sent_event,
                query_start_height,
                query_end_height,
                argument_filters={"nonce": nonce_hex},
            )

            if len(logs) > 0:
                return logs[0], self.advance_cursor(max(highest_block_scanned, logs[0].blockNumber))

            if query_end_height >= head:
                return None, self.advance_cursor(highest_block_scanned)

            query_start_height = query_end_height + 1
            query_end_height = query_start_height + self.max_block_range - 1

    async def processSentEvent(self, w3, db, event) -> None:
        nonce_int = int(event.args.nonce.hex(), 16)
        source = event.args.source
        if hasattr(source, "hex"):
            source = source if isinstance(source, bytes) else bytes.fromhex(source.hex())
        else:
            source = bytes.fromhex(str(source).replace("0x", ""))

        new_message = Message(
            nonce=event.args.nonce if isinstance(event.args.nonce, bytes) else bytes(event.args.nonce),
            source_chain=self.network_id.encode(),
            source=source,
            destination_chain=event.args.destination_chain,
            destination=event.args.destination,
            contents=join_message_contents(event.args.contents),
            source_block_number=event.blockNumber,
            source_timestamp=await self.get_block_timestamp(w3, event.blockNumber),
            source_transaction_hash=self.log_bytes(event.transactionHash),
            destination_block_number=None,
            destination_timestamp=None,
            destination_transaction_hash=None,
            status=MessageStatus.SENT,
        )

        self.log(f"Found new sent message {self.network_id}-{nonce_int} at block {event.blockNumber}")
        existing_message = db.query(Message).filter(and_(
            Message.nonce == new_message.nonce,
            Message.source_chain == new_message.source_chain,
        )).first()
        if existing_message is None:
            self.log(f"Adding message {self.network_id}-{nonce_int} to the database...")
            new_message = self.parse_message_func(db, new_message)
            db.add(new_message)
            db.commit()
        else:
            self.log(f"Message {self.network_id}-{nonce_int} already exists in the database")

    async def catchUpSentMessages(self, w3, db, portal) -> None:
        while True:
            event, next_scan_height = await self.fetchSentEvent(
                w3, portal, self.next_nonce, self.sent_scan_height
            )
            if event is None:
                self.sent_scan_height = next_scan_height
                self.log(f"Sent message catch-up complete; waiting for nonce {self.next_nonce}")
                return

            await self.processSentEvent(w3, db, event)
            self.next_nonce += 1
            self.sent_scan_height = self.advance_cursor(event.blockNumber)

    async def waitForSentMessage(self, db, message_nonce, source_chain: bytes) -> Message:
        logged_wait = False
        while True:
            msg = db.query(Message).filter(and_(
                Message.nonce == message_nonce,
                Message.source_chain == source_chain,
                Message.destination_chain == self.network_id.encode(),
            )).first()
            if msg is not None:
                return msg
            if not logged_wait:
                self.log(
                    f"Message {source_chain.decode()}-{message_nonce.hex()} "
                    f"not found in the database; waiting for listener on the other side..."
                )
                logged_wait = True
            await asyncio.sleep(5)

    async def processReceivedEvent(self, w3, db, event) -> None:
        message_nonce = event.args.nonce
        self.log(
            f"Found new received message "
            f"{event.args.source_chain.decode()}-{message_nonce.hex()} "
            f"at block {event.blockNumber}"
        )

        msg = await self.waitForSentMessage(db, message_nonce, event.args.source_chain)

        if msg.status == MessageStatus.RECEIVED:
            event_tx_hash = bytes(event.transactionHash)
            if (
                msg.destination_block_number == event.blockNumber
                and msg.destination_transaction_hash == event_tx_hash
            ):
                self.log(
                    f"Message {event.args.source_chain.decode()}-{message_nonce.hex()} "
                    f"already marked as 'RECEIVED' in the database."
                )
                return

            self.log(
                f"Updating RECEIVED message {event.args.source_chain.decode()}-"
                f"{message_nonce.hex()} to canonical block {event.blockNumber} "
                f"(was block {msg.destination_block_number})"
            )
            msg.destination_block_number = event.blockNumber
            msg.destination_timestamp = await self.get_block_timestamp(w3, event.blockNumber)
            msg.destination_transaction_hash = self.log_bytes(event.transactionHash)
            db.commit()
            return

        self.log(
            f"Marking message {event.args.source_chain.decode()}-{message_nonce.hex()} "
            f"as 'RECEIVED' in the database."
        )
        msg.status = MessageStatus.RECEIVED
        msg.destination_block_number = event.blockNumber
        msg.destination_timestamp = await self.get_block_timestamp(w3, event.blockNumber)
        msg.destination_transaction_hash = self.log_bytes(event.transactionHash)
        db.commit()

    async def catchUpReceivedMessages(self, w3, db, portal) -> None:
        query_start_height = self.received_scan_height
        received_event = portal.events.MessageReceived()

        while True:
            head = await self.get_head_block(w3)
            if query_start_height > head:
                self.log(
                    f"Received message catch-up complete from block {self.received_scan_height}"
                )
                return

            query_end_height = min(
                query_start_height + self.max_block_range - 1,
                head,
            )
            self.log(
                f"Catching up MessageReceived in blocks "
                f"{query_start_height}-{query_end_height}"
            )

            logs = await self.query_event_logs(
                w3,
                received_event,
                query_start_height,
                query_end_height,
            )

            for event in logs:
                await self.processReceivedEvent(w3, db, event)

            self.received_scan_height = self.advance_cursor(query_end_height)
            query_start_height = query_end_height + 1

    def subscription_topic(self, raw_log: dict) -> str | None:
        topics = raw_log.get("topics")
        if not topics:
            return None
        topic = topics[0]
        return topic.hex() if hasattr(topic, "hex") else str(topic)

    async def handleSentLog(self, w3, db, portal, raw_log: dict) -> None:
        if raw_log.get("removed"):
            await self.handleSentReorg(w3, db, raw_log)
            await self.catchUpSentMessages(w3, db, portal)
            return

        try:
            event = portal.events.MessageSent().process_log(raw_log)
        except Exception as e:
            self.log(f"Failed to decode MessageSent log: {self.format_error(e)}")
            return

        nonce_int = int(event.args.nonce.hex(), 16)

        if nonce_int < self.next_nonce:
            return

        if nonce_int > self.next_nonce:
            self.log(
                f"Unexpected nonce {nonce_int} (expected {self.next_nonce}); re-syncing..."
            )
            await self.catchUpSentMessages(w3, db, portal)
            return

        await self.processSentEvent(w3, db, event)
        self.next_nonce += 1
        self.sent_scan_height = self.advance_cursor(event.blockNumber)

    async def handleReceivedLog(self, w3, db, portal, raw_log: dict) -> None:
        if raw_log.get("removed"):
            await self.handleReceivedReorg(w3, db, portal, raw_log)
            await self.catchUpReceivedMessages(w3, db, portal)
            return

        try:
            event = portal.events.MessageReceived().process_log(raw_log)
        except Exception as e:
            self.log(f"Failed to decode MessageReceived log: {self.format_error(e)}")
            return

        await self.processReceivedEvent(w3, db, event)
        self.received_scan_height = max(
            self.received_scan_height,
            self.advance_cursor(event.blockNumber),
        )

    async def receivedLogProcessor(self, http_w3, portal, queue: asyncio.Queue) -> None:
        try:
            while True:
                raw_log = await queue.get()
                db = self.getDb()
                try:
                    await self.handleReceivedLog(http_w3, db, portal, raw_log)
                except Exception as e:
                    self.safe_db_rollback(db)
                    self.log(f"Error processing received log: {self.format_error(e)}")
                finally:
                    db.close()
                    queue.task_done()
        except asyncio.CancelledError:
            raise

    async def runSubscriptionSession(self) -> None:
        http_w3 = AsyncWeb3(self.getHttpProvider())
        provider = WebsocketProviderV2(
            self.getWebsocketUrl(),
            request_timeout=RPC_REQUEST_TIMEOUT,
        )
        self.log("Connecting to WebSocket...")

        async with AsyncWeb3.persistent_websocket(provider) as ws_w3:
            db = self.getDb()
            try:
                self.loadStateFromDb(db)

                portal = http_w3.eth.contract(address=self.portal_address, abi=PORTAL_EVENTS_ABI)
                received_queue: asyncio.Queue = asyncio.Queue()
                received_task = asyncio.create_task(
                    self.receivedLogProcessor(http_w3, portal, received_queue)
                )

                try:
                    await self.catchUpSentMessages(http_w3, db, portal)
                    await self.catchUpReceivedMessages(http_w3, db, portal)

                    sent_filter = {
                        "address": self.portal_address,
                        "topics": [MESSAGE_SENT_TOPIC],
                    }
                    received_filter = {
                        "address": self.portal_address,
                        "topics": [MESSAGE_RECEIVED_TOPIC],
                    }

                    sent_sub_id = await ws_w3.eth.subscribe("logs", sent_filter)
                    received_sub_id = await ws_w3.eth.subscribe("logs", received_filter)
                    self.log(
                        f"Subscribed to portal logs "
                        f"(MessageSent={sent_sub_id}, MessageReceived={received_sub_id})"
                    )

                    # eth_subscribe only delivers logs from this point forward; rescan the
                    # tip to close the gap between catch-up and subscription activation.
                    await self.catchUpSentMessages(http_w3, db, portal)
                    await self.catchUpReceivedMessages(http_w3, db, portal)

                    async for message in ws_w3.ws.process_subscriptions():
                        result = message.get("result")
                        if result is None:
                            continue

                        topic0 = self.subscription_topic(result)
                        if topic0 is None:
                            continue
                        if topic0 == MESSAGE_SENT_TOPIC:
                            try:
                                await self.handleSentLog(http_w3, db, portal, result)
                            except Exception as e:
                                self.safe_db_rollback(db)
                                self.log(f"Error processing sent log: {self.format_error(e)}")
                                self.loadStateFromDb(db)
                        elif topic0 == MESSAGE_RECEIVED_TOPIC:
                            await received_queue.put(result)
                finally:
                    received_task.cancel()
                    try:
                        await received_task
                    except asyncio.CancelledError:
                        pass
            finally:
                db.close()

    async def logSubscriptionWatcher(self):
        while True:
            try:
                await self.runSubscriptionSession()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.log(f"WebSocket session error: {self.format_error(e)}; reconnecting in 30s...")
                await asyncio.sleep(30)

    def start(self, loop):
        self.log("Starting...")

        self.tasks.append(
            loop.create_task(self.logSubscriptionWatcher())
        )

    def stop(self):
        self.log("Stopping...")

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
