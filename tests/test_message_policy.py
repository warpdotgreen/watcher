import asyncio
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.program import Program
from chia.types.blockchain_format.serialized_program import SerializedProgram
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.coin_spend import CoinSpend

from config import Network, NetworkType
from db import ChiaPortalState, KeyValueEntry, Message, MessageStatus, PolicyReject, setup_database
from watchers.cli_policy import BridgeRoutes, PolicyResult
from commands.policy_runtime import is_rpc_rate_limited
from commands.spend_policy import EvmRoute
from watchers.chia_watcher import ChiaWatcher
from watchers.evm_watcher import EVMWatcher


def _routes() -> BridgeRoutes:
    return BridgeRoutes(
        portal_launcher_id=b"\x01" * 32,
        eth=EvmRoute(b"eth", b"\x02" * 20, b"\x03" * 20, b"\x04" * 20, "0x" + "11" * 32),
        bse=EvmRoute(b"bse", b"\x05" * 20, b"\x06" * 20, b"\x07" * 20, "0x" + "22" * 32),
    )


def _temp_db():
    handle, path = tempfile.mkstemp(suffix=".db")
    os.close(handle)
    return setup_database("sqlite:///" + path), path


def _kv(db, key):
    row = db.query(KeyValueEntry).filter(KeyValueEntry.key == key).first()
    return None if row is None else row.value_int


def _volume_parser(db, message):
    from db import increment_key_value
    increment_key_value(db, "USDC_total_volume", 7, commit=False)
    increment_key_value(db, "USDC_locked", 3, commit=False)
    message.parsed = "ok"
    return message


def _watch_commits(db, path):
    reader = setup_database("sqlite:///" + path)
    log = []
    real_commit = db.commit

    def commit():
        real_commit()
        reader.rollback()
        messages = reader.query(Message).all()
        log.append({
            "count": len(messages),
            "parsed": messages[0].parsed if len(messages) == 1 else None,
            "volume": _kv(reader, "USDC_total_volume"),
            "locked": _kv(reader, "USDC_locked"),
        })

    db.commit = commit
    return reader, log


class _Args:
    def __init__(self, nonce):
        self.nonce = nonce
        self.source = b"\x22" * 20
        self.destination_chain = b"xch"
        self.destination = b"\x33" * 32
        self.contents = [b"\x44" * 32]


class _Event:
    def __init__(self, nonce):
        self.args = _Args(nonce)
        self.blockNumber = 100
        self.transactionHash = b"\x55" * 32


def _event(nonce: int, block: int) -> _Event:
    event = _Event(nonce.to_bytes(32, "big"))
    event.blockNumber = block
    event.transactionHash = nonce.to_bytes(32, "big")
    return event


def _sent_portal(event):
    def message_sent():
        return SimpleNamespace(process_log=lambda raw_log: event)

    return SimpleNamespace(events=SimpleNamespace(MessageSent=message_sent))


def _evm_watcher():
    network = Network(
        id="eth",
        type=NetworkType.EVM,
        rpc_url="http://127.0.0.1:1",
        min_height=1,
        portal_address="0x" + "11" * 20,
        portal_impl_codehash="0x" + "22" * 32,
    )
    watcher = EVMWatcher(network, _volume_parser, _routes())
    watcher.next_nonce = 1
    watcher.sent_scan_height = 1

    async def timestamp(w3, block_number):
        return 1

    watcher.get_block_timestamp = timestamp
    return watcher


def _open_chia(parse_message_func=_volume_parser):
    ph = bytes32(b"\xaa" * 32)
    network = Network(
        id="xch",
        type=NetworkType.CHIA,
        rpc_url="http://127.0.0.1:1",
        min_height=1,
        message_toll=1,
        portal_launcher_id="ab" * 32,
        bridging_puzzle_hash=ph.hex(),
    )
    watcher = ChiaWatcher(network, parse_message_func, _routes())
    db, path = _temp_db()
    memo = Program.to([b"eth", b"\x11" * 20, b"\x22" * 32])
    condition = Program.to([51, ph, 5, memo])
    puzzle = Program.to(1).cons(Program.to([condition]))
    parent = Coin(bytes32(b"\x01" * 32), bytes32(b"\x02" * 32), 1)
    spend = CoinSpend(
        parent,
        SerializedProgram.from_bytes(bytes(puzzle)),
        SerializedProgram.from_bytes(bytes(Program.to(0))),
    )
    bridging = Coin(parent.name(), ph, 5)
    parent_record = SimpleNamespace(coin=parent, spent_block_index=10, timestamp=1)
    coin_record = SimpleNamespace(coin=bridging, timestamp=123, confirmed_block_index=10)

    class Node:
        async def get_coin_record_by_name(self, name):
            if name == bridging.name():
                return coin_record
            return parent_record

        async def get_puzzle_and_solution(self, name, height):
            return spend

        async def get_block_record_by_height(self, height):
            return None

    return watcher, db, path, Node(), coin_record, bridging


class MessagePolicyTests(unittest.TestCase):
    def test_chia_parent_neither_locker_nor_burner_is_skipped(self):
        asyncio.run(self._chia_parent_neither_locker_nor_burner_is_skipped())

    async def _chia_parent_neither_locker_nor_burner_is_skipped(self):
        ph = bytes32(b"\xaa" * 32)
        network = Network(
            id="xch",
            type=NetworkType.CHIA,
            rpc_url="http://127.0.0.1:1",
            min_height=1,
            message_toll=1,
            portal_launcher_id="ab" * 32,
            bridging_puzzle_hash=ph.hex(),
        )
        watcher = ChiaWatcher(network, _volume_parser, _routes())
        db, path = _temp_db()
        try:
            memo = Program.to([b"eth", b"\x11" * 20, b"\x22" * 32])
            condition = Program.to([51, ph, 5, memo])
            puzzle = Program.to(1).cons(Program.to([condition]))
            parent = Coin(bytes32(b"\x01" * 32), bytes32(b"\x02" * 32), 1)
            spend = CoinSpend(
                parent,
                SerializedProgram.from_bytes(bytes(puzzle)),
                SerializedProgram.from_bytes(bytes(Program.to(0))),
            )
            bridging = Coin(parent.name(), ph, 5)
            parent_record = SimpleNamespace(coin=parent, spent_block_index=10, timestamp=1)
            coin_record = SimpleNamespace(coin=bridging, timestamp=123, confirmed_block_index=10)

            class Node:
                async def get_coin_record_by_name(self, name):
                    if name == bridging.name():
                        return coin_record
                    return parent_record

                async def get_puzzle_and_solution(self, name, height):
                    return spend

                async def get_block_record_by_height(self, height):
                    return None

            import watchers.chia_watcher as chia_mod
            real_policy = chia_mod.evaluate_chia_message_policy
            seen = []

            async def wrapped(**kwargs):
                result = await real_policy(**kwargs)
                seen.append(result)
                return result

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", wrapped):
                kind = await watcher.processCoinRecord(db, Node(), coin_record)

            self.assertEqual(kind, "reject")
            self.assertEqual(len(seen), 1)
            self.assertEqual(seen[0].kind, "reject")
            self.assertIn("neither", seen[0].reason)
            self.assertEqual(db.query(Message).count(), 0)
            self.assertIsNone(db.query(KeyValueEntry).filter(KeyValueEntry.key == "USDC_total_volume").first())
            self.assertIsNone(db.query(KeyValueEntry).filter(KeyValueEntry.key == "USDC_locked").first())

            skip_coin_ids = []
            revisit = watcher.note_coin_outcome(bridging.name(), kind, skip_coin_ids)
            self.assertFalse(revisit)
            self.assertIn(bridging.name(), skip_coin_ids)
            next_pass = list(watcher.skipped_coin_ids)
            self.assertIn(bridging.name(), next_pass)
        finally:
            db.close()
            os.remove(path)

    def test_evm_reject_is_not_stored_and_nonce_advances(self):
        asyncio.run(self._evm_reject_is_not_stored_and_nonce_advances())

    async def _evm_reject_is_not_stored_and_nonce_advances(self):
        await self._evm_policy_cursor(PolicyResult("reject", "test reject"), advanced=True)

    def test_evm_retry_is_not_stored_and_nonce_stays(self):
        asyncio.run(self._evm_retry_is_not_stored_and_nonce_stays())

    async def _evm_retry_is_not_stored_and_nonce_stays(self):
        await self._evm_policy_cursor(PolicyResult("retry", "test retry"), advanced=False)

    async def _evm_policy_cursor(self, result: PolicyResult, advanced: bool):
        network = Network(
            id="eth",
            type=NetworkType.EVM,
            rpc_url="http://127.0.0.1:1",
            min_height=1,
            portal_address="0x" + "11" * 20,
            portal_impl_codehash="0x" + "22" * 32,
        )
        watcher = EVMWatcher(network, _volume_parser, _routes())
        watcher.next_nonce = 1
        watcher.sent_scan_height = 1
        db, path = _temp_db()
        try:
            async def fake_policy(**kwargs):
                fake_policy.calls += 1
                return result

            fake_policy.calls = 0
            event = _Event((1).to_bytes(32, "big"))
            with patch("watchers.evm_watcher.evaluate_evm_message_policy", fake_policy):
                moved = await watcher.indexSentEvent(None, db, event)

            self.assertEqual(fake_policy.calls, 1)
            self.assertEqual(moved, advanced)
            self.assertEqual(watcher.next_nonce, 2 if advanced else 1)
            if advanced:
                self.assertGreater(watcher.sent_scan_height, 1)
            else:
                self.assertEqual(watcher.sent_scan_height, 1)
            self.assertEqual(db.query(Message).count(), 0)
            self.assertIsNone(db.query(KeyValueEntry).filter(KeyValueEntry.key == "USDC_total_volume").first())
            self.assertIsNone(db.query(KeyValueEntry).filter(KeyValueEntry.key == "USDC_locked").first())
        finally:
            db.close()
            os.remove(path)

    def test_evm_exact_live_log_reject_advances(self):
        asyncio.run(self._evm_exact_live_log(PolicyResult("reject", "test reject"), inserted=False))

    def test_evm_exact_live_log_accept_inserts(self):
        asyncio.run(self._evm_exact_live_log(PolicyResult("accept", "test accept"), inserted=True))

    async def _evm_exact_live_log(self, result: PolicyResult, inserted: bool):
        watcher = _evm_watcher()
        db, path = _temp_db()
        try:
            async def fake_fetch(w3, portal, nonce, start_height):
                raise AssertionError("catch-up should not run when the live nonce is next")

            async def fake_policy(**kwargs):
                return result

            with patch.object(watcher, "fetchSentEvent", fake_fetch), patch(
                "watchers.evm_watcher.evaluate_evm_message_policy", fake_policy
            ):
                await watcher.handleSentLog(None, db, _sent_portal(_event(1, 100)), {})

            self.assertEqual(watcher.next_nonce, 2)
            self.assertEqual(watcher.sent_scan_height, watcher.advance_cursor(100))
            self.assertEqual(db.query(Message).count(), 1 if inserted else 0)
        finally:
            db.close()
            os.remove(path)

    def test_evm_ahead_log_stays_put_when_catchup_pauses(self):
        for kind in ("retry", "hold"):
            with self.subTest(kind=kind):
                asyncio.run(self._evm_ahead_log_stays_put_when_catchup_pauses(kind))

    async def _evm_ahead_log_stays_put_when_catchup_pauses(self, kind: str):
        watcher = _evm_watcher()
        db, path = _temp_db()
        try:
            seen = []

            async def fake_fetch(w3, portal, nonce, start_height):
                seen.append(("fetch", nonce))
                return _event(1, 50), start_height

            async def fake_policy(**kwargs):
                seen.append(("policy", kwargs["block_number"]))
                if kwargs["block_number"] == 50:
                    return PolicyResult(kind, "pause")
                return PolicyResult("accept", "live")

            with patch.object(watcher, "fetchSentEvent", fake_fetch), patch(
                "watchers.evm_watcher.evaluate_evm_message_policy", fake_policy
            ):
                await watcher.handleSentLog(None, db, _sent_portal(_event(3, 100)), {})

            self.assertEqual(seen, [("fetch", 1), ("policy", 50)])
            self.assertEqual(watcher.next_nonce, 1)
            self.assertEqual(watcher.sent_scan_height, 1)
            self.assertEqual(db.query(Message).count(), 0)
            self.assertIsNone(db.query(KeyValueEntry).filter(KeyValueEntry.key == "USDC_total_volume").first())
            self.assertIsNone(db.query(KeyValueEntry).filter(KeyValueEntry.key == "USDC_locked").first())
        finally:
            db.close()
            os.remove(path)

    def test_evm_ahead_log_indexes_after_rejected_gap(self):
        asyncio.run(self._evm_ahead_log_indexes_after_rejected_gap())

    async def _evm_ahead_log_indexes_after_rejected_gap(self):
        watcher = _evm_watcher()
        db, path = _temp_db()
        try:
            seen = []

            async def fake_fetch(w3, portal, nonce, start_height):
                seen.append(("fetch", nonce))
                if nonce == 1:
                    return _event(1, 50), start_height
                return None, start_height

            async def fake_policy(**kwargs):
                seen.append(("policy", kwargs["block_number"]))
                if kwargs["block_number"] == 50:
                    return PolicyResult("reject", "gap")
                return PolicyResult("accept", "live")

            with patch.object(watcher, "fetchSentEvent", fake_fetch), patch(
                "watchers.evm_watcher.evaluate_evm_message_policy", fake_policy
            ):
                await watcher.handleSentLog(None, db, _sent_portal(_event(2, 100)), {})

            self.assertEqual(seen, [("fetch", 1), ("policy", 50), ("fetch", 2), ("policy", 100)])
            self.assertEqual(watcher.next_nonce, 3)
            self.assertEqual(watcher.sent_scan_height, watcher.advance_cursor(100))
            stored = db.query(Message).one()
            self.assertEqual(stored.nonce, (2).to_bytes(32, "big"))
        finally:
            db.close()
            os.remove(path)

    def test_evm_paused_retry_catchup_runs_again_without_later_log(self):
        asyncio.run(self._evm_paused_retry_catchup_runs_again_without_later_log())

    async def _evm_paused_retry_catchup_runs_again_without_later_log(self):
        watcher = _evm_watcher()
        db, path = _temp_db()
        try:
            fetches = []
            policies = []
            sleeps = []

            async def fake_fetch(w3, portal, nonce, start_height):
                fetches.append(nonce)
                if len(fetches) <= 2:
                    return _event(1, 100), start_height
                return None, watcher.advance_cursor(100)

            async def fake_policy(**kwargs):
                policies.append(kwargs["block_number"])
                if len(policies) == 1:
                    return PolicyResult("retry", "pause")
                return PolicyResult("accept", "retried")

            async def fake_sleep(seconds):
                sleeps.append((seconds, watcher.next_nonce, db.query(Message).count()))
                if len(sleeps) > 1:
                    raise asyncio.CancelledError()

            async def later_log(*args, **kwargs):
                raise AssertionError("second catch-up must not be driven by a later MessageSent")

            with patch.object(watcher, "fetchSentEvent", fake_fetch), patch.object(
                watcher, "handleSentLog", later_log
            ), patch(
                "watchers.evm_watcher.evaluate_evm_message_policy", fake_policy
            ), patch("watchers.evm_watcher.asyncio.sleep", fake_sleep):
                await watcher.catchUpSentMessages(None, db, None)
                self.assertEqual(fetches, [1])
                self.assertEqual(policies, [100])
                self.assertEqual(watcher.next_nonce, 1)
                self.assertEqual(watcher.sent_scan_height, 1)
                self.assertEqual(db.query(Message).count(), 0)

                with self.assertRaises(asyncio.CancelledError):
                    await watcher.sentCatchUpRetryLoop(None, db, None)

            self.assertEqual(sleeps, [(30, 1, 0), (30, 2, 1)])
            self.assertEqual(fetches, [1, 1, 2])
            self.assertEqual(policies, [100, 100])
            self.assertEqual(watcher.next_nonce, 2)
            self.assertEqual(watcher.sent_scan_height, watcher.advance_cursor(100))
            stored = db.query(Message).one()
            self.assertEqual(stored.nonce, (1).to_bytes(32, "big"))
            self.assertIsNotNone(
                db.query(KeyValueEntry).filter(KeyValueEntry.key == "USDC_total_volume").first()
            )
        finally:
            db.close()
            os.remove(path)

    def test_chia_insert_failure_after_accept_is_retried(self):
        asyncio.run(self._chia_insert_failure_after_accept_is_retried())

    async def _chia_insert_failure_after_accept_is_retried(self):
        watcher, db, path, node, coin_record, bridging = _open_chia()
        try:
            real_add = db.add

            def fail_insert(obj):
                if isinstance(obj, Message):
                    raise RuntimeError("insert failed")
                return real_add(obj)

            db.add = fail_insert

            async def accept(**kwargs):
                return PolicyResult("accept", "ok")

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", accept):
                kind = await watcher.processCoinRecord(db, node, coin_record)

            self.assertEqual(kind, "retry")
            self.assertEqual(db.query(Message).count(), 0)
            self.assertIsNone(_kv(db, "USDC_total_volume"))
            self.assertIsNone(_kv(db, "USDC_locked"))
            skip_coin_ids = []
            revisit = watcher.note_coin_outcome(bridging.name(), kind, skip_coin_ids)
            self.assertTrue(revisit)
            self.assertEqual(skip_coin_ids, [])
            self.assertNotIn(bridging.name(), watcher.skipped_coin_ids)
        finally:
            db.close()
            os.remove(path)

    def test_chia_policy_retry_and_hold_are_not_skipped(self):
        for kind in ("retry", "hold"):
            with self.subTest(kind=kind):
                asyncio.run(self._chia_policy_retry_or_hold_is_not_skipped(kind))

    async def _chia_policy_retry_or_hold_is_not_skipped(self, kind: str):
        watcher, db, path, node, coin_record, bridging = _open_chia()
        try:
            async def policy(**kwargs):
                return PolicyResult(kind, "later")

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", policy):
                outcome = await watcher.processCoinRecord(db, node, coin_record)

            self.assertEqual(outcome, kind)
            self.assertEqual(db.query(Message).count(), 0)
            self.assertIsNone(db.query(KeyValueEntry).filter(KeyValueEntry.key == "USDC_total_volume").first())
            skip_coin_ids = []
            revisit = watcher.note_coin_outcome(bridging.name(), outcome, skip_coin_ids)
            self.assertTrue(revisit)
            self.assertEqual(skip_coin_ids, [])
            self.assertNotIn(bridging.name(), watcher.skipped_coin_ids)
        finally:
            db.close()
            os.remove(path)

    def test_chia_missing_parent_then_disappeared_child_is_not_skipped(self):
        for case in ("gone", "height"):
            with self.subTest(case=case):
                asyncio.run(self._chia_missing_parent_then_disappeared_child_is_not_skipped(case))

    async def _chia_missing_parent_then_disappeared_child_is_not_skipped(self, case: str):
        watcher, db, path, _node, coin_record, bridging = _open_chia()
        try:
            child_id = bridging.name()
            seen = {"child": 0, "parent": 0, "policy": 0}

            class Node:
                async def get_coin_record_by_name(self, name):
                    if name == child_id:
                        seen["child"] += 1
                        if seen["child"] == 1:
                            return coin_record
                        if case == "height":
                            return SimpleNamespace(
                                coin=bridging,
                                confirmed_block_index=coin_record.confirmed_block_index + 1,
                                timestamp=coin_record.timestamp,
                            )
                        return None
                    seen["parent"] += 1
                    return None

                async def get_puzzle_and_solution(self, name, height):
                    raise AssertionError("parent spend fetched without a parent record")

            async def fail_policy(**kwargs):
                seen["policy"] += 1
                raise AssertionError("missing parent must not reach policy")

            async def fast_sleep(_seconds):
                return None

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", fail_policy), patch(
                "watchers.chia_watcher.asyncio.sleep", fast_sleep
            ):
                kind = await asyncio.wait_for(
                    watcher.processCoinRecord(db, Node(), coin_record),
                    timeout=2,
                )

            self.assertEqual(kind, "retry")
            self.assertGreaterEqual(seen["child"], 2)
            self.assertGreaterEqual(seen["parent"], 1)
            self.assertEqual(seen["policy"], 0)
            self.assertEqual(db.query(Message).count(), 0)

            skip_coin_ids = []
            revisit = watcher.note_coin_outcome(bridging.name(), kind, skip_coin_ids)
            self.assertTrue(revisit)
            self.assertEqual(skip_coin_ids, [])
            self.assertNotIn(child_id, watcher.skipped_coin_ids)
        finally:
            db.close()
            os.remove(path)

    def _assert_volume_commits_with_parsed(self, log):
        self.assertEqual(
            [(entry["count"], bool(entry["parsed"]), entry["volume"], entry["locked"]) for entry in log],
            [(1, False, None, None), (1, True, 7, 3)],
        )

    def test_chia_volume_waits_for_row_and_second_pass_does_not_add(self):
        asyncio.run(self._chia_volume_waits_for_row_and_second_pass_does_not_add())

    async def _chia_volume_waits_for_row_and_second_pass_does_not_add(self):
        watcher, db, path, node, coin_record, _bridging = _open_chia()
        reader, log = _watch_commits(db, path)
        try:
            async def accept(**kwargs):
                return PolicyResult("accept", "ok")

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", accept):
                kind = await watcher.processCoinRecord(db, node, coin_record)
                self.assertEqual(kind, "accept")
                self._assert_volume_commits_with_parsed(log)
                self.assertEqual(_kv(db, "USDC_total_volume"), 7)
                self.assertEqual(_kv(db, "USDC_locked"), 3)

                again = await watcher.processCoinRecord(db, node, coin_record)

            self.assertEqual(again, "accept")
            self.assertEqual(len(log), 2)
            self.assertEqual(_kv(db, "USDC_total_volume"), 7)
            self.assertEqual(_kv(db, "USDC_locked"), 3)
            stored = db.query(Message).one()
            self.assertEqual(stored.parsed, "ok")
        finally:
            reader.close()
            db.close()
            os.remove(path)

    def test_evm_volume_waits_for_row_and_second_pass_does_not_add(self):
        asyncio.run(self._evm_volume_waits_for_row_and_second_pass_does_not_add())

    async def _evm_volume_waits_for_row_and_second_pass_does_not_add(self):
        watcher = _evm_watcher()
        db, path = _temp_db()
        reader, log = _watch_commits(db, path)
        try:
            async def accept(**kwargs):
                return PolicyResult("accept", "ok")

            event = _Event((1).to_bytes(32, "big"))
            with patch("watchers.evm_watcher.evaluate_evm_message_policy", accept):
                kind = await watcher.processSentEvent(None, db, event)

            self.assertEqual(kind, "accept")
            self._assert_volume_commits_with_parsed(log)
            self.assertEqual(_kv(db, "USDC_total_volume"), 7)
            self.assertEqual(_kv(db, "USDC_locked"), 3)

            again = await watcher.processSentEvent(None, db, event)
            self.assertEqual(again, "accept")
            self.assertEqual(len(log), 2)
            self.assertEqual(_kv(db, "USDC_total_volume"), 7)
            self.assertEqual(_kv(db, "USDC_locked"), 3)
            self.assertEqual(db.query(Message).one().parsed, "ok")
        finally:
            reader.close()
            db.close()
            os.remove(path)

    def test_chia_unparsed_row_is_counted_once_on_retry(self):
        asyncio.run(self._chia_unparsed_row_is_counted_once_on_retry())

    async def _chia_unparsed_row_is_counted_once_on_retry(self):
        calls = {"n": 0}

        def crash_once(db, message):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("crash before volume")
            return _volume_parser(db, message)

        watcher, db, path, node, coin_record, _bridging = _open_chia(crash_once)
        reader, log = _watch_commits(db, path)
        try:
            async def accept(**kwargs):
                return PolicyResult("accept", "ok")

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", accept):
                kind = await watcher.processCoinRecord(db, node, coin_record)
                self.assertEqual(kind, "retry")
                self.assertEqual(log, [{
                    "count": 1,
                    "parsed": None,
                    "volume": None,
                    "locked": None,
                }])
                self.assertIsNone(_kv(db, "USDC_total_volume"))

                kind = await watcher.processCoinRecord(db, node, coin_record)
                self.assertEqual(kind, "accept")
                self.assertEqual(_kv(db, "USDC_total_volume"), 7)
                self.assertEqual(_kv(db, "USDC_locked"), 3)

                kind = await watcher.processCoinRecord(db, node, coin_record)

            self.assertEqual(kind, "accept")
            self.assertEqual(calls["n"], 2)
            self.assertEqual(len(log), 2)
            self.assertEqual(_kv(db, "USDC_total_volume"), 7)
            self.assertEqual(_kv(db, "USDC_locked"), 3)
        finally:
            reader.close()
            db.close()
            os.remove(path)

    def test_chia_stored_unparsed_row_is_filled_once(self):
        asyncio.run(self._chia_stored_unparsed_row_is_filled_once())

    async def _chia_stored_unparsed_row_is_filled_once(self):
        calls = {"parse": 0}

        def parse_once(db, message):
            calls["parse"] += 1
            return _volume_parser(db, message)

        watcher, db, path, _node, coin_record, bridging = _open_chia(parse_once)
        reader = None
        try:
            def stored_row(nonce):
                return Message(
                    nonce=nonce,
                    source_chain=b"xch",
                    source=b"\x02" * 32,
                    destination_chain=b"eth",
                    destination=b"\x11" * 20,
                    contents=b"\x22" * 32,
                    source_block_number=coin_record.confirmed_block_index,
                    source_timestamp=coin_record.timestamp,
                    source_transaction_hash=nonce,
                    status=MessageStatus.SENT,
                )

            db.add(stored_row(bridging.name()))
            # A second unparsed row keeps the scan on this height after the first
            # row is parsed, so the next pass still sees it and must not add volume.
            db.add(stored_row(b"\xee" * 32))
            db.commit()
            reader, log = _watch_commits(db, path)

            async def policy(**kwargs):
                raise AssertionError("stored row must not be re-evaluated")

            sleeps = {"n": 0}

            async def sleep(_seconds):
                sleeps["n"] += 1
                if sleeps["n"] >= 2:
                    raise asyncio.CancelledError()

            class Node:
                async def get_coin_records_by_puzzle_hash(
                    self, puzzle_hash, include_spent_coins=True, start_height=None
                ):
                    return [coin_record]

                async def get_blockchain_state(self):
                    raise AssertionError("stored row must not wait for confirmations")

                async def get_coin_record_by_name(self, name):
                    raise AssertionError("stored row must not be re-fetched")

                async def get_puzzle_and_solution(self, name, height):
                    raise AssertionError("stored row must not load a spend")

            watcher.getNode = lambda: Node()
            watcher.getDb = lambda: db

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", policy), patch(
                "watchers.chia_watcher.asyncio.sleep", sleep
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await watcher.sentMessageWatcher()

            self.assertEqual(calls["parse"], 1)
            self.assertEqual(sleeps["n"], 2)
            self.assertEqual(len(log), 1)
            self.assertEqual(log[0]["volume"], 7)
            self.assertEqual(log[0]["locked"], 3)
            self.assertEqual(_kv(db, "USDC_total_volume"), 7)
            self.assertEqual(_kv(db, "USDC_locked"), 3)
            stored = db.query(Message).filter(Message.nonce == bridging.name()).one()
            self.assertEqual(stored.parsed, "ok")
            self.assertIsNone(db.query(Message).filter(Message.nonce == b"\xee" * 32).one().parsed)
        finally:
            if reader is not None:
                reader.close()
            db.close()
            os.remove(path)

    def test_evm_unparsed_row_is_counted_once_on_retry(self):
        asyncio.run(self._evm_unparsed_row_is_counted_once_on_retry())

    async def _evm_unparsed_row_is_counted_once_on_retry(self):
        calls = {"n": 0}

        def crash_once(db, message):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("crash before volume")
            return _volume_parser(db, message)

        watcher = _evm_watcher()
        watcher.parse_message_func = crash_once
        db, path = _temp_db()
        reader, log = _watch_commits(db, path)
        try:
            async def accept(**kwargs):
                return PolicyResult("accept", "ok")

            event = _Event((1).to_bytes(32, "big"))
            with patch("watchers.evm_watcher.evaluate_evm_message_policy", accept):
                with self.assertRaises(RuntimeError):
                    await watcher.processSentEvent(None, db, event)

            self.assertEqual(log, [{
                "count": 1,
                "parsed": None,
                "volume": None,
                "locked": None,
            }])
            self.assertEqual(db.query(Message).count(), 1)
            self.assertIsNone(_kv(db, "USDC_total_volume"))

            kind = await watcher.processSentEvent(None, db, event)
            self.assertEqual(kind, "accept")
            self.assertEqual(_kv(db, "USDC_total_volume"), 7)
            self.assertEqual(_kv(db, "USDC_locked"), 3)

            kind = await watcher.processSentEvent(None, db, event)
            self.assertEqual(kind, "accept")
            self.assertEqual(calls["n"], 2)
            self.assertEqual(len(log), 2)
            self.assertTrue(log[1]["parsed"])
            self.assertEqual(log[1]["volume"], 7)
            self.assertEqual(log[1]["locked"], 3)
            self.assertEqual(_kv(db, "USDC_total_volume"), 7)
            self.assertEqual(_kv(db, "USDC_locked"), 3)
        finally:
            reader.close()
            db.close()
            os.remove(path)

    def test_evm_stored_unparsed_message_is_parsed_once_after_load(self):
        asyncio.run(self._evm_stored_unparsed_message_is_parsed_once_after_load())

    async def _evm_stored_unparsed_message_is_parsed_once_after_load(self):
        calls = {"parse": 0}

        def parse_once(db, message):
            calls["parse"] += 1
            return _volume_parser(db, message)

        watcher = _evm_watcher()
        watcher.parse_message_func = parse_once
        watcher.max_block_range = 1000
        db, path = _temp_db()
        reader = None
        try:
            def stored(nonce, block, source_chain, parsed):
                db.add(Message(
                    nonce=nonce.to_bytes(32, "big"),
                    source_chain=source_chain,
                    source=b"\x22" * 20,
                    destination_chain=b"xch",
                    destination=b"\x33" * 32,
                    contents=b"\x44" * 32,
                    source_block_number=block,
                    source_timestamp=1,
                    source_transaction_hash=nonce.to_bytes(32, "big"),
                    status=MessageStatus.SENT,
                    parsed=parsed,
                ))

            unparsed_block = 100
            later_block = 3000
            stored(1, 10, b"bse", None)
            stored(2, 40, b"eth", "ok")
            stored(3, unparsed_block, b"eth", None)
            stored(4, later_block, b"eth", "ok")
            db.commit()
            reader, log = _watch_commits(db, path)

            watcher.loadStateFromDb(db)
            self.assertEqual(watcher.next_nonce, 3)
            self.assertLessEqual(watcher.sent_scan_height, unparsed_block)

            events_by_nonce = {
                watcher.nonceIntToBytes(3): _event(3, unparsed_block),
                watcher.nonceIntToBytes(4): _event(4, later_block),
            }

            async def head(_w3):
                return later_block

            async def query_logs(_w3, _sent_event, from_block, to_block, argument_filters=None):
                nonce_filter = None if argument_filters is None else argument_filters.get("nonce")
                event = events_by_nonce.get(nonce_filter)
                if event is not None and from_block <= event.blockNumber <= to_block:
                    return [event]
                return []

            watcher.get_head_block = head
            watcher.query_event_logs = query_logs
            portal = SimpleNamespace(events=SimpleNamespace(MessageSent=lambda: None))

            async def policy(**kwargs):
                raise AssertionError("stored row must not be re-evaluated")

            with patch("watchers.evm_watcher.evaluate_evm_message_policy", policy):
                await watcher.catchUpSentMessages(None, db, portal)
                self.assertEqual(calls["parse"], 1)
                self.assertEqual(_kv(db, "USDC_total_volume"), 7)
                self.assertEqual(_kv(db, "USDC_locked"), 3)
                self.assertEqual(
                    db.query(Message).filter(Message.nonce == (3).to_bytes(32, "big")).one().parsed,
                    "ok",
                )

                await watcher.catchUpSentMessages(None, db, portal)

            self.assertEqual(calls["parse"], 1)
            self.assertEqual(len(log), 1)
            self.assertEqual(log[0]["volume"], 7)
            self.assertEqual(log[0]["locked"], 3)
            self.assertEqual(_kv(db, "USDC_total_volume"), 7)
            self.assertEqual(_kv(db, "USDC_locked"), 3)
            self.assertIsNone(db.query(Message).filter(Message.source_chain == b"bse").one().parsed)

            watcher.loadStateFromDb(db)
            self.assertEqual(watcher.next_nonce, 5)
            self.assertEqual(watcher.sent_scan_height, watcher.advance_cursor(later_block))
        finally:
            if reader is not None:
                reader.close()
            db.close()
            os.remove(path)


def _sent_message(nonce: bytes, source_chain: bytes, destination_chain: bytes) -> Message:
    return Message(
        nonce=nonce,
        source_chain=source_chain,
        source=b"\x22" * 20,
        destination_chain=destination_chain,
        destination=b"\x33" * 32,
        contents=b"\x44" * 32,
        source_block_number=40,
        source_timestamp=1,
        source_transaction_hash=nonce,
        status=MessageStatus.SENT,
    )


def _received_event(nonce: bytes, block: int, source_chain: bytes) -> _Event:
    event = _Event(nonce)
    event.blockNumber = block
    event.transactionHash = nonce
    event.args.source_chain = source_chain
    return event


def _portal_spend(portal_coin: Coin, new_ph: bytes32, chains_and_nonces):
    inner = Program.to([0, chains_and_nonces, 0])
    solution = Program.to([
        [portal_coin.parent_coin_info, bytes32(b"\x08" * 32), 1],
        1,
        inner,
    ])
    condition = Program.to([51, new_ph, 1])
    puzzle = Program.to(1).cons(Program.to([condition]))
    return CoinSpend(
        portal_coin,
        SerializedProgram.from_bytes(bytes(puzzle)),
        SerializedProgram.from_bytes(bytes(solution)),
    )


class RejectMarkerTests(unittest.TestCase):
    def test_retry_and_hold_do_not_write_reject_marker(self):
        for kind in ("retry", "hold"):
            with self.subTest(kind=kind):
                asyncio.run(self._retry_and_hold_do_not_write_reject_marker(kind))

    async def _retry_and_hold_do_not_write_reject_marker(self, kind: str):
        watcher = _evm_watcher()
        db, path = _temp_db()
        chia, chia_db, chia_path, node, coin_record, _bridging = _open_chia()
        try:
            async def pause(**kwargs):
                return PolicyResult(kind, "later")

            event = _Event((1).to_bytes(32, "big"))
            with patch("watchers.evm_watcher.evaluate_evm_message_policy", pause):
                outcome = await watcher.processSentEvent(None, db, event)
            self.assertEqual(outcome, kind)
            self.assertEqual(db.query(PolicyReject).count(), 0)
            self.assertEqual(db.query(Message).count(), 0)

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", pause):
                outcome = await chia.processCoinRecord(chia_db, node, coin_record)
            self.assertEqual(outcome, kind)
            self.assertEqual(chia_db.query(PolicyReject).count(), 0)
            self.assertEqual(chia_db.query(Message).count(), 0)
            self.assertIsNone(_kv(db, "USDC_total_volume"))
            self.assertIsNone(_kv(chia_db, "USDC_locked"))
        finally:
            db.close()
            chia_db.close()
            os.remove(path)
            os.remove(chia_path)

    def test_evm_reject_marker_lets_receive_continue(self):
        asyncio.run(self._evm_reject_marker_lets_receive_continue())

    async def _evm_reject_marker_lets_receive_continue(self):
        chia, db, path, node, coin_record, bridging = _open_chia()
        try:
            async def reject(**kwargs):
                return PolicyResult("reject", "no")

            with patch("watchers.chia_watcher.evaluate_chia_message_policy", reject):
                kind = await chia.processCoinRecord(db, node, coin_record)

            rejected_nonce = bridging.name()
            self.assertEqual(kind, "reject")
            self.assertEqual(db.query(Message).count(), 0)
            self.assertIsNone(_kv(db, "USDC_total_volume"))
            self.assertIsNone(_kv(db, "USDC_locked"))
            marker = db.query(PolicyReject).one()
            self.assertEqual(marker.source_chain, b"xch")
            self.assertEqual(marker.nonce, rejected_nonce)

            kept_nonce = b"\x99" * 32
            db.add(_sent_message(kept_nonce, b"xch", b"bse"))
            db.commit()

            receiver = _evm_watcher()
            receiver.network_id = "bse"
            receiver.received_scan_height = 1
            receiver.max_block_range = 1000
            events = [
                _received_event(rejected_nonce, 10, b"xch"),
                _received_event(kept_nonce, 20, b"xch"),
            ]

            async def head(_w3):
                return 100

            async def query_logs(_w3, _event, from_block, to_block, argument_filters=None):
                return [event for event in events if from_block <= event.blockNumber <= to_block]

            receiver.get_head_block = head
            receiver.query_event_logs = query_logs

            async def fail_sleep(_seconds):
                raise AssertionError("rejected nonce must not wait")

            portal = SimpleNamespace(events=SimpleNamespace(MessageReceived=lambda: None))
            with patch("watchers.evm_watcher.asyncio.sleep", fail_sleep):
                await asyncio.wait_for(
                    receiver.catchUpReceivedMessages(None, db, portal),
                    timeout=2,
                )

            self.assertEqual(db.query(Message).count(), 1)
            stored = db.query(Message).one()
            self.assertEqual(stored.nonce, kept_nonce)
            self.assertEqual(stored.status, MessageStatus.RECEIVED)
            self.assertEqual(stored.destination_block_number, 20)
            self.assertIsNone(db.query(Message).filter(Message.nonce == rejected_nonce).first())
            self.assertEqual(receiver.received_scan_height, receiver.advance_cursor(100))
            self.assertIsNone(_kv(db, "USDC_total_volume"))
            self.assertIsNone(_kv(db, "USDC_locked"))
        finally:
            db.close()
            os.remove(path)

    def test_chia_reject_marker_lets_portal_spend_continue(self):
        asyncio.run(self._chia_reject_marker_lets_portal_spend_continue())

    async def _chia_reject_marker_lets_portal_spend_continue(self):
        evm = _evm_watcher()
        chia, db, path, _node, _coin_record, _bridging = _open_chia()
        try:
            rejected_nonce = b"\x21" * 32
            kept_nonce = b"\x22" * 32

            async def reject(**kwargs):
                return PolicyResult("reject", "no")

            with patch("watchers.evm_watcher.evaluate_evm_message_policy", reject):
                kind = await evm.processSentEvent(None, db, _Event(rejected_nonce))

            self.assertEqual(kind, "reject")
            self.assertEqual(db.query(Message).count(), 0)
            marker = db.query(PolicyReject).one()
            self.assertEqual(marker.source_chain, b"eth")
            self.assertEqual(marker.nonce, rejected_nonce)
            self.assertIsNone(_kv(db, "USDC_total_volume"))
            self.assertIsNone(_kv(db, "USDC_locked"))

            db.add(_sent_message(kept_nonce, b"eth", b"xch"))
            portal_coin = Coin(bytes32(b"\x10" * 32), bytes32(b"\x11" * 32), 1)
            new_ph = bytes32(b"\x12" * 32)
            spent_height = 50
            coin_record = SimpleNamespace(
                coin=portal_coin,
                spent_block_index=spent_height,
                timestamp=9,
            )
            spend = _portal_spend(
                portal_coin,
                new_ph,
                [(b"eth", rejected_nonce), (b"eth", kept_nonce)],
            )
            db.add(ChiaPortalState(
                chain_id=b"xch",
                coin_id=portal_coin.name(),
                parent_id=portal_coin.parent_coin_info,
                height=40,
            ))
            db.commit()

            class Node:
                async def get_coin_record_by_name(self, name):
                    if name == portal_coin.name():
                        return coin_record
                    return None

                async def get_puzzle_and_solution(self, name, height):
                    return spend

                async def get_block_record_by_height(self, height):
                    return SimpleNamespace(timestamp=77)

            async def fail_sleep(_seconds):
                raise AssertionError("rejected nonce must not wait")

            with patch("watchers.chia_watcher.asyncio.sleep", fail_sleep):
                synced = await asyncio.wait_for(
                    chia.syncPortal(db, Node(), db.query(ChiaPortalState).one()),
                    timeout=2,
                )

            self.assertIsNone(db.query(Message).filter(Message.nonce == rejected_nonce).first())
            kept = db.query(Message).filter(Message.nonce == kept_nonce).one()
            self.assertEqual(kept.status, MessageStatus.RECEIVED)
            self.assertEqual(kept.destination_block_number, spent_height)
            self.assertEqual(kept.destination_timestamp, 77)
            self.assertEqual(kept.destination_transaction_hash, portal_coin.name())
            expected = Coin(portal_coin.name(), new_ph, 1)
            self.assertEqual(synced.coin_id, expected.name())
            self.assertEqual(synced.parent_id, portal_coin.name())
            self.assertEqual(synced.height, spent_height)
            stored = db.query(ChiaPortalState).filter(
                ChiaPortalState.coin_id == expected.name()
            ).one()
            self.assertEqual(stored.parent_id, portal_coin.name())
            self.assertEqual(db.query(Message).count(), 1)
            self.assertIsNone(_kv(db, "USDC_total_volume"))
            self.assertIsNone(_kv(db, "USDC_locked"))
        finally:
            db.close()
            os.remove(path)

    def test_missing_row_without_marker_still_waits(self):
        asyncio.run(self._missing_row_without_marker_still_waits())

    async def _missing_row_without_marker_still_waits(self):
        receiver = _evm_watcher()
        receiver.network_id = "bse"
        db, path = _temp_db()
        chia, chia_db, chia_path, _node, _coin_record, _bridging = _open_chia()
        try:
            sleeps = []

            async def evm_sleep(seconds):
                sleeps.append(seconds)
                raise asyncio.CancelledError()

            event = _received_event((1).to_bytes(32, "big"), 10, b"xch")
            with patch("watchers.evm_watcher.asyncio.sleep", evm_sleep):
                with self.assertRaises(asyncio.CancelledError):
                    await receiver.processReceivedEvent(None, db, event)

            self.assertEqual(sleeps, [5])
            self.assertEqual(db.query(Message).count(), 0)
            self.assertEqual(db.query(PolicyReject).count(), 0)
            self.assertEqual(receiver.received_scan_height, receiver.min_height)

            portal_coin = Coin(bytes32(b"\x10" * 32), bytes32(b"\x11" * 32), 1)
            nonce = b"\x21" * 32
            spend = _portal_spend(portal_coin, bytes32(b"\x12" * 32), [(b"eth", nonce)])
            coin_record = SimpleNamespace(
                coin=portal_coin,
                spent_block_index=50,
                timestamp=9,
            )
            chia_db.add(ChiaPortalState(
                chain_id=b"xch",
                coin_id=portal_coin.name(),
                parent_id=portal_coin.parent_coin_info,
                height=40,
            ))
            chia_db.commit()
            before = chia_db.query(ChiaPortalState).one().coin_id

            class Node:
                async def get_coin_record_by_name(self, name):
                    if name == portal_coin.name():
                        return coin_record
                    return None

                async def get_puzzle_and_solution(self, name, height):
                    return spend

                async def get_block_record_by_height(self, height):
                    raise AssertionError("a missing source message must not finish the spend")

            chia_sleeps = []

            async def chia_sleep(seconds):
                chia_sleeps.append(seconds)
                raise asyncio.CancelledError()

            with patch("watchers.chia_watcher.asyncio.sleep", chia_sleep):
                with self.assertRaises(asyncio.CancelledError):
                    await chia.syncPortal(chia_db, Node(), chia_db.query(ChiaPortalState).one())

            self.assertEqual(chia_sleeps, [10])
            self.assertEqual(chia_db.query(Message).count(), 0)
            self.assertEqual(chia_db.query(PolicyReject).count(), 0)
            self.assertEqual(chia_db.query(ChiaPortalState).one().coin_id, before)
        finally:
            db.close()
            chia_db.close()
            os.remove(path)
            os.remove(chia_path)

    def test_http_error_log_includes_body_and_403_is_not_retried(self):
        from watchers.http_rpc import RpcHttpError

        watcher = _evm_watcher()
        body = '{"jsonrpc":"2.0","error":{"code":-32005,"message":"Too Many Requests"}}'
        limited = RpcHttpError(429, "Too Many Requests", body)
        forbidden = RpcHttpError(
            403,
            "Forbidden",
            '{"jsonrpc":"2.0","error":{"code":-32005,"message":"daily request count exceeded, request rate limited"}}',
        )
        self.assertIn(body, watcher.format_error(limited))
        self.assertNotIn("RequestInfo", watcher.format_error(limited))
        self.assertTrue(watcher.is_transient_rpc_error(limited))
        self.assertFalse(watcher.is_transient_rpc_error(forbidden))
        self.assertTrue(watcher.is_transient_rpc_error(TimeoutError("timed out")))
        watcher.rpc_url = "https://mainnet.infura.io/v3/secretkey"
        leaked = RpcHttpError(429, "Too Many Requests", "see " + watcher.rpc_url)
        self.assertNotIn("secretkey", watcher.format_error(leaked))

    def test_rate_limit_backoff_grows_and_then_resets(self):
        chia, db, path, _node, _coin_record, _bridging = _open_chia()
        try:
            self.assertTrue(is_rpc_rate_limited(SimpleNamespace(status=429, message="Too Many Requests")))
            self.assertFalse(is_rpc_rate_limited(SimpleNamespace(status=500, message="no")))
            self.assertEqual(
                [chia.retry_delay_seconds("rpc rate limited") for _ in range(12)],
                [5, 10, 15, 20, 30, 45, 60, 120, 180, 240, 300, 300],
            )
            self.assertEqual(chia.retry_delay_seconds("missing WrappedCAT bytecode or views"), 30)
            self.assertEqual(chia.retry_delay_seconds("rpc rate limited"), 5)
        finally:
            db.close()
            os.remove(path)

    def test_low_toll_coin_is_not_logged_on_the_next_pass(self):
        chia, _db, path, _node, _coin_record, _bridging = _open_chia()
        try:
            nonce = bytes32(b"\x44" * 32)
            first_pass = []
            chia.remember_low_toll(nonce, first_pass)
            self.assertEqual(first_pass, [nonce])
            self.assertIn(nonce, chia.skipped_coin_ids)

            second_pass = list(chia.skipped_coin_ids)
            self.assertIn(nonce, second_pass)
            chia.remember_low_toll(nonce, second_pass)
            self.assertEqual(second_pass.count(nonce), 1)
        finally:
            _db.close()
            os.remove(path)

    def test_portal_wait_sees_row_committed_by_the_other_watcher(self):
        asyncio.run(self._portal_wait_sees_row_committed_by_the_other_watcher())

    async def _portal_wait_sees_row_committed_by_the_other_watcher(self):
        chia, db, path, _node, _coin_record, _bridging = _open_chia()
        other = setup_database("sqlite:///" + path)
        try:
            nonce = b"\x21" * 32
            portal_coin = Coin(bytes32(b"\x10" * 32), bytes32(b"\x11" * 32), 1)
            new_ph = bytes32(b"\x12" * 32)
            spend = _portal_spend(portal_coin, new_ph, [(b"bse", nonce)])
            coin_record = SimpleNamespace(
                coin=portal_coin,
                spent_block_index=50,
                timestamp=9,
            )
            db.add(ChiaPortalState(
                chain_id=b"xch",
                coin_id=portal_coin.name(),
                parent_id=portal_coin.parent_coin_info,
                height=40,
            ))
            db.commit()

            class Node:
                async def get_coin_record_by_name(self, name):
                    if name == portal_coin.name():
                        return coin_record
                    return None

                async def get_puzzle_and_solution(self, name, height):
                    return spend

                async def get_block_record_by_height(self, height):
                    return SimpleNamespace(timestamp=77)

            inserted = False

            async def sleep(seconds):
                nonlocal inserted
                self.assertEqual(seconds, 10)
                if inserted:
                    raise AssertionError("portal is still waiting after bse-4 was committed")
                other.add(_sent_message(nonce, b"bse", b"xch"))
                other.commit()
                inserted = True

            with patch("watchers.chia_watcher.asyncio.sleep", sleep):
                synced = await asyncio.wait_for(
                    chia.syncPortal(db, Node(), db.query(ChiaPortalState).one()),
                    timeout=2,
                )

            stored = db.query(Message).filter(Message.nonce == nonce).one()
            self.assertEqual(stored.status, MessageStatus.RECEIVED)
            self.assertEqual(stored.destination_block_number, 50)
            self.assertEqual(synced.height, 50)
        finally:
            other.close()
            db.close()
            os.remove(path)

    def test_sent_catchup_runs_before_the_websocket(self):
        asyncio.run(self._sent_catchup_runs_before_the_websocket())

    async def _sent_catchup_runs_before_the_websocket(self):
        watcher = _evm_watcher()
        db, path = _temp_db()
        order = []
        try:
            watcher.getDb = lambda: db

            async def catch_sent(*_args, **_kwargs):
                order.append("sent")

            async def catch_received(*_args, **_kwargs):
                order.append("received")

            watcher.catchUpSentMessages = catch_sent
            watcher.catchUpReceivedMessages = catch_received

            class WS:
                async def __aenter__(self):
                    order.append("ws")
                    raise asyncio.CancelledError()

                async def __aexit__(self, *_args):
                    return False

            with patch("watchers.evm_watcher.AsyncWeb3") as web3_cls:
                web3_cls.return_value.eth.contract.return_value = object()
                web3_cls.persistent_websocket.return_value = WS()
                with self.assertRaises(asyncio.CancelledError):
                    await watcher.runSubscriptionSession()
            self.assertEqual(order, ["sent", "received", "ws"])
        finally:
            db.close()
            os.remove(path)


if __name__ == "__main__":
    unittest.main()
