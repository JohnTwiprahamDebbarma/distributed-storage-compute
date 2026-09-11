"""
Hermetic unit tests for the KV state machine (no gRPC, no cluster, no Raft).

The state machine is the deterministic core every replica runs, so testing it in
isolation pins down the semantics (versioning, delete, compare-and-swap,
snapshot/restore) without spinning up a cluster.

    python3 test_kv_state_machine.py
"""

import base64
import json
import unittest

from kv_state_machine import KVStateMachine


def _put(sm, key, value):
    return sm.apply({"op_type": "put", "filename": key, "data": value})


def _delete(sm, key):
    return sm.apply({"op_type": "delete", "filename": key, "data": b""})


def _cas(sm, key, expected, new, expect_absent=False):
    cmd = json.dumps({
        "expected": base64.b64encode(expected).decode(),
        "new": base64.b64encode(new).decode(),
        "expect_absent": expect_absent,
    }).encode()
    return sm.apply({"op_type": "cas", "filename": key, "data": cmd})


class KVStateMachineTest(unittest.TestCase):

    def test_put_get_and_versioning(self):
        sm = KVStateMachine()
        self.assertEqual(_put(sm, "a", b"1")["version"], 1)
        self.assertEqual(_put(sm, "a", b"2")["version"], 2)   # version bumps
        self.assertEqual(sm.get("a"), (b"2", 2, True))
        self.assertEqual(sm.get("missing"), (None, 0, False))

    def test_delete_reports_existence(self):
        sm = KVStateMachine()
        _put(sm, "a", b"1")
        self.assertTrue(_delete(sm, "a")["existed"])
        self.assertFalse(_delete(sm, "a")["existed"])
        self.assertEqual(sm.get("a"), (None, 0, False))

    def test_cas_match_and_mismatch(self):
        sm = KVStateMachine()
        _put(sm, "a", b"1")
        self.assertTrue(_cas(sm, "a", b"1", b"2")["swapped"])
        self.assertEqual(sm.get("a")[0], b"2")

        mismatch = _cas(sm, "a", b"1", b"3")   # stale expected value
        self.assertFalse(mismatch["swapped"])
        self.assertEqual(mismatch["current"], b"2")   # returns the real value
        self.assertEqual(sm.get("a")[0], b"2")         # unchanged

    def test_cas_absent_is_create_if_not_exists(self):
        sm = KVStateMachine()
        self.assertTrue(_cas(sm, "x", b"", b"v", expect_absent=True)["swapped"])
        self.assertFalse(_cas(sm, "x", b"", b"w", expect_absent=True)["swapped"])
        self.assertEqual(sm.get("x")[0], b"v")

    def test_snapshot_and_restore_roundtrip(self):
        sm = KVStateMachine()
        _put(sm, "a", b"1")
        _put(sm, "b", b"\x00\xff\x10")   # arbitrary bytes survive
        blob = sm.snapshot()

        restored = KVStateMachine()
        restored.apply({"op_type": "_snapshot", "snapshot_data": blob})
        self.assertEqual(restored.get("a"), (b"1", 1, True))
        self.assertEqual(restored.get("b"), (b"\x00\xff\x10", 1, True))


if __name__ == "__main__":
    unittest.main(verbosity=2)
