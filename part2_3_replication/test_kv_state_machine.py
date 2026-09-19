"""
Hermetic unit tests for the KV state machine (no gRPC, no cluster, no Raft).

The state machine is the deterministic core every replica runs, so testing it in
isolation pins down the semantics (versioning, delete, value- and version-based
compare-and-swap, replicated de-duplication, snapshot/restore) without spinning
up a cluster.

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


def _cas_version(sm, key, expected_version, new):
    cmd = json.dumps({
        "new": base64.b64encode(new).decode(),
        "expected_version": expected_version,
    }).encode()
    return sm.apply({"op_type": "cas", "filename": key, "data": cmd})


# Entries tagged with the (client_id, seq_num) of the request that created them,
# as the leader appends them for real clients.
def _put_by(client_id, seq_num, key, value):
    return {"op_type": "put", "filename": key, "data": value,
            "client_id": client_id, "seq_num": seq_num}


def _cas_by(client_id, seq_num, key, expected, new):
    cmd = json.dumps({
        "expected": base64.b64encode(expected).decode(),
        "new": base64.b64encode(new).decode(),
    }).encode()
    return {"op_type": "cas", "filename": key, "data": cmd,
            "client_id": client_id, "seq_num": seq_num}


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


class VersionCasTest(unittest.TestCase):

    def test_cas_by_version_match_and_mismatch(self):
        sm = KVStateMachine()
        _put(sm, "a", b"1")                                    # version 1
        ok = _cas_version(sm, "a", 1, b"2")
        self.assertEqual((ok["swapped"], ok["version"]), (True, 2))

        stale = _cas_version(sm, "a", 1, b"3")                 # version moved on
        self.assertEqual((stale["swapped"], stale["version"]), (False, 2))
        self.assertEqual(sm.get("a"), (b"2", 2, True))          # unchanged

    def test_version_cas_is_not_fooled_by_aba(self):
        sm = KVStateMachine()
        _put(sm, "a", b"A")      # a client reads (A, version 1) ...
        _put(sm, "a", b"B")      # ... someone changes it ...
        _put(sm, "a", b"A")      # ... and back: same value, version 3
        self.assertFalse(_cas_version(sm, "a", 1, b"X")["swapped"])   # change detected
        self.assertTrue(_cas(sm, "a", b"A", b"X")["swapped"])         # value CAS misses it

    def test_versions_never_repeat_across_delete_and_recreate(self):
        sm = KVStateMachine()
        _put(sm, "a", b"1")
        _put(sm, "a", b"2")                                     # version 2
        _delete(sm, "a")
        self.assertEqual(_put(sm, "a", b"new")["version"], 3)   # not back to 1
        self.assertFalse(_cas_version(sm, "a", 1, b"x")["swapped"])

    def test_cas_by_version_on_absent_key(self):
        sm = KVStateMachine()
        miss = _cas_version(sm, "missing", 1, b"v")
        self.assertEqual((miss["swapped"], miss["version"]), (False, 0))
        self.assertEqual(sm.get("missing"), (None, 0, False))


class SessionDedupTest(unittest.TestCase):

    def test_retried_write_is_applied_once(self):
        sm = KVStateMachine()
        first = sm.apply(_put_by("c1", 1, "k", b"x"))
        retry = sm.apply(_put_by("c1", 1, "k", b"x"))    # the same request again
        self.assertEqual(retry, first)                     # the original answer
        self.assertEqual(sm.get("k"), (b"x", 1, True))     # version not bumped twice

    def test_retried_cas_returns_its_original_outcome(self):
        sm = KVStateMachine()
        _put(sm, "k", b"1")
        self.assertTrue(sm.apply(_cas_by("c1", 1, "k", b"1", b"2"))["swapped"])
        # Re-running the CAS would compare against the value it already changed
        # and wrongly report swapped=False; the session table returns the truth.
        self.assertTrue(sm.apply(_cas_by("c1", 1, "k", b"1", b"2"))["swapped"])
        self.assertEqual(sm.get("k"), (b"2", 2, True))

    def test_late_older_request_is_not_applied(self):
        sm = KVStateMachine()
        sm.apply(_put_by("c1", 2, "k", b"new"))
        late = sm.apply(_put_by("c1", 1, "k", b"old"))   # delayed duplicate of seq 1
        self.assertTrue(late.get("stale"))
        self.assertEqual(sm.get("k"), (b"new", 1, True))

    def test_clients_are_tracked_independently(self):
        sm = KVStateMachine()
        sm.apply(_put_by("c1", 1, "k", b"a"))
        sm.apply(_put_by("c2", 1, "k", b"b"))            # same seq, different client
        self.assertEqual(sm.get("k"), (b"b", 2, True))

    def test_lookup_session_sees_only_applied_requests(self):
        sm = KVStateMachine()
        self.assertIsNone(sm.lookup_session("c1", 1))
        sm.apply(_put_by("c1", 1, "k", b"x"))
        self.assertEqual(sm.lookup_session("c1", 1)["version"], 1)
        self.assertIsNone(sm.lookup_session("c1", 2))    # not applied yet
        self.assertIsNone(sm.lookup_session("", 1))      # untracked request

    def test_sessions_and_tombstones_survive_snapshot(self):
        sm = KVStateMachine()
        _put(sm, "k", b"1")
        sm.apply(_cas_by("c1", 1, "k", b"1", b"\x00\xff"))   # cached result holds raw bytes
        _put(sm, "gone", b"x")
        _delete(sm, "gone")                                   # tombstone at version 1

        restored = KVStateMachine()
        restored.apply({"op_type": "_snapshot", "snapshot_data": sm.snapshot()})
        retry = restored.apply(_cas_by("c1", 1, "k", b"1", b"\x00\xff"))
        self.assertEqual((retry["swapped"], retry["current"]), (True, b"\x00\xff"))
        self.assertEqual(restored.get("k"), (b"\x00\xff", 2, True))    # not re-applied
        self.assertEqual(_put(restored, "gone", b"y")["version"], 2)   # numbering continues

    def test_session_table_is_bounded(self):
        sm = KVStateMachine(max_sessions=2)
        sm.apply(_put_by("c1", 1, "k", b"v"))
        sm.apply(_put_by("c2", 1, "k", b"v"))
        sm.apply(_put_by("c1", 2, "k", b"v"))   # c1 is active again
        sm.apply(_put_by("c3", 1, "k", b"v"))   # table full: evict c2, the idlest
        self.assertEqual(list(sm.sessions), ["c1", "c3"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
