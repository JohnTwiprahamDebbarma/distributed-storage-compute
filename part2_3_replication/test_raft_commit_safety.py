"""
Regression test for a linearizability bug in RaftNode (raft_node.py).

THE BUG
-------
`append_entry()` blocks until the entry it just appended is committed, using a
notification keyed by *log index only* (`_commit_events[idx]`). The applier
fires that notification for whatever entry ends up committed at that index.

Those are not always the same entry. If a leader appends a client's write at
index N, then loses leadership before replicating it, a new leader can overwrite
index N with a different entry and commit *that*. The old leader's applier then
fires the waiter for index N -- with the new entry's result -- so `append_entry`
returns success even though the client's write was silently discarded. The
gRPC layer (RaftDSFSServicer.Close) then replies `success=True` for a write
that no longer exists in the log. That is a lost acknowledged write: a
linearizability violation.

THE FIX
-------
Tag each waiter with the identity (term, client_id, seq_num) of the entry the
caller appended, and:
  * when the node stops being leader (`_become_follower`), fail every pending waiter
    with NotLeaderError so the client retries against the new leader; and
  * before handing a result back in the applier, confirm the entry that
    actually committed at that index is the caller's own.

This test drives the RPC handlers directly -- no gRPC, no generated stubs, no
running cluster -- so `python3 test_raft_commit_safety.py` reproduces the whole
scenario in under a second.
"""
import logging
import tempfile
import threading
import time
import unittest

import raft_node
from raft_node import RaftNode, NotLeaderError

# Keep the randomised election timer far out of the way of these tests and
# silence Raft's INFO logging so the test output is clean.
raft_node.ELECTION_TIMEOUT_MIN = 1000.0
raft_node.ELECTION_TIMEOUT_MAX = 1001.0
logging.disable(logging.CRITICAL)


def _make_leader(apply_fn):
    """A 3-node RaftNode (2 peers, so majority = 2) already leader in term 1."""
    node = RaftNode(node_id=0, peers={1: "peer1:1", 2: "peer2:2"},
                    persist_dir=tempfile.mkdtemp(prefix="raft_test_"),
                    apply_fn=apply_fn)
    with node.lock:
        node.current_term = 1
        node._become_leader()   # log = [noop@term1 at index 1]
    return node


def _append_in_thread(node, client_id, seq_num):
    """Start node.append_entry(write) in a background thread; return (thread, outcome)."""
    outcome = {}

    def run():
        try:
            outcome["returned"] = node.append_entry(
                op_type="write", filename="ledger.txt", data=b"the-clients-write",
                client_id=client_id, seq_num=seq_num, timeout=5.0)
        except Exception as e:
            outcome["raised"] = e

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t, outcome


def _wait_for_pending_write(node, client_id, timeout=2.0):
    """Block until the client's write entry is in the log (waiter registered)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        with node.lock:
            if node.log and node.log[-1].get("client_id") == client_id:
                return
        time.sleep(0.01)
    raise AssertionError("client write never reached the log")


class RaftCommitSafetyTest(unittest.TestCase):

    def test_overwritten_entry_is_not_reported_committed(self):
        """A write overwritten by a new leader must NOT be reported as committed."""
        applied = []
        node = _make_leader(lambda e: applied.append(e["client_id"]) or {"new_version": 2})

        # Client A's write lands at index 2 but is never replicated (this node is
        # about to be partitioned away and superseded).
        t, outcome = _append_in_thread(node, client_id="A", seq_num=1)
        _wait_for_pending_write(node, "A")
        self.assertEqual(node.log[1]["client_id"], "A")

        # A new leader for term 2 sends an AppendEntries that overwrites index 2
        # with its own no-op and commits it.
        reply = node.handle_append_entries({
            "term": 2, "leader_id": 1,
            "prev_log_index": 1, "prev_log_term": 1,
            "entries": [{"term": 2, "index": 2, "op_type": "noop", "filename": "",
                         "data": "", "client_id": "", "seq_num": 0}],
            "leader_commit": 2,
        })
        self.assertTrue(reply["success"])
        t.join(timeout=6)
        self.assertFalse(t.is_alive(), "append_entry never returned")

        # Client A's write was discarded from the log ...
        self.assertNotEqual(node.log[1]["client_id"], "A")
        self.assertNotIn("A", applied)
        # ... so append_entry must report failure, not success. On the buggy code
        # it returns normally here (the lost-write bug); the fix raises instead.
        self.assertNotIn("returned", outcome,
                         "BUG: append_entry reported success for a discarded write")
        self.assertIn("raised", outcome)
        self.assertIsInstance(outcome["raised"], NotLeaderError)

    def test_normally_committed_write_still_succeeds(self):
        """The fix must not break the happy path: a committed write returns its result."""
        applied = []
        node = _make_leader(lambda e: applied.append(e["client_id"]) or {"new_version": 2})

        t, outcome = _append_in_thread(node, client_id="B", seq_num=1)
        _wait_for_pending_write(node, "B")
        idx = node.log[-1]["index"]

        # Simulate one peer acknowledging replication -> majority (self + peer 1).
        with node.lock:
            node.match_index[1] = idx
            node._advance_commit_index()   # commits index idx, wakes the applier

        t.join(timeout=6)
        self.assertFalse(t.is_alive(), "append_entry never returned")
        self.assertEqual(outcome.get("returned"), {"new_version": 2})
        self.assertIn("B", applied)


if __name__ == "__main__":
    unittest.main(verbosity=2)
