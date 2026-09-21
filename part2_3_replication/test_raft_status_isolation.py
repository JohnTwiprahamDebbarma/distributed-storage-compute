"""
Hermetic tests for RaftNode.status() and the isolate-node fault injection
(raft_node.py).

Like test_raft_commit_safety.py this needs no gRPC, generated stubs or running
cluster: the peers are fake stubs that count the RPCs they receive, and a
stand-in raft_pb2 module lets the send paths build their requests.

    python3 test_raft_status_isolation.py
"""
import logging
import sys
import tempfile
import time
import types
import unittest
from unittest import mock

import raft_node
from raft_node import RaftNode

# Keep the randomised election timer far out of the way of these tests and
# silence Raft's INFO logging so the test output is clean.
raft_node.ELECTION_TIMEOUT_MIN = 1000.0
raft_node.ELECTION_TIMEOUT_MAX = 1001.0
logging.disable(logging.CRITICAL)


class _Message:
    """Stand-in for a generated protobuf message: it only keeps its fields."""
    def __init__(self, **fields):
        self.__dict__.update(fields)


FAKE_RAFT_PB2 = types.SimpleNamespace(
    LogEntry=_Message, AppendEntriesArgs=_Message, RequestVoteArgs=_Message)


class CountingStub:
    """Stands in for a peer's RaftServiceStub: counts the RPCs that reach it,
    then fails them the way an unreachable peer would."""
    def __init__(self):
        self.calls = 0

    def _call(self, request, timeout=None):
        self.calls += 1
        raise ConnectionError("peer unreachable")

    AppendEntries = _call
    RequestVote = _call


def _make_node():
    """Node 0 of a 3-node cluster whose two peers are CountingStubs."""
    node = RaftNode(node_id=0, peers={1: "peer1:1", 2: "peer2:2"},
                    persist_dir=tempfile.mkdtemp(prefix="raft_test_"))
    node.peer_stubs = {1: CountingStub(), 2: CountingStub()}
    return node


def _make_leader(node):
    """Leader for term 1, without the background heartbeat thread, so each test
    sends heartbeats explicitly and nothing races its assertions."""
    node._start_heartbeat = lambda: None
    with node.lock:
        node.current_term = 1
        node._become_leader()   # log = [noop@term1 at index 1]


def _send_heartbeats(node):
    for peer_id, stub in node.peer_stubs.items():
        node._send_append_entries_to(peer_id, stub)


def _calls(node):
    return sum(stub.calls for stub in node.peer_stubs.values())


class StatusTest(unittest.TestCase):

    def test_follower_status_reflects_what_the_leader_sent(self):
        node = _make_node()
        node.handle_append_entries({
            "term": 3, "leader_id": 2, "prev_log_index": 0, "prev_log_term": 0,
            "entries": [{"term": 3, "index": 1, "op_type": "noop", "filename": "",
                         "data": "", "client_id": "", "seq_num": 0}],
            "leader_commit": 1,
        })
        s = node.status()
        self.assertEqual((s["state"], s["term"], s["leader_id"]), ("follower", 3, 2))
        self.assertEqual((s["last_log_index"], s["last_log_term"], s["commit_index"]),
                         (1, 3, 1))
        self.assertFalse(s["isolated"])
        self.assertEqual(s["match_index"], {})   # only a leader tracks replication

    def test_leader_status_includes_replication_progress(self):
        node = _make_node()
        _make_leader(node)
        s = node.status()
        self.assertEqual((s["state"], s["term"], s["leader_id"]), ("leader", 1, 0))
        # its own no-op is at index 1; neither follower has acknowledged it yet
        self.assertEqual(s["match_index"], {0: 1, 1: 0, 2: 0})


class IsolationTest(unittest.TestCase):

    def setUp(self):
        # Let the send paths build their requests without generated gRPC stubs.
        patcher = mock.patch.dict(sys.modules, {"raft_pb2": FAKE_RAFT_PB2})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_connected_leader_sends_heartbeats(self):
        # Control for the next test: without isolation the heartbeats go out.
        node = _make_node()
        _make_leader(node)
        _send_heartbeats(node)
        self.assertEqual(_calls(node), 2)

    def test_isolated_leader_sends_nothing(self):
        node = _make_node()
        _make_leader(node)
        node.set_isolated(True)
        _send_heartbeats(node)
        self.assertEqual(_calls(node), 0)
        s = node.status()
        self.assertTrue(s["isolated"])
        self.assertEqual(s["state"], "leader")   # it still believes it leads

    def _lose_one_election(self, node):
        node._reset_election_timer = lambda: None   # no retry once this election is lost
        with mock.patch.object(raft_node, "ELECTION_TIMEOUT_MIN", 0.1):
            node._start_election()                  # waits 0.1 s for votes that never come

    def test_connected_candidate_requests_votes(self):
        # Control for the next test: without isolation the vote requests go out.
        node = _make_node()
        self._lose_one_election(node)
        deadline = time.time() + 2
        while _calls(node) < 2 and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(_calls(node), 2)

    def test_isolated_candidate_requests_no_votes(self):
        node = _make_node()
        node.set_isolated(True)
        self._lose_one_election(node)
        self.assertEqual(_calls(node), 0)
        # Its term still rises on every timeout. That is why a healed node can
        # disrupt the cluster, and what Raft's PreVote extension prevents.
        s = node.status()
        self.assertEqual((s["state"], s["term"]), ("candidate", 1))

    def test_heal_after_reconnects_automatically(self):
        node = _make_node()
        node.set_isolated(True, heal_after=0.2)
        self.assertTrue(node.isolated)
        deadline = time.time() + 3
        while node.isolated and time.time() < deadline:
            time.sleep(0.05)
        self.assertFalse(node.isolated)


if __name__ == "__main__":
    unittest.main(verbosity=2)
