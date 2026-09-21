"""
gRPC client for the Raft key-value cluster: the only part of the gateway that
talks to the nodes (the "repository" layer).

It keeps one channel per node and remembers which node is the leader. A call
goes to the presumed leader; a NOT_LEADER reply redirects it, and an unreachable
node sends it on to the next one, with capped exponential backoff, until the
request's deadline runs out. Retrying a write is safe because every write
carries a (client_id, seq_num) that the store de-duplicates (kv_state_machine.py).

I wrote this instead of reusing kv_client_stub.py because that stub is one client
object making one call at a time, while this client is shared by every request
the web server handles concurrently.
"""

import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc

# The generated stubs live in ../../../part2_3_replication (setup_raft.sh there
# generates them). I append rather than insert so nothing there shadows this package.
_KV_DIR = str(Path(__file__).resolve().parents[3] / "part2_3_replication")
if _KV_DIR not in sys.path:
    sys.path.append(_KV_DIR)

import kv_pb2  # noqa: E402
import kv_pb2_grpc  # noqa: E402
import raft_pb2  # noqa: E402
import raft_pb2_grpc  # noqa: E402

from gateway.domain import ClusterUnavailable, NodeStatus, StoreError, WriteOutcomeUnknown  # noqa: E402

# Transport failures worth retrying on another node.
_RETRYABLE = (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)

# I shorten gRPC's reconnect backoff (its default grows to 2 min) so a channel
# reconnects soon after its node comes back.
_CHANNEL_OPTIONS = [
    ("grpc.initial_reconnect_backoff_ms", 200),
    ("grpc.max_reconnect_backoff_ms", 2000),
]


class _Node:
    def __init__(self, node_id: int, address: str):
        self.node_id = node_id
        self.address = address
        self.channel = grpc.insecure_channel(address, options=_CHANNEL_OPTIONS)
        self.kv = kv_pb2_grpc.KVServiceStub(self.channel)
        self.raft = raft_pb2_grpc.RaftServiceStub(self.channel)


class RaftCluster:
    def __init__(self, addresses, rpc_timeout: float = 3.0,
                 request_deadline: float = 12.0, status_ttl: float = 0.25):
        if not addresses:
            raise ValueError("at least one node address is required")
        self.nodes = [_Node(i, addr) for i, addr in enumerate(addresses)]
        self.node_count = len(self.nodes)
        self._by_address = {n.address: n for n in self.nodes}
        self.rpc_timeout = rpc_timeout
        self.request_deadline = request_deadline

        self._lock = threading.Lock()
        self._leader = self.nodes[0]            # a guess until a node names the real leader

        # I cache the status briefly so any number of dashboards polling at once
        # cost the nodes the same as one; the lock makes concurrent callers
        # share a single fetch instead of each starting their own.
        self._status_lock = threading.Lock()
        self._status_ttl = status_ttl
        self._status_cache = (0.0, None)
        self._pool = ThreadPoolExecutor(max_workers=self.node_count,
                                        thread_name_prefix="kv-status")

    # Key-value operations
    def get(self, key: str, linearizable: bool = False):
        """Return (found, value, version) as the leader sees it."""
        resp = self._call("Get", kv_pb2.GetRequest(key=key, linearizable=linearizable),
                          write=False)
        return resp.found, resp.value, resp.version

    def put(self, key: str, value: bytes, client_id: str, seq_num: int) -> int:
        """Set key=value; return the new version."""
        req = kv_pb2.PutRequest(key=key, value=value, client_id=client_id, seq_num=seq_num)
        return self._call("Put", req, write=True).version

    def cas(self, key: str, new_value: bytes, client_id: str, seq_num: int, *,
            expected_version: int = 0, expect_absent: bool = False):
        """Compare-and-swap by version, or create-only; return (swapped, version).

        version is the new version if it swapped, else the key's current one.
        """
        req = kv_pb2.CasRequest(key=key, new_value=new_value,
                                expected_version=expected_version,
                                expect_absent=expect_absent,
                                client_id=client_id, seq_num=seq_num)
        resp = self._call("Cas", req, write=True)
        return resp.swapped, resp.version

    def delete(self, key: str, client_id: str, seq_num: int) -> bool:
        """Delete key; return whether it existed."""
        req = kv_pb2.DeleteRequest(key=key, client_id=client_id, seq_num=seq_num)
        return self._call("Delete", req, write=True).existed

    # Admin
    def node_statuses(self) -> list[NodeStatus]:
        """Every node's own report of its state, fetched in parallel."""
        with self._status_lock:
            fetched_at, cached = self._status_cache
            if cached is not None and time.monotonic() - fetched_at < self._status_ttl:
                return cached
            statuses = list(self._pool.map(self._status_of, self.nodes))
            self._status_cache = (time.monotonic(), statuses)
            return statuses

    def set_isolated(self, node_id: int, isolated: bool, heal_after: float = 0.0) -> bool:
        """Cut a node off from the cluster, or reconnect it (fault injection)."""
        node = self.nodes[node_id]
        req = raft_pb2.SetIsolatedRequest(isolated=isolated, heal_after_seconds=heal_after)
        try:
            reply = node.raft.SetIsolated(req, timeout=self.rpc_timeout)
        except grpc.RpcError as e:
            raise ClusterUnavailable(
                f"node {node_id} ({node.address}) is unreachable: {e.code().name}") from None
        with self._status_lock:
            self._status_cache = (0.0, None)    # show the change on the next status read
        return reply.isolated

    def close(self):
        self._pool.shutdown(wait=False)
        for node in self.nodes:
            node.channel.close()

    # Internals
    def _status_of(self, node: _Node) -> NodeStatus:
        try:
            s = node.raft.GetStatus(raft_pb2.GetStatusRequest(), timeout=1.0)
        except grpc.RpcError:
            return NodeStatus(node_id=node.node_id, address=node.address, reachable=False)
        return NodeStatus(
            node_id=node.node_id, address=node.address, reachable=True,
            state=s.state, term=s.term,
            leader_id=s.leader_id if s.has_leader else None,
            commit_index=s.commit_index, last_applied=s.last_applied,
            last_log_index=s.last_log_index, last_log_term=s.last_log_term,
            isolated=s.isolated, match_index=dict(s.match_index))

    def _call(self, method: str, request, write: bool):
        """Send one KV RPC to the leader, following redirects and failing over."""
        deadline = time.monotonic() + self.request_deadline
        backoff = 0.05
        node = self._leader_hint()
        unreachable = set()     # nodes that failed at the transport level this request
        redirects = 0           # consecutive redirects followed without a pause
        # A write whose attempt failed in transit may have reached the leader and
        # committed. Retrying it is still safe (the store de-duplicates it), but
        # if time runs out it has to be reported as "maybe applied", never as
        # "not applied".
        maybe_applied = False
        last_problem = "no attempt finished"

        while True:
            timeout = min(self.rpc_timeout, deadline - time.monotonic())
            if timeout <= 0:
                break
            try:
                resp = getattr(node.kv, method)(request, timeout=timeout)
            except grpc.RpcError as e:
                if e.code() not in _RETRYABLE:
                    raise StoreError(f"{node.address}: {e.code().name}: {e.details()}") from None
                maybe_applied = maybe_applied or write
                unreachable.add(node.node_id)
                last_problem = f"{node.address} {e.code().name}"
                node = self._next_node(node, unreachable)
                redirects = 0
            else:
                if resp.success:
                    self._set_leader_hint(node)
                    return resp
                if not resp.error_message.startswith("NOT_LEADER:"):
                    raise StoreError(f"{node.address}: {resp.error_message}")
                last_problem = f"{node.address} is not the leader"
                target = self._redirect_target(resp.error_message)
                if (target is not None and target is not node
                        and target.node_id not in unreachable and redirects < 3):
                    node, redirects = target, redirects + 1
                    continue    # follow the redirect without pausing
                # No usable leader yet (an election is running): try another node.
                node = self._next_node(node, unreachable)
                redirects = 0

            # Pause before the next attempt; the jitter keeps concurrent requests
            # from retrying in lockstep.
            pause = backoff * random.uniform(0.5, 1.0)
            if time.monotonic() + pause >= deadline:
                break
            time.sleep(pause)
            backoff = min(backoff * 2, 0.5)

        if maybe_applied:
            raise WriteOutcomeUnknown(
                f"write not confirmed within {self.request_deadline:g}s ({last_problem})")
        raise ClusterUnavailable(
            f"no leader reachable within {self.request_deadline:g}s ({last_problem})")

    def _redirect_target(self, error_message: str):
        """The node a NOT_LEADER:<id>:<host:port> reply points to, if it is a known node."""
        _, leader_id, address = (error_message.split(":", 2) + ["", ""])[:3]
        if address in self._by_address:
            return self._by_address[address]
        if leader_id.isdigit() and int(leader_id) < self.node_count:
            return self.nodes[int(leader_id)]
        return None     # "NOT_LEADER:None:" - the node knows no leader yet

    def _next_node(self, node: _Node, unreachable: set):
        """The next node after `node` that hasn't failed during this request."""
        for step in range(1, self.node_count + 1):
            candidate = self.nodes[(node.node_id + step) % self.node_count]
            if candidate.node_id not in unreachable:
                return candidate
        unreachable.clear()     # every node has failed once; give them all another go
        return self.nodes[(node.node_id + 1) % self.node_count]

    def _leader_hint(self) -> _Node:
        with self._lock:
            return self._leader

    def _set_leader_hint(self, node: _Node):
        with self._lock:
            self._leader = node
