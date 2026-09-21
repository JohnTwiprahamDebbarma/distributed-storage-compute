"""
Functional tests for the Raft KV store (mini-etcd) against a running cluster.

Assumes a 3-node cluster on localhost:50051-50053, e.g.:
    ./setup_raft.sh          # once, to install deps + generate stubs
    ./start_kv_cluster.sh    # start the 3 nodes
    python test_kv_cluster.py

Covers: put/get, versioning, delete, compare-and-swap (by value and by version),
put-if-absent, linearizable reads, idempotent retries, node status, exactly-once
retries across a failover (driven by isolating the leader), and automated
leader failover.

The cluster keeps its Raft state on disk, so start each run from fresh state:
    rm -rf raft_state_node*
"""

import os
import sys
import time

import grpc

import raft_pb2
import raft_pb2_grpc
import kv_pb2
import kv_pb2_grpc
from kv_client_stub import RaftKVClient

NODES = [("localhost", 50051), ("localhost", 50052), ("localhost", 50053)]


def check(label, got, expected):
    if got == expected:
        print(f"  [PASS] {label}")
    else:
        print(f"  [FAIL] {label}\n         expected: {expected!r}\n         got:      {got!r}")
        sys.exit(1)


def find_leader_address():
    for host, port in NODES:
        try:
            with grpc.insecure_channel(f"{host}:{port}") as ch:
                r = raft_pb2_grpc.RaftServiceStub(ch).GetLeader(
                    raft_pb2.GetLeaderRequest(), timeout=2.0)
                if r.has_leader and r.leader_address:
                    return r.leader_address
        except grpc.RpcError:
            continue
    return None


def node_statuses():
    """GetStatus from every node, keyed by "host:port" (None if it does not answer)."""
    out = {}
    for host, port in NODES:
        addr = f"{host}:{port}"
        try:
            with grpc.insecure_channel(addr) as ch:
                out[addr] = raft_pb2_grpc.RaftServiceStub(ch).GetStatus(
                    raft_pb2.GetStatusRequest(), timeout=2.0)
        except grpc.RpcError:
            out[addr] = None
    return out


def set_isolated(address, isolated, heal_after=0.0):
    with grpc.insecure_channel(address) as ch:
        raft_pb2_grpc.RaftServiceStub(ch).SetIsolated(
            raft_pb2.SetIsolatedRequest(isolated=isolated, heal_after_seconds=heal_after),
            timeout=2.0)


def wait_for_leader(excluding=None, timeout=15.0):
    """Address of a connected node that reports itself leader, other than `excluding`."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for addr, s in node_statuses().items():
            if addr != excluding and s is not None and s.state == "leader" and not s.isolated:
                return addr
        time.sleep(0.3)
    return None


def main():
    c = RaftKVClient(NODES)

    print("== Basic put / get ==")
    c.put("city", "Kolkata")
    check("get returns the written value", c.get("city"), b"Kolkata")
    check("absent key returns None", c.get("does-not-exist"), None)

    print("== Versioning ==")
    c.put("city", "Delhi")
    check("version increments on overwrite", c.get_versioned("city")[1], 2)

    print("== Delete ==")
    check("delete of existing key reports existed", c.delete("city"), True)
    check("get after delete is None", c.get("city"), None)
    check("delete of missing key reports not-existed", c.delete("city"), False)

    print("== Compare-and-swap ==")
    c.put("k", "v1")
    check("CAS with matching expected swaps", c.cas("k", "v1", "v2"), True)
    check("value after CAS", c.get("k"), b"v2")
    check("CAS with stale expected is rejected", c.cas("k", "v1", "v3"), False)
    check("value unchanged after failed CAS", c.get("k"), b"v2")
    check("put_if_absent on new key", c.put_if_absent("fresh", "1"), True)
    check("put_if_absent on existing key", c.put_if_absent("fresh", "2"), False)

    print("== Linearizable read ==")
    c.put("lin", "yes")
    check("linearizable get", c.get("lin", linearizable=True), b"yes")

    print("== Idempotency (a retried write with the same seq_num does not re-apply) ==")
    c.put("idem", "first")
    replay = kv_pb2.PutRequest(key="idem", value=b"SECOND",
                               client_id=c.client_id, seq_num=c.seq_num)  # reuse last seq
    c._execute("Put", replay)   # server returns the cached response, no new write
    check("duplicate seq_num did not overwrite", c.get("idem"), b"first")

    print("== Node status ==")
    statuses = node_statuses()
    leaders = [a for a, s in statuses.items() if s and s.state == "leader"]
    check("exactly one node reports itself leader", len(leaders), 1)
    check("all nodes agree on the term", len({s.term for s in statuses.values() if s}), 1)

    print("== Version-based compare-and-swap ==")
    v = c.put("doc", "draft")
    check("CAS at the current version swaps", c.cas_version("doc", v, "final"), (True, v + 1))
    check("CAS at a stale version is rejected", c.cas_version("doc", v, "other"), (False, v + 1))
    check("value after version CAS", c.get("doc"), b"final")

    print("== Exactly-once across failover (replicated de-duplication) ==")
    v = c.put("once", "x")
    replay = kv_pb2.PutRequest(key="once", value=b"x",
                               client_id=c.client_id, seq_num=c.seq_num)  # the same request again
    old_leader = wait_for_leader()
    set_isolated(old_leader, True)
    try:
        with grpc.insecure_channel(old_leader) as ch:
            try:
                kv_pb2_grpc.KVServiceStub(ch).Get(kv_pb2.GetRequest(key="once"), timeout=2.0)
                code = grpc.StatusCode.OK
            except grpc.RpcError as e:
                code = e.code()
        check("isolated node rejects client requests", code, grpc.StatusCode.UNAVAILABLE)
        new_leader = wait_for_leader(excluding=old_leader)
        check("a new leader is elected without the isolated node", new_leader is not None, True)
        resp = c._execute("Put", replay)   # the retry now reaches the NEW leader
        check("retry on the new leader returns the original version", resp.version, v)
        check("retry did not apply the write twice", c.get_versioned("once")[1], v)
    finally:
        set_isolated(old_leader, False)
    time.sleep(3)
    check("healed node rejoins as a follower", node_statuses()[old_leader].state, "follower")

    print("== Leader failover ==")
    leader = find_leader_address()
    print(f"  current leader: {leader}")
    if leader:
        port = leader.split(":")[-1]
        # I pass -sTCP:LISTEN so only the listening server is killed, not this
        # test's own client connection to that port (which lsof would also return).
        os.system(f"lsof -ti:{port} -sTCP:LISTEN | xargs kill 2>/dev/null")
        print(f"  killed leader on :{port}; waiting for re-election...")
        time.sleep(6)
        c.put("after", "failover")
        check("write succeeds after failover", c.get("after"), b"failover")
        check("data written before failover survives", c.get("k"), b"v2")
    else:
        print("  [SKIP] no leader discovered")

    c.close()
    print("\n== All KV cluster tests passed ==")


if __name__ == "__main__":
    main()
