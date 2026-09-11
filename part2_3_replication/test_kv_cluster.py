"""
Functional tests for the Raft KV store (mini-etcd) against a running cluster.

Assumes a 3-node cluster on localhost:50051-50053, e.g.:
    ./setup_raft.sh          # once, to install deps + generate stubs
    ./start_kv_cluster.sh    # start the 3 nodes
    python test_kv_cluster.py

Covers: put/get, versioning, delete, compare-and-swap, put-if-absent,
linearizable reads, idempotent retries, and automated leader failover.
"""

import os
import sys
import time

import grpc

import raft_pb2
import raft_pb2_grpc
import kv_pb2
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

    print("== Leader failover ==")
    leader = find_leader_address()
    print(f"  current leader: {leader}")
    if leader:
        port = leader.split(":")[-1]
        # -sTCP:LISTEN so we kill only the listening server, not our own client
        # connection to that port (which lsof would otherwise also return).
        os.system(f"lsof -ti:{port} -sTCP:LISTEN | xargs kill 2>/dev/null")
        print(f"  killed leader on :{port}; waiting for re-election…")
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
