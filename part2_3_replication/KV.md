# Raft Key-Value Store (mini-etcd)

A linearizable, replicated key-value store I built on the Raft consensus engine
from this project — the same core that replicates my file system, pointed at a
key-value state machine instead. It is a small analogue of **etcd / ZooKeeper /
Consul**: writes are agreed by a majority through the Raft log, and the cluster
keeps serving through a leader failure.

## 1. What it reuses

I separated consensus from its state machine precisely so that the hard
part — leader election, log replication, commit, persistence, snapshot install —
is reused **unchanged**:

| Reused as-is | Adapted | New |
|---|---|---|
| `raft_node.py` (consensus engine) | `kv_client_stub.py` (from `raft_client_stub.py`: redirect/retry/idempotency) | `kv.proto` (client API) |
| `raft.proto` + `RaftGRPCServicer` (inter-node RPCs, imported from `raft_server.py`) | | `kv_state_machine.py` (the KV state machine) |
| `_build_peer_stubs`, cluster bootstrap, `nodes_config.json` | | `raft_kv_server.py` (`RaftKVServicer` + `serve()`) |

## 2. API (`kv.proto`)

| RPC | Path | Semantics |
|---|---|---|
| `Put(key, value)` | Raft log | Set `key=value`; returns the new version |
| `Delete(key)` | Raft log | Remove `key`; reports whether it existed |
| `Cas(key, expected, new_value)` | Raft log | Compare-and-swap: set only if the current value equals `expected` |
| `Cas(key, new_value, expect_absent=true)` | Raft log | Put-if-absent (create only when the key is missing) |
| `Get(key, linearizable=false)` | Leader | Read the current value |

**Why CAS must go through the log:** the condition (*does the current value equal
`expected`?*) and the write are evaluated together, atomically, in the state
machine at apply time. Two clients cannot both win the same compare-and-swap —
the log serializes them. This is the primitive you build locks, leader-election,
and optimistic concurrency on top of (exactly what etcd's CAS is for).

## 3. Consistency model

- **Writes** (`Put` / `Delete` / `Cas`) are linearizable: they commit only after a
  majority of replicas has the entry, then apply in log order on every node.
- **Reads** (`Get`) are served by the leader:
  - `linearizable=false` (default) — served immediately from applied state. Fast,
    and consistent except in the brief window where a partitioned "zombie" leader
    hasn't yet learned it was deposed (a stale read).
  - `linearizable=true` — the leader first commits a no-op **read barrier**. That
    entry cannot commit without a current majority, which proves this node is
    still the leader and its applied state reflects every acknowledged write (a
    simple [ReadIndex](https://raft.github.io/)).
- **Idempotency:** every mutating call carries a `(client_id, seq_num)`. A retry
  after a redirect or failover returns the cached response instead of applying
  twice.

## 4. Run it

```bash
cd part2_3_replication
./setup_raft.sh          # install deps + generate fs/raft/kv stubs
./start_kv_cluster.sh    # 3 nodes on :50051-:50053; wait ~4s for a leader
python test_kv_cluster.py
kill $(cat .kv_cluster_pids)   # stop

# State-machine unit tests (no cluster needed):
python3 test_kv_state_machine.py
```

Programmatic use:

```python
from kv_client_stub import RaftKVClient
kv = RaftKVClient([("localhost", 50051), ("localhost", 50052), ("localhost", 50053)])
kv.put("city", "Kolkata")
kv.get("city")                        # b"Kolkata"
kv.cas("city", "Kolkata", "Delhi")    # True
kv.get("city", linearizable=True)     # b"Delhi"
kv.put_if_absent("owner", "node-a")   # True the first time, False after
```

## 5. Tests

- `test_kv_state_machine.py` — hermetic unit tests of the state machine
  (versioning, delete, CAS match/mismatch, put-if-absent, snapshot/restore). No
  gRPC or cluster required.
- `test_kv_cluster.py` — functional tests against a live 3-node cluster: all of
  the above over the network, linearizable reads, idempotent retries, and an
  automated **leader-failover** test (kills the leader, then confirms writes
  still succeed and prior data survives).

## 6. Known limitations (future work)

- **Idempotency across failover:** the dedup cache is per-node and in-memory, so a
  write retried against a *new* leader after failover could apply twice. Making
  dedup part of the replicated state machine fixes this. (Shared with the
  file-system build.)
- **No log compaction trigger:** `InstallSnapshot` is implemented, but the leader
  does not yet snapshot-and-truncate on a size threshold, so the log grows
  unbounded over a long run.
- **Single-key operations only:** no multi-key transactions, watches, or leases
  yet — the natural next features for a real etcd-like store.
