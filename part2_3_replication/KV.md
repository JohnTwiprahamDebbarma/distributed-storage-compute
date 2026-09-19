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
| `Cas(key, new_value, expected_version=v)` | Raft log | Compare-and-swap by version: set only if the key is still at version `v` |
| `Cas(key, new_value, expect_absent=true)` | Raft log | Put-if-absent (create only when the key is missing) |
| `Get(key, linearizable=false)` | Leader | Read the current value |

**Why CAS must go through the log:** the condition (*does the current value equal
`expected`?*) and the write are evaluated together, atomically, in the state
machine at apply time. Two clients cannot both win the same compare-and-swap —
the log serializes them. This is the primitive you build locks, leader-election,
and optimistic concurrency on top of (exactly what etcd's CAS is for).

**Why compare versions, not values:** a value can change and change back
(A → B → A), and a value-based CAS cannot tell. A key's version only ever grows —
even across delete and re-create, because the state machine remembers a deleted
key's last version — so a version-based CAS is immune to this ABA problem.

**Admin RPCs** (on every node's `RaftService`, see `raft.proto`):

| RPC | Semantics |
|---|---|
| `GetStatus()` | Role, term, leader, commit / applied / log indexes, isolation flag and, on the leader, how far each follower has replicated |
| `SetIsolated(isolated, heal_after_seconds)` | Fault injection: the node drops all Raft and client traffic, as if unplugged, but keeps answering these two admin RPCs; optionally rejoins on its own |

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
- **Exactly-once writes:** every mutating call carries a `(client_id, seq_num)`.
  The state machine keeps a **session table** (client → latest `seq_num` and its
  result) that only `apply()` changes, so it is replicated with the log and every
  node holds the same copy. A retry is answered from the table instead of being
  applied twice — even one that reaches a *new* leader after failover, which a
  per-node cache would re-apply (`test_kv_cluster.py` checks exactly this). The
  table is bounded at 10,000 clients; the least recently active one is evicted in
  log order, so every replica evicts the same one.

## 4. Run it

```bash
cd part2_3_replication
./setup_raft.sh          # install deps + generate fs/raft/kv stubs
./start_kv_cluster.sh    # 3 nodes on :50051-:50053; wait ~4s for a leader
python test_kv_cluster.py
kill $(cat .kv_cluster_pids)   # stop
rm -rf raft_state_node*        # the next run needs fresh Raft state

# Unit tests (no cluster needed):
python3 test_kv_state_machine.py
python3 test_raft_status_isolation.py
```

Programmatic use:

```python
from kv_client_stub import RaftKVClient
kv = RaftKVClient([("localhost", 50051), ("localhost", 50052), ("localhost", 50053)])
kv.put("city", "Kolkata")
kv.get("city")                        # b"Kolkata"
kv.cas("city", "Kolkata", "Delhi")    # True
kv.cas_version("city", 2, "Mumbai")   # (True, 3): only if still at version 2
kv.get("city", linearizable=True)     # b"Mumbai"
kv.put_if_absent("owner", "node-a")   # True the first time, False after
```

## 5. Tests

- `test_kv_state_machine.py` — hermetic unit tests of the state machine
  (versioning, delete, CAS by value and by version including the ABA case,
  put-if-absent, replicated de-duplication, snapshot/restore). No gRPC or cluster
  required.
- `test_raft_status_isolation.py` — hermetic tests of `GetStatus` and of
  isolation: an isolated leader sends no heartbeats, an isolated candidate
  requests no votes, and a node heals itself on schedule.
- `test_kv_cluster.py` — functional tests against a live 3-node cluster: all of
  the above over the network, linearizable reads, node status, **exactly-once
  retries across failover** (isolates the leader, then re-sends an
  already-committed write to the new leader), and an automated
  **leader-failover** test (kills the leader, then confirms writes still succeed
  and prior data survives).

## 6. Known limitations (future work)

- **No PreVote:** an isolated follower keeps timing out and starting elections
  nobody hears, so its term climbs. When it rejoins, that higher term makes the
  current leader step down and forces one extra election. Raft's PreVote
  extension (used by etcd) avoids this.
- **No log compaction trigger:** `InstallSnapshot` is implemented, but the leader
  does not yet snapshot-and-truncate on a size threshold, so the log grows
  unbounded over a long run.
- **Single-key operations only:** no multi-key transactions, watches, or leases
  yet — the natural next features for a real etcd-like store.
