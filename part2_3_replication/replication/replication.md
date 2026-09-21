# Replicated File Server (Primary-Backup)

---

## 1. Overview

Replicated File Server adds **high availability** to the DSFS file system built in Tasks 1A, 1B, and 2. The filesystem now runs as a **cluster of three replica servers**. If any single server fails, clients can continue reading and writing without interruption. When the failed server restarts, it automatically re-joins the cluster and catches up on any writes it missed.

The implementation lives entirely in `fs/replication/` and reuses the existing `fs/fs.proto` gRPC service (same client API) while adding a new `replication.proto` for peer-to-peer communication between replicas.

---

## 2. Algorithm Choice and Rationale

### Why Primary-Backup?

The project workload has two key properties that make primary-backup the optimal choice:

| Property | Implication |
|---|---|
| Input files are **static** | No concurrent write conflicts to resolve |
| Output (`results.txt`) is **write-once** | A single write event, no concurrent updates |
| Workload is **read-heavy** | Backups can serve reads, reducing load on primary |
| Whole-file caching on clients | Replication granularity is whole-file - simple to propagate |

**Raft / Paxos** solve the *arbitrary concurrent write* problem via log replication and quorum commits. Since this workload has no concurrent writes, paying that complexity cost is unnecessary.

**Primary-backup** is simpler, correct for this workload, and matches the spec's hint to "stick to a simple approach," which Prof. Yuvi also recommended.

### When Primary-Backup Fails

Primary-backup has known failure modes that the implementation must address:

| Scenario | Risk | Mitigation in this implementation |
|---|---|---|
| **Network partition** | Split-brain: two primaries | Live-RPC election (not timestamp-based) |
| **Pre-sync ACK + crash** | Data loss | Primary replicates to all backups *before* ACKing the client |
| **False failure detection** | Premature election | `last_heartbeat` initialised to `0` (not current time), so election only fires after a real heartbeat has been received and then missed |
| **Recovering server assumes wrong role** | Split-brain on restart | Startup always queries peers first; only becomes primary if peers confirm it or if no peers are reachable |
| **gRPC channel backoff** | Silent replication failures after restart | Peer channels use short reconnect backoff (200 ms initial, 1 s max) |

---

## 3. System Architecture

```mermaid
graph TB
    subgraph Clients
        C["ReplicatedDSFSClientStub<br/>- Discovers primary via GetClusterInfo<br/>- Routes all writes to primary<br/>- On FAILED_PRECONDITION -> redirects<br/>- On UNAVAILABLE -> rediscovers primary, retries with backoff"]
    end

    subgraph Cluster
        S0["Server 0 (PRIMARY)<br/>localhost:50050<br/>data_dir: ./data_50050"]
        S1["Server 1 (BACKUP)<br/>localhost:50051<br/>data_dir: ./data_50051"]
        S2["Server 2 (BACKUP)<br/>localhost:50052<br/>data_dir: ./data_50052"]
    end

    C -->|"FileSystem gRPC"| S0
    C -->|"FileSystem gRPC"| S1
    S0 <-->|"heartbeat + replica RPCs<br/>(ReplicationService gRPC)"| S1
    S0 <-->|"heartbeat + replica RPCs<br/>(ReplicationService gRPC)"| S2
    S1 <-->|"heartbeat + replica RPCs<br/>(ReplicationService gRPC)"| S2
```

**Key architectural decisions:**

- Both the `FileSystem` service (client-facing) and the `ReplicationService` (peer-to-peer) run on the **same gRPC server and same port**. No separate replication port is needed.
- The **preferred primary** is the server with the lexicographically smallest `host:port` address. This is only a default; after a failover, the elected primary keeps its role even if the original primary restarts.
- All servers are equal in capability; the primary/backup distinction is runtime state, not a fixed configuration.

---

## 4. Directory Structure

```
fs/replication/
|-- replication.proto          # Peer-to-peer RPC definitions
|-- replication_pb2.py         # Generated protobuf classes (auto-generated)
|-- replication_pb2_grpc.py    # Generated gRPC stubs  (auto-generated)
|-- server.py                  # Unified primary/backup replica server
|-- client_stub.py             # Client stub with replica awareness
|-- client.py                  # Legacy interactive test client
|-- generate_proto.sh          # Regenerates pb2 files from replication.proto
|-- start_cluster.sh           # Convenience script to start 3 servers
|-- venv/                      # Python virtual environment
`-- tests/
    |-- __init__.py
    |-- test_suite.py          # 32 test cases, cluster-agnostic
    |-- run_local.py           # Automated runner: starts cluster, tests, teardown
    `-- run_distributed.py     # Runner for multi-machine clusters
```

- `server.py` imports `fs_pb2` / `fs_pb2_grpc` from `../` (the parent `fs/` directory).
- `client_stub.py` is a drop-in replacement for `../client_stub.py`. It exposes the same `open / read / write / close / create / test_version_number` API.
- Here `fs` means File System (Part 1). In this repository the code for Parts 2-3 lives in the `part2_3_replication/` directory.

---

## 5. Protocol Buffers (replication.proto)

The `ReplicationService` handles all peer-to-peer communication. It runs on the same port as the client-facing `FileSystem` service.

```protobuf
service ReplicationService {
  rpc ReplicateWrite   (ReplicateWriteRequest)   returns (ReplicateWriteResponse)   {}
  rpc ReplicateCreate  (ReplicateCreateRequest)  returns (ReplicateCreateResponse)  {}
  rpc Heartbeat        (HeartbeatRequest)        returns (HeartbeatResponse)        {}
  rpc GetClusterInfo   (GetClusterInfoRequest)   returns (GetClusterInfoResponse)   {}
  rpc SyncState        (SyncStateRequest)        returns (SyncStateResponse)        {}
  rpc AnnounceLeader   (AnnounceLeaderRequest)   returns (AnnounceLeaderResponse)   {}
}
```

| RPC | Direction | Purpose |
|---|---|---|
| `ReplicateWrite` | Primary -> Backups | Propagate a file write (whole file + new version number) |
| `ReplicateCreate` | Primary -> Backups | Propagate a new empty file |
| `Heartbeat` | Every server -> every peer | Liveness detection (2 s interval) |
| `GetClusterInfo` | Any -> any | Returns current primary address and own role |
| `SyncState` | Recovering server -> primary | Fetch full filesystem snapshot |
| `AnnounceLeader` | Newly-elected primary -> peers | Notify peers of new primary after election |

### Key Messages

```protobuf
// Write replication payload
message ReplicateWriteRequest {
  string filename = 1;
  bytes  file_data = 2;   // entire file contents
  int32  new_version = 3;
}

// Cluster state query (used by clients and peers)
message GetClusterInfoResponse {
  string primary_address = 1;  // "host:port" of current primary
  string my_role = 2;  // "primary" or "backup"
  bool success = 3;
}

// Full state snapshot entry
message FileEntry {
  string filename = 1;
  bytes file_data = 2;
  int32 version = 3;
}
```

To regenerate the Python stubs after editing the proto:

```bash
cd fs/replication
bash generate_proto.sh
```

---

## 6. Server Design (server.py)

The server is a single class `ReplicatedDSFSServer` that implements **both** gRPC service interfaces:

```python
class ReplicatedDSFSServer(
    fs_pb2_grpc.FileSystemServicer,       # client-facing (Tasks 1-2)
    replication_pb2_grpc.ReplicationServiceServicer,  # peer-to-peer (Task 3)
):
```

**Constants:**

| Constant | Value | Meaning |
|---|---|---|
| `HEARTBEAT_INTERVAL` | 2 s | How often each server sends heartbeats to peers |
| `HEARTBEAT_TIMEOUT` | 6 s | How long without a heartbeat before a peer is declared dead |

---

### 6.1 Startup and Role Determination

On startup, a server **always queries its peers first** before assuming any role. This prevents the most common split-brain scenario: a server that was previously primary restarting and reclaiming that role even though another server was elected primary during its absence.

```mermaid
flowchart TD
    A["_determine_startup_role()"] --> B["_query_peers_for_primary()<br/>live RPCs to all known peers"]
    B --> C{"Peers respond with<br/>a primary address?"}
    C -->|Yes| D{"claimed == my_addr?"}
    C -->|"No peers respond<br/>(fresh cluster or all down)"| G{"my_addr ==<br/>lowest address?"}

    D -->|Yes| E["Peers confirm me as PRIMARY<br/>Load local files, ready = True"]
    D -->|No| F["Existing primary found.<br/>Starting as BACKUP.<br/>_sync_or_elect(claimed_primary)"]

    G -->|Yes| H["Become PRIMARY<br/>Load local files, ready = True"]
    G -->|No| I["Attempt sync from preferred primary<br/>_sync_or_elect(preferred_primary)"]

    F --> J["_sync_or_elect(target)"]
    I --> J

    J --> K["_try_sync_from(target, 15s)"]
    K -->|Success| L["role = backup, ready = True"]
    K -->|"Failure after 15s"| M["_elect_new_primary()<br/>(live election)"]
```

**Why this matters for recovery:** When Server 0 (original primary) crashes, Server 1 is elected. When Server 0 restarts, it asks peers who the primary is. Peers respond "primary = Server 1". Server 0 syncs from Server 1 and rejoins as a backup. No split-brain.

---

### 6.2 Heartbeat and Failure Detection

Two daemon threads run for the lifetime of the server:

**`_heartbeat_sender` thread:**
- Sends `Heartbeat` RPC to every peer every `HEARTBEAT_INTERVAL` seconds.
- On success, records `last_heartbeat[peer] = time.time()`.
- Failures are silently swallowed (peer might be temporarily down).

**`_failure_detector` thread:**
- Runs every `HEARTBEAT_INTERVAL` seconds.
- Only fires on **backups** (primaries don't need to monitor each other).
- Condition: `last_heartbeat[primary] > 0` **AND** `now - last_heartbeat[primary] > HEARTBEAT_TIMEOUT`.
  - The `> 0` guard is critical: it prevents a newly-started server from declaring an election before it has ever received a heartbeat from the primary.

Timeline example - primary fails at t=10:

```mermaid
sequenceDiagram
    participant S1 as Server 1 (Backup)
    participant S0 as Server 0 (Primary)

    Note over S1: t=0: Starts, last_heartbeat[S0] = 0.0
    S0->>S1: t=2: Heartbeat
    Note over S1: last_heartbeat[S0] = 2.0
    S0->>S1: t=4: Heartbeat
    Note over S1: last_heartbeat[S0] = 4.0
    S0->>S1: t=6: Heartbeat
    Note over S1: last_heartbeat[S0] = 6.0
    S0->>S1: t=8: Heartbeat
    Note over S1: last_heartbeat[S0] = 8.0
    Note over S0: t=10: CRASHES
    S0--xS1: t=12: No heartbeat
    Note over S1: now - last = 4s < 6s -> no action
    S0--xS1: t=14: No heartbeat
    Note over S1: now - last = 6s >= threshold -> _elect_new_primary()
```

---

### 6.3 Election Algorithm

When a backup triggers an election, it performs **live queries** to all peers:

```python
def _elect_new_primary(self):
    alive = [self.my_addr]                      # I am always alive
    for addr, (_, _, rep_stub) in self.peer_stubs.items():
        try:
            resp = rep_stub.GetClusterInfo(..., timeout=2.0)
            if resp.success:
                alive.append(addr)              # peer responded -> alive
        except Exception:
            pass                                # peer unreachable -> dead

    new_primary = sorted(alive)[0]              # lowest address wins
    ...
    if self.role == "primary":
        self._announce_leader(new_primary)      # notify all peers
```

**Why live RPCs instead of heartbeat timestamps?**

At the moment an election is triggered, the `last_heartbeat` timestamps may be stale or uninitialised (especially at startup). Using live `GetClusterInfo` RPCs gives an accurate picture of which servers are currently reachable. A server that cannot respond to a 2-second RPC is considered dead.

**Announcement:** The newly-elected primary calls `AnnounceLeader` on all peers so they update their `primary_addr` immediately, without waiting for the next heartbeat.

---

### 6.4 Write Replication

Every mutating operation on the primary is replicated to all backups **synchronously before the client is ACKed**. This guarantees that if the primary ACKs a write and then immediately crashes, at least one backup has the data.

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Primary
    participant B1 as Backup 1
    participant B2 as Backup 2

    C->>P: Close(write)
    par Replicate to backups
        P->>B1: ReplicateWrite
        P->>B2: ReplicateWrite
    end
    B1-->>P: success
    B2-->>P: success
    P-->>C: ACK
```

Replication failures to individual backups are logged but do not block the primary from ACKing the client. This means:
- A backup that is temporarily down will miss writes.
- When it restarts, it uses `SyncState` to catch up (see §6.5 below).

**Create replication** follows the same pattern: after creating the empty file locally, the primary calls `ReplicateCreate` on each backup.

---

### 6.5 State Synchronisation (Recovery)

When a backup restarts, it calls `SyncState` on the current primary:

```python
# Primary responds:
def SyncState(self, request, context):
    entries = []
    for filename, version in self.file_versions.items():
        with open(os.path.join(self.data_dir, filename), "rb") as f:
            data = f.read()
        entries.append(FileEntry(filename=filename, file_data=data, version=version))
    return SyncStateResponse(files=entries, success=True)
```

The recovering backup:
1. Writes every received file to its `data_dir`.
2. Sets `file_versions[filename] = entry.version` for each file.
3. Sets `ready = True` and starts accepting client requests.

The backup does **not** begin heartbeating or answering client RPCs until `ready = True`, preventing stale reads during the sync window.

---

### 6.6 Client-Facing RPCs

The server serves the same `FileSystem` gRPC service as the single-server implementation in `fs/server.py`. The key additions:

| RPC | Restriction | Behaviour on backup |
|---|---|---|
| `TestVersionNumber` | Any replica | Served locally - no redirect |
| `Open` (read mode) | Any replica | Served locally - no redirect |
| `Open` (write mode) | Primary only | Returns `FAILED_PRECONDITION("NOT_PRIMARY:<addr>")` |
| `Create` | Primary only | Returns `FAILED_PRECONDITION("NOT_PRIMARY:<addr>")` |
| `Close` (write) | Primary only | Returns `FAILED_PRECONDITION("NOT_PRIMARY:<addr>")` |

The `FAILED_PRECONDITION` detail string encodes the primary's address (`NOT_PRIMARY:host:port`), which the client stub parses to transparently redirect.

**Readiness gate:** All RPCs check `self.ready`. If the server is still syncing, it returns `UNAVAILABLE("Server syncing, try again shortly")`, which the client treats as a transient error and retries with backoff.

**Idempotency** (carried over from Task 2): `Create` and `Close` check `dedup_cache[client_id] = (last_seq_num, cached_response)`. Duplicate requests with the same `seq_num` return the cached response without re-executing the operation.

---

## 7. Client Stub (client_stub.py)

`ReplicatedDSFSClientStub` is a drop-in replacement for the single-server `DSFSClientStub`. It has the same public API (`open`, `read`, `write`, `close`, `create`, `test_version_number`).

### Initialisation

```python
client = ReplicatedDSFSClientStub(
    server_addresses=["localhost:50050", "localhost:50051", "localhost:50052"],
    cache_dir="/tmp/dsfs",
)
```

On construction:
1. Creates gRPC channels to every server in the list.
2. Calls `GetClusterInfo` on each server in turn until one responds.
3. Records the current `primary_addr`.

### RPC Execution (`_execute_rpc`)

Every RPC goes through a central retry loop:

```mermaid
flowchart TD
    A["attempt:<br/>call RPC on current primary"] --> B{Response?}
    B -->|success| C[return response]
    B -->|"FAILED_PRECONDITION<br/>(NOT_PRIMARY:addr)"| D["Update primary_addr to addr<br/>Retry immediately"]
    B -->|"UNAVAILABLE /<br/>DEADLINE_EXCEEDED /<br/>RESOURCE_EXHAUSTED"| E["Rediscover primary via<br/>_discover_primary()<br/>Sleep 2^attempt seconds, retry"]
    B -->|other gRPC error| F[raise exception]
    D --> A
    E --> A
```

### Primary Redirect (Transparent)

If a client sends a write RPC to a backup (e.g. because the primary changed after the client last checked), the backup replies with `FAILED_PRECONDITION("NOT_PRIMARY:localhost:50051")`. The client stub:

1. Parses the new primary address from the error detail.
2. Updates `self._primary_addr`.
3. Retries the same RPC on the new primary - all transparent to the caller.

### Caching

Caching behaviour is identical to `fs/client_stub.py`:
- First `open` fetches the file from the server and writes it to `cache_dir/filename_vN.bin`.
- Subsequent opens send `TestVersionNumber` first. If the local version matches the server version, the cached file is used (`CACHE HIT`). Otherwise the file is re-fetched (`CACHE MISS`).
- On `close` in write mode, the entire local file is sent to the server via `Close` RPC. The new version number is used to rename the cache file.

---

## 8. Known Failure Modes and Handling

| Failure scenario | What happens | Outcome |
|---|---|---|
| **Primary crashes** | Backups miss heartbeats for 6 s -> election -> lowest-address backup becomes new primary -> announces to peers | System available within ~8 s. Writes during the election window are blocked (client retries). |
| **Backup crashes** | Primary continues normally. Replication to dead backup logs an error and moves on. | No client impact. |
| **Primary crashes before replicating** | Client's `Close` RPC gets no ACK (or UNAVAILABLE). Client retries -> hits new primary -> re-executes the write. Idempotency (seq_num dedup) prevents double-writes if the first write partially succeeded. | Write is re-executed exactly once. |
| **Network partition (minority)** | Server in minority partition cannot reach peers -> election -> minority elects itself. Heals when partition resolves. | Potential split-brain. Mitigation: election requires live RPC responses from at least one peer; a truly isolated server (no responses) stays in its current role rather than electing. |
| **Recovering primary rejoins** | On restart, queries peers -> finds new primary -> syncs full state -> becomes backup | Old primary does not reclaim its role. No split-brain. |
| **All servers restart simultaneously** | Lowest-address server gets no peer responses -> becomes primary (preferred-address fallback). Others sync from it. | Clean restart. Staggered startup (primary first, then backups) is the recommended practice. |
| **Backup not yet ready (still syncing)** | Backup returns `UNAVAILABLE("Server syncing")` to clients | Client retries with backoff. |

---

## 9. Running the Cluster

### Prerequisites

```bash
cd fs/replication
python3 -m venv venv
./venv/bin/pip install grpcio grpcio-tools
bash generate_proto.sh   # generates replication_pb2.py and replication_pb2_grpc.py
```

### Local (single machine, 3 terminals)

```bash
# Terminal 1 - primary
./venv/bin/python server.py \
    --addr  localhost:50050 \
    --peers localhost:50051,localhost:50052 \
    --data-dir ./data_50050

# Terminal 2 - backup 1 (start after primary is listening)
./venv/bin/python server.py \
    --addr  localhost:50051 \
    --peers localhost:50050,localhost:50052 \
    --data-dir ./data_50051

# Terminal 3 - backup 2
./venv/bin/python server.py \
    --addr  localhost:50052 \
    --peers localhost:50050,localhost:50051 \
    --data-dir ./data_50052
```

Or use the convenience script:

```bash
bash start_cluster.sh
```

### Multi-machine

Assign one port (e.g. 50050) on each machine. Replace `localhost` with the actual hostname or IP.

```bash
# Machine A (initial primary - must have lowest address)
./venv/bin/python server.py \
    --addr  machineA:50050 \
    --peers machineB:50050,machineC:50050 \
    --data-dir ./server_data

# Machine B
./venv/bin/python server.py \
    --addr  machineB:50050 \
    --peers machineA:50050,machineC:50050 \
    --data-dir ./server_data

# Machine C
./venv/bin/python server.py \
    --addr  machineC:50050 \
    --peers machineA:50050,machineB:50050 \
    --data-dir ./server_data
```

> **Important:** Start Machine A (lowest address) first. Backups will wait up to 15 seconds for the primary to become reachable before falling back to election.

### Using the client

```python
from fs.replication.client_stub import ReplicatedDSFSClientStub

client = ReplicatedDSFSClientStub(
    server_addresses=["localhost:50050", "localhost:50051", "localhost:50052"],
    cache_dir="/tmp/afs_cache",
)

client.create("input_dataset_001.txt")
fh = client.open("input_dataset_001.txt", mode="w")
client.write(fh, 0, b"1 2 3 4 5 ...\n")
client.close(fh)

fh   = client.open("input_dataset_001.txt", mode="r")
data = client.read(fh, 0, 1024)
client.close(fh)
```

Or, for a quick test, run client.py using
```bash
source venv/bin/activate && python3 client.py
```

---

## 10. Test Suite

### 10.1 Test Groups

The test suite in `tests/test_suite.py` contains **32 automated test cases** across 9 groups.

#### Group A - Basic DSFS Functionality (T01-T08)

| Test | What it verifies |
|---|---|
| T01 | `create()` returns file handle 0 |
| T02 | Duplicate `create()` is rejected |
| T03 | `open()` on a non-existent file in read mode raises an error |
| T04 | Write then read back returns exact bytes |
| T05 | Version number increments on every write |
| T06 | Multiple independent files do not interfere |
| T07 | Large file (1 MB) round-trip over the network |
| T08 | Writing empty content (`b""`) is handled correctly |

#### Group B - Client-Side Caching (T09-T11)

| Test | What it verifies |
|---|---|
| T09 | Second `open()` on unchanged file hits local cache (no network fetch) |
| T10 | `open()` after a remote write detects version mismatch and re-fetches |
| T11 | Repeated opens with no intervening write always hit cache |

#### Group C - Replication Correctness (T12-T14)

| Test | What it verifies |
|---|---|
| T12 | After a write, all 3 data directories contain the new file content |
| T13 | All replicas report the same version number after a write |
| T14 | A direct gRPC read from a backup returns the same data as the primary |

#### Group D - Primary Failure / Failover (T15-T19)

| Test | What it verifies |
|---|---|
| T15 | A new primary is elected after killing the primary |
| T16 | Reads succeed from the new primary after failover |
| T17 | Writes succeed after failover |
| T18 | The new primary is the lowest-address surviving server |
| T19 | Three sequential writes to the new primary all succeed |

#### Group E - Backup Failure (T20-T21)

| Test | What it verifies |
|---|---|
| T20 | Primary still accepts writes when one backup is down |
| T21 | Reads still succeed when one backup is down |

#### Group F - Server Recovery / Re-sync (T22-T24)

| Test | What it verifies |
|---|---|
| T22 | A restarted backup receives data written while it was down |
| T23 | The recovered old primary also receives post-failover writes |
| T24 | The recovered old primary rejoins as backup (not primary) |

#### Group G - Idempotency / Duplicate Request Handling (T25-T26)

| Test | What it verifies |
|---|---|
| T25 | A duplicate `Create` RPC (same `seq_num`) returns the cached response, not an error |
| T26 | A duplicate `Close` RPC with different data (same `seq_num`) does not double-write |

#### Group H - Concurrent Clients (T27-T28)

| Test | What it verifies |
|---|---|
| T27 | Two clients writing to different files simultaneously do not interfere |
| T28 | Four concurrent readers all get consistent data |

#### Group I - Edge Cases (T29-T32)

| Test | What it verifies |
|---|---|
| T29 | Reading past EOF returns only available bytes (no crash) |
| T30 | Reading at a non-zero offset returns the correct bytes |
| T31 | Using an invalid file handle raises an error |
| T32 | `test_version_number` on a non-existent file returns version 1 (default) |

---

### 10.2 Running Tests Locally

`tests/run_local.py` is fully automated. It:
1. Kills any stale server processes on the test ports.
2. Starts a 3-node cluster (primary first, then backups).
3. Polls until all servers are ready (`GetClusterInfo` returns a definite role).
4. Waits 3 additional seconds for gRPC peer channels to stabilise.
5. Runs all 32 tests, including fault-injection (kill / restart servers).
6. Tears down all servers and removes test data directories.

```bash
cd fs/replication

# Full suite (all 32 tests, including fault injection):
./venv/bin/python tests/run_local.py

# Smoke test (22 tests, no fault injection - faster):
./venv/bin/python tests/run_local.py --no-fault

# Custom ports (if 50050-50052 are in use):
./venv/bin/python tests/run_local.py --ports 51050,51051,51052

# Leave servers running after tests (for manual inspection):
./venv/bin/python tests/run_local.py --no-cleanup
```

Expected output (all pass):

```
==============================================================
  Results: 32 tests | 32 passed | 0 failed | 0 skipped
==============================================================
```

---

### 10.3 Running Tests on Multiple Machines

Start the cluster on the three machines as described in §9. Then run `tests/run_distributed.py` from any machine (or a separate client machine).

#### Option A - SSH fault injection (fully automated)

The test runner kills and restarts servers via SSH. No manual intervention required.

```bash
./venv/bin/python tests/run_distributed.py \
    --servers   machineA:50050,machineB:50050,machineC:50050 \
    --ssh-user  ubuntu \
    --ssh-key   ~/.ssh/id_rsa \
    --server-dir ~/fs/replication \
    --remote-data-dir ./server_data
```

The runner uses `pkill -f 'server.py.*<port>'` to kill and `nohup python server.py ... &` to restart.

#### Option B - Manual fault injection

The runner pauses and prints instructions for the operator to kill/restart servers.

```bash
./venv/bin/python tests/run_distributed.py \
    --servers machineA:50050,machineB:50050,machineC:50050
```

When the fault-injection tests are reached, they print prompts like:

```
    ACTION REQUIRED:
     On machine 'machineA', kill the server on port 50050:
       pkill -f 'server.py.*50050'
     Press Enter once the server is stopped ...
```

#### Option C - Skip fault injection (read-only)

Run Groups A-C, G-I only (22 tests, no servers are killed).

```bash
./venv/bin/python tests/run_distributed.py \
    --servers machineA:50050,machineB:50050,machineC:50050 \
    --no-fault
```

#### Pre-flight connectivity check

`run_distributed.py` always runs a connectivity check before starting tests:

```
[PRE-FLIGHT] Checking connectivity ...
  YES  machineA:50050  role=primary  primary=machineA:50050
  YES  machineB:50050  role=backup   primary=machineA:50050
  YES  machineC:50050  role=backup   primary=machineA:50050
```

If any server is unreachable, the runner aborts with an error.
