import os
import sys
import time
import threading
import argparse
import grpc
from concurrent import futures

# Allow importing fs_pb2 from parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import fs_pb2
import fs_pb2_grpc
import replication_pb2
import replication_pb2_grpc

HEARTBEAT_INTERVAL = 2.0   # seconds between heartbeat sends
HEARTBEAT_TIMEOUT  = 6.0   # seconds before a peer is considered dead
GRPC_MAX_MSG_SIZE = 64 * 1024 * 1024   # 64 MB (gRPC default is 4 MB)

class ReplicatedDSFSServer(
    fs_pb2_grpc.FileSystemServicer,
    replication_pb2_grpc.ReplicationServiceServicer,
):
    # Initialisation

    def __init__(self, my_addr: str, peer_addrs: list, data_dir: str):
        """
        my_addr    - "host:port" this server listens on
        peer_addrs - list of "host:port" for the OTHER replicas
        data_dir   - directory where files are persisted
        """
        self.my_addr    = my_addr
        self.peer_addrs = peer_addrs
        self.all_addrs  = sorted([my_addr] + peer_addrs)
        self.data_dir   = data_dir
        os.makedirs(data_dir, exist_ok=True)

        self.state_lock = threading.Lock()

        # DSFS file-system state
        self.file_versions: dict = {}   # filename -> int
        self.open_files:    dict = {}   # handle   -> {filename, mode}
        self.next_handle:   int  = 1
        self.dedup_cache:   dict = {}   # client_id -> (seq_num, response)

        # Replication state - role is determined by _determine_startup_role()
        self.primary_addr = self.all_addrs[0]  # tentative; updated below
        self.role         = "unknown"
        self.ready        = False

        # Heartbeat tracking  { peer_addr -> last_heartbeat_time }
        # Initialise to 0 so that a freshly-started server does NOT look alive
        # before it has actually sent a heartbeat.
        self.last_heartbeat: dict = {p: 0.0 for p in peer_addrs}

        # gRPC peer connections  { addr -> (channel, fs_stub, rep_stub) }
        self.peer_stubs: dict = {}
        self._connect_peers()

        # Determine initial role by querying peers first.
        # This handles the recovery case where a server restarts after a failover: the restarting server must NOT assume it is primary just because it has the lowest address: another server may have been elected during its absence.
        self._determine_startup_role()

        # Background threads (start after sync so election doesn't fire early)
        threading.Thread(target=self._heartbeat_sender,  daemon=True).start()
        threading.Thread(target=self._failure_detector,  daemon=True).start()

    # Peer connectivity

    def _connect_peers(self):
        for addr in self.peer_addrs:
            self._connect_one_peer(addr)

    def _connect_one_peer(self, addr: str):
        # Short backoff options so the channel reconnects quickly after a peer restarts (default gRPC backoff can be several seconds, which causes the first replication attempt to fail even though the peer is up).
        options = [
            ("grpc.initial_reconnect_backoff_ms", 200),
            ("grpc.max_reconnect_backoff_ms",     1000),
            ("grpc.min_reconnect_backoff_ms",     100),
        ]
        ch = grpc.insecure_channel(addr, options=options)
        self.peer_stubs[addr] = (
            ch,
            fs_pb2_grpc.FileSystemStub(ch),
            replication_pb2_grpc.ReplicationServiceStub(ch),
        )

    def _rep_stub(self, addr: str):
        return self.peer_stubs[addr][2]

    # Startup helpers

    def _load_existing_files(self):
        """Populate file_versions for files already present on disk."""
        for fname in os.listdir(self.data_dir):
            fpath = os.path.join(self.data_dir, fname)
            if os.path.isfile(fpath) and fname not in self.file_versions:
                self.file_versions[fname] = 1

    def _determine_startup_role(self):
        """
        Decide whether to start as primary or backup by first consulting peers.

        This avoids the split-brain that occurs when a recovering server
        blindly assumes it is primary because it has the lowest address, when
        another server may have been elected during its absence.

        Logic:
        1. Query all peers for their current cluster info.
        2. If any peer names an existing primary:
           a. If that primary is THIS server -> it was already elected;
              load local files and become primary.
           b. Otherwise -> sync from that primary and become a backup.
        3. If no peer responds (fresh cluster or all peers down):
           a. If this server has the lowest address -> become primary.
           b. Otherwise -> wait for the preferred primary and sync.
        """
        claimed = self._query_peers_for_primary()

        if claimed:
            if claimed == self.my_addr:
                # Peers already know this server as primary (e.g. a cluster
                # restart where it was primary last time too).
                self.role = "primary"
                self.primary_addr = self.my_addr
                self._load_existing_files()
                self.ready = True
                print(f"[{self.my_addr}] Peers confirm this node as PRIMARY", flush=True)
            else:
                # Another server is currently primary - become a backup.
                self.role = "backup"
                self.primary_addr = claimed
                if claimed not in self.peer_stubs:
                    self._connect_one_peer(claimed)
                print(f"[{self.my_addr}] Existing primary found ({claimed}). "
                      f"Starting as BACKUP.", flush=True)
                self._sync_or_elect(claimed)
        else:
            # No peers responded - either fresh startup or all peers are down.
            preferred = self.all_addrs[0]   # lowest address preferred
            if self.my_addr == preferred:
                self.role = "primary"
                self.primary_addr = self.my_addr
                self._load_existing_files()
                self.ready = True
                print(f"[{self.my_addr}] No peers reachable. Starting as PRIMARY "
                      f"(preferred address).", flush=True)
            else:
                self.role = "backup"
                self.primary_addr = preferred
                print(f"[{self.my_addr}] No peers reachable yet. "
                      f"Attempting sync from preferred primary {preferred}.", flush=True)
                self._sync_or_elect(preferred)

    def _sync_or_elect(self, target: str):
        """Try to sync from target; fall back to a live election on failure."""
        if self._try_sync_from(target, timeout_secs=15):
            return
        print(f"[{self.my_addr}] Cannot sync from {target}. Holding election.",
              flush=True)
        self._elect_new_primary()
        if self.role == "primary":
            self._load_existing_files()
        self.ready = True

    def _try_sync_from(self, source_addr: str, timeout_secs: float) -> bool:
        """
        Attempt to sync from source_addr for up to timeout_secs.
        Returns True on success, False otherwise.
        """
        deadline = time.time() + timeout_secs
        while time.time() < deadline:
            try:
                stub = self._rep_stub(source_addr)
                resp = stub.SyncState(
                    replication_pb2.SyncStateRequest(requester_addr=self.my_addr),
                    timeout=5.0,
                )
                if resp.success:
                    with self.state_lock:
                        for entry in resp.files:
                            fpath = os.path.join(self.data_dir, entry.filename)
                            with open(fpath, "wb") as f:
                                f.write(entry.file_data)
                            self.file_versions[entry.filename] = entry.version
                        self.primary_addr = source_addr
                        self.role = "backup"
                    self.ready = True
                    print(f"[{self.my_addr}] Synced from {source_addr} "
                          f"({len(resp.files)} files). Role = BACKUP.", flush=True)
                    return True
            except grpc.RpcError as e:
                print(f"[{self.my_addr}] Sync attempt from {source_addr} failed "
                      f"({e.code()}), retrying ...", flush=True)
            time.sleep(2)
        return False

    def _query_peers_for_primary(self) -> str | None:
        """Ask each peer for cluster info; return the most commonly named primary."""
        votes: dict = {}
        for addr, (_, _, rep_stub) in self.peer_stubs.items():
            try:
                resp = rep_stub.GetClusterInfo(
                    replication_pb2.GetClusterInfoRequest(), timeout=2.0
                )
                if resp.success and resp.primary_address:
                    votes[resp.primary_address] = votes.get(resp.primary_address, 0) + 1
            except Exception:
                pass
        if not votes:
            return None
        # Return address with most votes
        return max(votes, key=lambda k: votes[k])

    # Heartbeat + failure detection

    def _heartbeat_sender(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.state_lock:
                role    = self.role
                primary = self.primary_addr

            for addr, (_, _, rep_stub) in self.peer_stubs.items():
                try:
                    rep_stub.Heartbeat(
                        replication_pb2.HeartbeatRequest(
                            sender_addr=self.my_addr,
                            role=role,
                        ),
                        timeout=1.0,
                    )
                    with self.state_lock:
                        self.last_heartbeat[addr] = time.time()
                except Exception:
                    pass

    def _failure_detector(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.state_lock:
                role    = self.role
                primary = self.primary_addr

            # Only backups need to detect primary failure
            if role == "backup" and primary != self.my_addr:
                last = self.last_heartbeat.get(primary, 0)
                if last > 0 and (time.time() - last) > HEARTBEAT_TIMEOUT:
                    print(f"[{self.my_addr}] Primary {primary} timed out. Triggering election.", flush=True)
                    self._elect_new_primary()

    # Election

    def _elect_new_primary(self):
        """
        Live election: query all peers via GetClusterInfo RPC.
        The lowest-address server among those that respond becomes primary.
        Initialised heartbeat timestamps (= 0) are NOT used to infer liveness.
        """
        alive = [self.my_addr]
        for addr, (_, _, rep_stub) in self.peer_stubs.items():
            try:
                resp = rep_stub.GetClusterInfo(
                    replication_pb2.GetClusterInfoRequest(), timeout=2.0
                )
                if resp.success:
                    alive.append(addr)
            except Exception:
                pass

        new_primary = sorted(alive)[0]

        with self.state_lock:
            self.primary_addr = new_primary
            self.role = "primary" if new_primary == self.my_addr else "backup"

        print(f"[{self.my_addr}] Election: alive={alive}  ->primary={new_primary}  "
              f"(my role={self.role})", flush=True)

        if self.role == "primary":
            self._announce_leader(new_primary)

    def _announce_leader(self, new_primary_addr: str):
        req = replication_pb2.AnnounceLeaderRequest(new_primary_address=new_primary_addr)
        for addr, (_, _, rep_stub) in self.peer_stubs.items():
            try:
                rep_stub.AnnounceLeader(req, timeout=2.0)
            except Exception:
                pass

    # Replication helpers (primary -> backups)

    def _replicate_write(self, filename: str, file_data: bytes, new_version: int):
        req = replication_pb2.ReplicateWriteRequest(
            filename=filename,
            file_data=file_data,
            new_version=new_version,
        )
        for addr, (_, _, rep_stub) in self.peer_stubs.items():
            try:
                rep_stub.ReplicateWrite(req, timeout=5.0)
            except Exception as e:
                print(f"[{self.my_addr}] Write-replication to {addr} failed: {e}", flush=True)

    def _replicate_create(self, filename: str):
        req = replication_pb2.ReplicateCreateRequest(filename=filename)
        for addr, (_, _, rep_stub) in self.peer_stubs.items():
            try:
                rep_stub.ReplicateCreate(req, timeout=5.0)
            except Exception as e:
                print(f"[{self.my_addr}] Create-replication to {addr} failed: {e}", flush=True)

    # ReplicationService RPCs  (peer-to-peer)

    def Heartbeat(self, request, context):
        with self.state_lock:
            if request.sender_addr in self.last_heartbeat:
                self.last_heartbeat[request.sender_addr] = time.time()
            primary = self.primary_addr
            role    = self.role
        return replication_pb2.HeartbeatResponse(
            alive=True,
            role=role,
            primary_address=primary,
        )

    def AnnounceLeader(self, request, context):
        new_primary = request.new_primary_address
        with self.state_lock:
            old = self.primary_addr
            self.primary_addr = new_primary
            self.role = "primary" if new_primary == self.my_addr else "backup"
        print(f"[{self.my_addr}] Leader changed {old} -> {new_primary}  "
              f"(my role={self.role})", flush=True)
        return replication_pb2.AnnounceLeaderResponse(success=True)

    def GetClusterInfo(self, request, context):
        with self.state_lock:
            primary = self.primary_addr
            role    = self.role
        return replication_pb2.GetClusterInfoResponse(
            primary_address=primary,
            my_role=role,
            success=True,
        )

    def ReplicateWrite(self, request, context):
        fpath = os.path.join(self.data_dir, request.filename)
        try:
            with self.state_lock:
                with open(fpath, "wb") as f:
                    f.write(request.file_data)
                self.file_versions[request.filename] = request.new_version
            print(f"[{self.my_addr}] Replicated write: {request.filename} "
                  f"v{request.new_version}", flush=True)
            return replication_pb2.ReplicateWriteResponse(success=True)
        except Exception as e:
            return replication_pb2.ReplicateWriteResponse(success=False, error_message=str(e))

    def ReplicateCreate(self, request, context):
        fpath = os.path.join(self.data_dir, request.filename)
        try:
            if not os.path.exists(fpath):
                open(fpath, "wb").close()
            with self.state_lock:
                self.file_versions[request.filename] = 1
            print(f"[{self.my_addr}] Replicated create: {request.filename}", flush=True)
            return replication_pb2.ReplicateCreateResponse(success=True)
        except Exception as e:
            return replication_pb2.ReplicateCreateResponse(success=False, error_message=str(e))

    def SyncState(self, request, context):
        """Send full filesystem snapshot to a recovering peer."""
        entries = []
        with self.state_lock:
            for filename, version in self.file_versions.items():
                fpath = os.path.join(self.data_dir, filename)
                try:
                    with open(fpath, "rb") as f:
                        data = f.read()
                    entries.append(replication_pb2.FileEntry(
                        filename=filename,
                        file_data=data,
                        version=version,
                    ))
                except Exception:
                    pass
        print(f"[{self.my_addr}] SyncState ->{request.requester_addr} "
              f"({len(entries)} files)", flush=True)
        return replication_pb2.SyncStateResponse(files=entries, success=True)

    # FileSystem RPCs  (client-facing)

    def _require_primary(self, context) -> bool:
        """
        If not primary, set FAILED_PRECONDITION with the primary address so
        the client can redirect.  Returns True when the caller must abort.
        """
        with self.state_lock:
            if self.role != "primary":
                context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
                context.set_details(f"NOT_PRIMARY:{self.primary_addr}")
                return True
        return False

    def _not_ready(self, context) -> bool:
        if not self.ready:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("Server syncing, try again shortly")
            return True
        return False

    # ---- Idempotency helpers ----

    def _check_idempotent(self, client_id: str, seq_num: int):
        if not client_id or seq_num <= 0:
            return None
        with self.state_lock:
            if client_id in self.dedup_cache:
                last_seq, cached = self.dedup_cache[client_id]
                if seq_num <= last_seq:
                    return cached
        return None

    def _store_response(self, client_id: str, seq_num: int, response):
        if client_id and seq_num > 0:
            with self.state_lock:
                self.dedup_cache[client_id] = (seq_num, response)

    def _full_path(self, filename: str) -> str:
        return os.path.join(self.data_dir, filename)

    # ---- TestVersionNumber - any replica can answer ----

    def TestVersionNumber(self, request, context):
        if self._not_ready(context):
            return fs_pb2.TestVersionResponse(version=0, success=False,
                                              error_message="Server not ready")
        with self.state_lock:
            version = self.file_versions.get(request.filename, 1)
        return fs_pb2.TestVersionResponse(version=version, success=True, error_message="")

    # ---- Create - primary only ----

    def Create(self, request, context):
        if self._not_ready(context):
            return fs_pb2.CreateResponse(file_handle=-1, success=False,
                                         error_message="Server not ready")
        if self._require_primary(context):
            return fs_pb2.CreateResponse(file_handle=-1, success=False,
                                         error_message="Not primary")
        cached = self._check_idempotent(request.client_id, request.seq_num)
        if cached is not None:
            return cached

        fpath = self._full_path(request.filename)
        if os.path.exists(fpath):
            return fs_pb2.CreateResponse(file_handle=-1, success=False,
                                         error_message="File already exists")
        try:
            open(fpath, "wb").close()
            with self.state_lock:
                self.file_versions[request.filename] = 1

            self._replicate_create(request.filename)

            resp = fs_pb2.CreateResponse(file_handle=0, success=True, error_message="")
            self._store_response(request.client_id, request.seq_num, resp)
            print(f"[{self.my_addr}] Created: {request.filename}", flush=True)
            return resp
        except Exception as e:
            return fs_pb2.CreateResponse(file_handle=-1, success=False, error_message=str(e))

    # ---- Open - any replica for reads; primary only for writes ----

    def Open(self, request, context):
        if self._not_ready(context):
            return fs_pb2.OpenResponse(file_handle=-1, success=False,
                                       error_message="Server not ready")
        if request.mode not in ("r", "w"):
            return fs_pb2.OpenResponse(file_handle=-1, success=False,
                                       error_message="Mode must be 'r' or 'w'")
        if request.mode == "w" and self._require_primary(context):
            return fs_pb2.OpenResponse(file_handle=-1, success=False,
                                       error_message="Not primary")

        fpath = self._full_path(request.filename)
        try:
            if not os.path.exists(fpath) and request.mode == "r":
                return fs_pb2.OpenResponse(file_handle=-1, success=False,
                                           error_message="File not found")

            file_data = b""
            if request.fetch_data and os.path.exists(fpath):
                with open(fpath, "rb") as f:
                    file_data = f.read()

            with self.state_lock:
                handle = self.next_handle
                self.next_handle += 1
                self.open_files[handle] = {"filename": request.filename, "mode": request.mode}
                if request.filename not in self.file_versions and os.path.exists(fpath):
                    self.file_versions[request.filename] = 1
                version = self.file_versions.get(request.filename, 1)

            return fs_pb2.OpenResponse(
                file_handle=handle,
                file_data=file_data,
                server_version=version,
                success=True,
            )
        except Exception as e:
            return fs_pb2.OpenResponse(file_handle=-1, success=False, error_message=str(e))

    # ---- Close - primary only for write-mode ----

    def Close(self, request, context):
        if self._not_ready(context):
            return fs_pb2.CloseResponse(success=False, error_message="Server not ready")

        with self.state_lock:
            file_info = self.open_files.get(request.file_handle)

        if file_info and file_info["mode"] == "w" and self._require_primary(context):
            return fs_pb2.CloseResponse(success=False, error_message="Not primary")

        cached = self._check_idempotent(request.client_id, request.seq_num)
        if cached is not None:
            return cached

        with self.state_lock:
            file_info = self.open_files.pop(request.file_handle, None)
        if not file_info:
            return fs_pb2.CloseResponse(success=False, error_message="Invalid file handle")

        try:
            filename    = file_info["filename"]
            new_version = 0

            if file_info["mode"] == "w":
                fpath = self._full_path(filename)
                with open(fpath, "wb") as f:
                    f.write(request.file_data)

                with self.state_lock:
                    cur = self.file_versions.get(filename, 1)
                    self.file_versions[filename] = cur + 1
                    new_version = self.file_versions[filename]

                # Replicate to backups BEFORE ACKing the client
                self._replicate_write(filename, request.file_data, new_version)
                print(f"[{self.my_addr}] Committed write: {filename} v{new_version}", flush=True)

            resp = fs_pb2.CloseResponse(success=True, new_version=new_version)
            self._store_response(request.client_id, request.seq_num, resp)
            return resp
        except Exception as e:
            return fs_pb2.CloseResponse(success=False, error_message=str(e))


# Entry point

def main():
    parser = argparse.ArgumentParser(description="Replicated DSFS server (Task 3)")
    parser.add_argument("--addr",     required=True,
                        help="This server's 'host:port', e.g. localhost:50050")
    parser.add_argument("--peers",    required=True,
                        help="Comma-separated peer addresses, "
                             "e.g. localhost:50051,localhost:50052")
    parser.add_argument("--data-dir", default=None,
                        help="Directory for file storage (default: ./data_<port>)")
    args = parser.parse_args()

    peer_addrs = [p.strip() for p in args.peers.split(",") if p.strip()]
    port       = args.addr.split(":")[-1]
    data_dir   = args.data_dir or f"./data_{port}"

    servicer = ReplicatedDSFSServer(
        my_addr=args.addr,
        peer_addrs=peer_addrs,
        data_dir=data_dir,
    )

    grpc_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ("grpc.max_send_message_length",    GRPC_MAX_MSG_SIZE),
            ("grpc.max_receive_message_length",  GRPC_MAX_MSG_SIZE),
        ],
    )
    fs_pb2_grpc.add_FileSystemServicer_to_server(servicer, grpc_server)
    replication_pb2_grpc.add_ReplicationServiceServicer_to_server(servicer, grpc_server)

    grpc_server.add_insecure_port(f"[::]:{port}")
    print(f"[{args.addr}] gRPC server listening on port {port}", flush=True)
    grpc_server.start()
    grpc_server.wait_for_termination()


if __name__ == "__main__":
    main()
