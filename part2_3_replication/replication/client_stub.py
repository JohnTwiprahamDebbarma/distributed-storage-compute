import os
import sys
import time
import uuid
import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import fs_pb2
import fs_pb2_grpc
import replication_pb2
import replication_pb2_grpc

CLIENT_CACHE_DIR = "./temp_cache_files"
GRPC_MAX_MSG_SIZE = 64 * 1024 * 1024   # 64 MB

class ReplicatedDSFSClientStub:
    def __init__(self, server_addresses: list, cache_dir: str = CLIENT_CACHE_DIR,
                 max_retries: int = 5):
        """
        server_addresses – list of "host:port" strings for ALL replicas
        cache_dir        – local directory for cached files
        """
        self.server_addresses = server_addresses
        self.cache_dir        = cache_dir
        self.max_retries      = max_retries
        self.timeout          = 5.0
        os.makedirs(cache_dir, exist_ok=True)

        self.client_id = str(uuid.uuid4())
        self.seq_num   = 0

        self.cache_metadata: dict = {}   # filename -> {version, path}
        self.active_handles: dict = {}   # handle   -> {path, filename}

        # Build a connection for every known replica
        # { addr -> (channel, fs_stub, rep_stub) }
        self._conns: dict = {}
        for addr in server_addresses:
            self._connect(addr)

        # Discover which server is currently the primary
        self._primary_addr: str = self._discover_primary()
        print(f"[CLIENT] Primary = {self._primary_addr}")

    # Connection management

    def _connect(self, addr: str):
        ch = grpc.insecure_channel(addr, options=[
            ("grpc.max_send_message_length",    GRPC_MAX_MSG_SIZE),
            ("grpc.max_receive_message_length",  GRPC_MAX_MSG_SIZE),
        ])
        self._conns[addr] = (
            ch,
            fs_pb2_grpc.FileSystemStub(ch),
            replication_pb2_grpc.ReplicationServiceStub(ch),
        )

    def _fs_stub(self, addr: str = None):
        addr = addr or self._primary_addr
        return self._conns[addr][1]

    def _rep_stub(self, addr: str):
        return self._conns[addr][2]

    # Primary discovery

    def _discover_primary(self) -> str:
        """Ask each known server for cluster info; return the primary address."""
        for addr in self.server_addresses:
            try:
                resp = self._rep_stub(addr).GetClusterInfo(
                    replication_pb2.GetClusterInfoRequest(), timeout=2.0
                )
                if resp.success and resp.primary_address:
                    primary = resp.primary_address
                    # Ensure we have a connection to the primary
                    if primary not in self._conns:
                        self._connect(primary)
                    return primary
            except Exception:
                continue
        # Fallback: use the first address in the list
        return self.server_addresses[0]

    def _handle_not_primary(self, details: str) -> bool:
        """
        Parse a NOT_PRIMARY:<addr> error detail, update primary, and return True
        so the caller can retry. Returns False if the detail is not that error.
        """
        if details and details.startswith("NOT_PRIMARY:"):
            new_primary = details.split(":", 1)[1]
            if new_primary not in self._conns:
                self._connect(new_primary)
            self._primary_addr = new_primary
            print(f"[CLIENT] Redirected to new primary: {new_primary}")
            return True
        return False

    # RPC execution with retry + redirect

    def _next_seq(self) -> int:
        self.seq_num += 1
        return self.seq_num

    def _execute_rpc(self, rpc_call, request, addr: str = None):
        """
        Execute an RPC with:
          - Transparent redirect on NOT_PRIMARY
          - Retry with exponential backoff on transient errors
          - Primary re-discovery if the current primary is unreachable
        """
        addr = addr or self._primary_addr
        last_err = None

        for attempt in range(self.max_retries + 1):
            try:
                stub_fn = rpc_call(self._conns[addr][1])   # pass fs_stub
                response = stub_fn(request, timeout=self.timeout)

                # Application-level failure
                if not response.success:
                    err = response.error_message
                    # Transparent redirect
                    if self._handle_not_primary(err):
                        addr = self._primary_addr
                        continue
                    raise Exception(err)

                return response

            except grpc.RpcError as e:
                last_err = e
                details  = e.details() or ""

                # Server told us to redirect
                if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    if self._handle_not_primary(details):
                        addr = self._primary_addr
                        continue
                    raise Exception(f"gRPC FAILED_PRECONDITION: {details}")

                # Transient errors – rediscover primary and retry
                if e.code() in (grpc.StatusCode.UNAVAILABLE,
                                grpc.StatusCode.DEADLINE_EXCEEDED,
                                grpc.StatusCode.RESOURCE_EXHAUSTED):
                    if attempt < self.max_retries:
                        backoff = 2 ** attempt
                        print(f"[CLIENT] {e.code()} – rediscovering primary, "
                              f"retry {attempt+1}/{self.max_retries} in {backoff}s …")
                        time.sleep(backoff)
                        self._primary_addr = self._discover_primary()
                        addr = self._primary_addr
                        continue
                    raise Exception(f"RPC failed after {self.max_retries} retries: {details}")

                raise Exception(f"gRPC error: {details}")

        raise Exception(f"RPC failed: {last_err}")

    # Convenient wrappers that hide the stub-selection closure

    def _rpc(self, method_name: str):
        """Return a callable f(stub) -> bound_method for _execute_rpc."""
        return lambda stub: getattr(stub, method_name)

    # Cache helpers (identical to original client_stub.py)

    def _cache_path(self, filename: str, version: int) -> str:
        return os.path.join(self.cache_dir, f"{filename}_v{version}.bin")

    def _delete_old_cached_versions(self, filename: str, keep_path: str = None):
        prefix   = f"{filename}_v"
        suffix   = ".bin"
        keep_abs = os.path.abspath(keep_path) if keep_path else None
        for entry in os.listdir(self.cache_dir):
            if entry.startswith(prefix) and entry.endswith(suffix):
                path = os.path.join(self.cache_dir, entry)
                if keep_abs is None or os.path.abspath(path) != keep_abs:
                    try:
                        os.remove(path)
                    except FileNotFoundError:
                        pass

    # Public DSFS API

    def create(self, filename: str) -> int:
        seq = self._next_seq()
        req = fs_pb2.CreateRequest(filename=filename,
                                   client_id=self.client_id,
                                   seq_num=seq)
        return self._execute_rpc(self._rpc("Create"), req).file_handle

    def test_version_number(self, filename: str) -> int:
        req  = fs_pb2.TestVersionRequest(filename=filename)
        # TestVersionNumber can be served by any replica; try primary first
        for attempt in range(self.max_retries + 1):
            try:
                stub = self._fs_stub()
                resp = stub.TestVersionNumber(req, timeout=self.timeout)
                if resp.success:
                    return resp.version
            except grpc.RpcError:
                if attempt < self.max_retries:
                    self._primary_addr = self._discover_primary()
                    time.sleep(1)
        raise Exception("TestVersionNumber failed on all replicas")

    def open(self, filename: str, mode: str = "r") -> int:
        if mode not in ("r", "w"):
            raise ValueError("Mode must be 'r' or 'w'")

        fetch_data = True

        if filename in self.cache_metadata:
            local_version  = self.cache_metadata[filename]["version"]
            server_version = self.test_version_number(filename)
            cached_path    = self.cache_metadata[filename]["path"]

            if server_version == local_version and os.path.exists(cached_path):
                fetch_data = False
                print(f"$$$$[CACHE HIT] {filename} v{local_version} – skipping download.")
            elif server_version == local_version:
                print(f"$$$$[CACHE MISS] {filename} cache file missing. Fetching …")
            else:
                print(f"$$$$[CACHE MISS] {filename} outdated "
                      f"(local v{local_version}, server v{server_version}). Fetching …")

        req      = fs_pb2.OpenRequest(filename=filename, mode=mode, fetch_data=fetch_data)
        response = self._execute_rpc(self._rpc("Open"), req)

        handle         = response.file_handle
        server_version = response.server_version

        if fetch_data:
            local_path = self._cache_path(filename, server_version)
            with open(local_path, "wb") as f:
                f.write(response.file_data)
            self.cache_metadata[filename] = {"version": server_version, "path": local_path}
            self._delete_old_cached_versions(filename, keep_path=local_path)
        else:
            local_path = self.cache_metadata[filename]["path"]

        self.active_handles[handle] = {"path": local_path, "filename": filename, "mode": mode}
        return handle

    def read(self, file_handle: int, offset: int, length: int) -> bytes:
        if file_handle not in self.active_handles:
            raise Exception("Invalid file handle")
        local_path = self.active_handles[file_handle]["path"]
        with open(local_path, "rb") as f:
            f.seek(offset)
            return f.read(length)

    def write(self, file_handle: int, offset: int, data) -> int:
        if file_handle not in self.active_handles:
            raise Exception("Invalid file handle")
        if isinstance(data, str):
            data = data.encode("utf-8")
            
        file_info = self.active_handles[file_handle]
        local_path = file_info["path"]
        
        # If writing at offset 0 and we opened in "w", we should truncate the file
        # to match standard POSIX overwrite behavior, otherwise we just overwrite
        # the first N bytes and leave the rest (which caused Test 6 to fail).
        if offset == 0 and "mode" in file_info and file_info["mode"] == "w":
            with open(local_path, "wb") as f:
                f.write(data)
        else:
            with open(local_path, "r+b") as f:
                f.seek(offset)
                f.write(data)
                
        return len(data)

    def close(self, file_handle: int) -> bool:
        if file_handle not in self.active_handles:
            raise Exception("Invalid file handle")

        file_info  = self.active_handles.pop(file_handle)
        local_path = file_info["path"]
        filename   = file_info["filename"]

        with open(local_path, "rb") as f:
            final_data = f.read()

        seq = self._next_seq()
        req = fs_pb2.CloseRequest(
            file_handle=file_handle,
            file_data=final_data,
            client_id=self.client_id,
            seq_num=seq,
        )
        response = self._execute_rpc(self._rpc("Close"), req)

        if response.new_version > 0:
            new_path = self._cache_path(filename, response.new_version)
            if os.path.abspath(local_path) != os.path.abspath(new_path):
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.replace(local_path, new_path)
            self.cache_metadata[filename] = {
                "version": response.new_version,
                "path": new_path,
            }
            self._delete_old_cached_versions(filename, keep_path=new_path)

        return True

    def disconnect(self):
        for ch, _, _ in self._conns.values():
            ch.close()
