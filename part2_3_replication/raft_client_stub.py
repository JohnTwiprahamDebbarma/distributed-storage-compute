import os
import time
import uuid
import grpc
import fs_pb2
import fs_pb2_grpc

CLIENT_CACHE_DIR = "./temp_cache_files"
os.makedirs(CLIENT_CACHE_DIR, exist_ok=True)


class RaftDSFSClientStub:
    def __init__(self, nodes: list, max_retries: int = 5,
                 cache_dir: str = CLIENT_CACHE_DIR):
        self.nodes= list(nodes)          # [(host, port), ...]
        self.max_retries = max_retries
        self.cache_dir = cache_dir
        self.timeout= 10.0

        # Idempotency
        self.client_id = str(uuid.uuid4())
        self.seq_num = 0

        # Cache
        self.cache_metadata = {}   # filename -> {version, path}
        self.active_handles = {}   # handle   -> {path, filename}

        # Current connection
        self._current_idx = 0      # index into self.nodes
        self.channel = None
        self.stub = None
        self._connect(self._current_idx)

    # Connection management
    def _connect(self, idx: int):
        if self.channel:
            try:
                self.channel.close()
            except Exception:
                pass
        host, port = self.nodes[idx % len(self.nodes)]
        self._current_idx = idx % len(self.nodes)
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = fs_pb2_grpc.FileSystemStub(self.channel)

    def _connect_to_address(self, address: str):
        if self.channel:
            try:
                self.channel.close()
            except Exception:
                pass
        self.channel = grpc.insecure_channel(address)
        self.stub    = fs_pb2_grpc.FileSystemStub(self.channel)

    def _next_node(self):
        self._connect((self._current_idx + 1) % len(self.nodes))

    # RPC execution with leader-redirect + failover
    def _execute_rpc(self, method_name: str, request):
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                # Re-bind on every attempt so that after a redirect/reconnect each call goes through the current (live) stub, not a bound method captured from a now-closed channel.
                rpc_call = getattr(self.stub, method_name)
                response = rpc_call(request, timeout=self.timeout)

                # Check for NOT_LEADER redirect in error_message field
                if hasattr(response, "success") and not response.success:
                    msg = getattr(response, "error_message", "")
                    if msg.startswith("NOT_LEADER:"):
                        # Parse NOT_LEADER:<id>:<host:port>
                        parts = msg.split(":", 2)
                        leader_addr = parts[2] if len(parts) > 2 and parts[2] else None
                        if leader_addr:
                            print(f"####[REDIRECT] Connecting to leader at {leader_addr}")
                            self._connect_to_address(leader_addr)
                        else:
                            print("####[REDIRECT] No leader address, round-robining...")
                            self._next_node()
                        time.sleep(0.5)
                        continue
                    raise Exception(msg)

                return response

            except grpc.RpcError as e:
                last_error = e
                if e.code() in (grpc.StatusCode.UNAVAILABLE,
                                 grpc.StatusCode.DEADLINE_EXCEEDED,
                                 grpc.StatusCode.RESOURCE_EXHAUSTED,
                                 grpc.StatusCode.ABORTED):
                    backoff = min(2 ** attempt, 8)
                    print(f"####[RETRY {attempt+1}/{self.max_retries}] "
                          f"{e.code()} - switching node, retrying in {backoff}s...")
                    self._next_node()
                    time.sleep(backoff)
                else:
                    raise Exception(f"gRPC error: {e.details()}")

        raise Exception(f"RPC failed after {self.max_retries} retries: {last_error}")

    def _next_seq(self):
        self.seq_num += 1
        return self.seq_num

    # Cache helpers
    def _cache_path(self, filename, version):
        return os.path.join(self.cache_dir, f"{filename}_v{version}.bin")

    def _delete_old_cached_versions(self, filename, keep_path=None):
        prefix= f"{filename}_v"
        suffix= ".bin"
        keep_abs = os.path.abspath(keep_path) if keep_path else None
        for entry in os.listdir(self.cache_dir):
            if entry.startswith(prefix) and entry.endswith(suffix):
                path = os.path.join(self.cache_dir, entry)
                if keep_abs is None or os.path.abspath(path) != keep_abs:
                    try:
                        os.remove(path)
                    except FileNotFoundError:
                        pass

    # Public DSFS API (identical signature to DSFSClientStub in client_stub.py)
    def create(self, filename: str) -> int:
        seq = self._next_seq()
        req = fs_pb2.CreateRequest(
            filename=filename, client_id=self.client_id, seq_num=seq)
        return self._execute_rpc("Create", req).file_handle

    def test_version_number(self, filename: str) -> int:
        req = fs_pb2.TestVersionRequest(filename=filename)
        return self._execute_rpc("TestVersionNumber", req).version

    def open(self, filename: str, mode: str = "r") -> int:
        if mode not in ("r", "w"):
            raise ValueError("Mode must be 'r' or 'w'")

        fetch_data = True

        if filename in self.cache_metadata:
            local_ver= self.cache_metadata[filename]["version"]
            server_ver = self.test_version_number(filename)
            cached_path = self.cache_metadata[filename]["path"]

            if server_ver == local_ver and os.path.exists(cached_path):
                fetch_data = False
                print(f"$$$$[CACHE HIT] {filename} v{local_ver} - skipping download")
            else:
                print(f"$$$$[CACHE MISS] {filename} "
                      f"(local v{local_ver}, server v{server_ver})")

        req = fs_pb2.OpenRequest(filename=filename, mode=mode,
                                      fetch_data=fetch_data)
        response = self._execute_rpc("Open", req)

        handle = response.file_handle
        server_version = response.server_version

        if fetch_data:
            local_path = self._cache_path(filename, server_version)
            with open(local_path, "wb") as f:
                f.write(response.file_data)
            self.cache_metadata[filename] = {
                "version": server_version, "path": local_path}
            self._delete_old_cached_versions(filename, keep_path=local_path)
        else:
            local_path = self.cache_metadata[filename]["path"]

        self.active_handles[handle] = {"path": local_path, "filename": filename}
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
        local_path = self.active_handles[file_handle]["path"]
        with open(local_path, "r+b") as f:
            f.seek(offset)
            f.write(data)
        return len(data)

    def close(self, file_handle: int) -> bool:
        if file_handle not in self.active_handles:
            raise Exception("Invalid file handle")

        file_info= self.active_handles.pop(file_handle)
        local_path = file_info["path"]
        filename = file_info["filename"]

        with open(local_path, "rb") as f:
            final_data = f.read()

        seq = self._next_seq()
        req = fs_pb2.CloseRequest(
            file_handle=file_handle, file_data=final_data,
            client_id=self.client_id, seq_num=seq)
        response = self._execute_rpc("Close", req)

        if response.new_version > 0:
            new_path = self._cache_path(filename, response.new_version)
            if os.path.abspath(local_path) != os.path.abspath(new_path):
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.replace(local_path, new_path)
            self.cache_metadata[filename] = {
                "version": response.new_version, "path": new_path}
            self._delete_old_cached_versions(filename, keep_path=new_path)

        return True

    def disconnect(self):
        if self.channel:
            self.channel.close()
