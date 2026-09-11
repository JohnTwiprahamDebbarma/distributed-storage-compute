import os
import time
import uuid
import grpc
import fs_pb2
import fs_pb2_grpc

CLIENT_CACHE_DIR = "./temp_cache_files"
os.makedirs(CLIENT_CACHE_DIR, exist_ok=True)


class DSFSClientStub:
    def __init__(self, host="localhost", port=50051):
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = fs_pb2_grpc.FileSystemStub(self.channel)
        self.timeout = 5.0
        self.client_id = str(uuid.uuid4())

        self.active_handles = {}
        self.cache_metadata = {}

    def _cache_path(self, filename, version):
        return os.path.join(CLIENT_CACHE_DIR, f"{filename}_v{version}.bin")

    def _delete_old_cached_versions(self, filename, keep_path=None):
        prefix = f"{filename}_v"
        suffix = ".bin"
        keep_abs = os.path.abspath(keep_path) if keep_path else None

        for entry in os.listdir(CLIENT_CACHE_DIR):
            if entry.startswith(prefix) and entry.endswith(suffix):
                path = os.path.join(CLIENT_CACHE_DIR, entry)
                if keep_abs is None or os.path.abspath(path) != keep_abs:
                    try:
                        os.remove(path)
                    except FileNotFoundError:
                        pass

    def _execute_rpc(self, rpc_call, request):
        try:
            response = rpc_call(request, timeout=self.timeout)
            if not response.success:
                raise Exception(response.error_message)
            return response
        except grpc.RpcError as e:
            raise Exception(f"Network/gRPC Error: {e.details()}")

    def create(self, filename):
        req = fs_pb2.CreateRequest(filename=filename)
        return self._execute_rpc(self.stub.Create, req).file_handle

    def test_version_number(self, filename):
        req = fs_pb2.TestVersionRequest(filename=filename)
        return self._execute_rpc(self.stub.TestVersionNumber, req).version

    def lock_file(self, filename, timeout_seconds):
        req = fs_pb2.LockFileRequest(filename=filename, timeout_seconds=int(timeout_seconds), client_id=self.client_id)
        response = self._execute_rpc(self.stub.LockFile, req)
        return response.expires_at_epoch_ms

    def unlock_file(self, filename):
        req = fs_pb2.UnlockFileRequest(filename=filename, client_id=self.client_id)
        self._execute_rpc(self.stub.UnlockFile, req)
        return True

    def get_lock_notifications(self):
        req = fs_pb2.LockNotificationRequest(client_id=self.client_id)
        response = self._execute_rpc(self.stub.GetLockNotifications, req)
        return list(response.messages)

    def wait_for_lock_notification(self, timeout_seconds=10, poll_interval=0.5):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            messages = self.get_lock_notifications()
            if messages:
                return messages
            time.sleep(poll_interval)
        return []

    def open(self, filename, mode="r"):
        if mode not in ["r", "w"]:
            raise ValueError("Mode must be 'r' or 'w'")

        fetch_data = True

        if filename in self.cache_metadata:
            local_version = self.cache_metadata[filename]["version"]
            server_version = self.test_version_number(filename)
            cached_path = self.cache_metadata[filename]["path"]

            if server_version == local_version and os.path.exists(cached_path):
                fetch_data = False
                print(f"=-->[CACHE HIT] {filename} is up-to-date (v{local_version}). \nSkipping network download.")
            elif server_version == local_version:
                print(f"=-->[CACHE MISS] {filename} cache file missing. Fetching...")
            else:
                print(f"=-->[CACHE MISS] {filename} outdated \n(Local: v{local_version}, Server: v{server_version}). Fetching...")

        req = fs_pb2.OpenRequest(filename=filename, mode=mode, fetch_data=fetch_data)
        response = self._execute_rpc(self.stub.Open, req)

        handle = response.file_handle
        server_version = response.server_version

        if fetch_data:
            local_path = self._cache_path(filename, server_version)
            with open(local_path, "wb") as f:
                f.write(response.file_data)

            self.cache_metadata[filename] = {"version": server_version, "path": local_path}
            self._delete_old_cached_versions(filename, keep_path=local_path)
        else:
            local_path = self.cache_metadata[filename]["path"]

        self.active_handles[handle] = {"path": local_path, "filename": filename}
        return handle

    def read(self, file_handle, offset, length):
        if file_handle not in self.active_handles:
            raise Exception("Invalid file handle")

        local_path = self.active_handles[file_handle]["path"]
        with open(local_path, "rb") as f:
            f.seek(offset)
            return f.read(length)

    def write(self, file_handle, offset, data):
        if file_handle not in self.active_handles:
            raise Exception("Invalid file handle")

        if isinstance(data, str):
            data = data.encode("utf-8")

        local_path = self.active_handles[file_handle]["path"]
        with open(local_path, "r+b") as f:
            f.seek(offset)
            f.write(data)
            return len(data)

    def close(self, file_handle):
        if file_handle not in self.active_handles:
            raise Exception("Invalid file handle")

        file_info = self.active_handles[file_handle]
        local_path = file_info["path"]
        filename = file_info["filename"]

        with open(local_path, "rb") as f:
            final_data = f.read()

        req = fs_pb2.CloseRequest(file_handle=file_handle, file_data=final_data)
        response = self._execute_rpc(self.stub.Close, req)

        self.active_handles.pop(file_handle, None)

        if response.new_version > 0:
            new_path = self._cache_path(filename, response.new_version)

            if os.path.abspath(local_path) != os.path.abspath(new_path):
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.replace(local_path, new_path)

            self.cache_metadata[filename] = {"version": response.new_version, "path": new_path}
            self._delete_old_cached_versions(filename, keep_path=new_path)

        return True

    def disconnect(self):
        self.channel.close()