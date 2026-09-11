import socket
import pickle
import struct
import os
import base64
import hashlib
from cryptography.fernet import Fernet, InvalidToken

PASSPHRASE = b"VERY_SECRET_KEY_BETWEEN_SERVER_AND_ALL_CLIENTS"
SECRET_KEY = base64.urlsafe_b64encode(hashlib.sha256(PASSPHRASE).digest())
cipher = Fernet(SECRET_KEY)


def send_msg(sock, msg_dict):
    raw_bytes = pickle.dumps(msg_dict)
    encrypted_bytes = cipher.encrypt(raw_bytes)
    length = struct.pack("!Q", len(encrypted_bytes))
    sock.sendall(length + encrypted_bytes)


def recvall(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return bytes(data)


def recv_msg(sock):
    raw_msglen = recvall(sock, 8)
    if not raw_msglen:
        return None
    msglen = struct.unpack("!Q", raw_msglen)[0]
    encrypted_data = recvall(sock, msglen)
    if not encrypted_data:
        return None
    raw_bytes = cipher.decrypt(encrypted_data)
    return pickle.loads(raw_bytes)


CLIENT_CACHE_DIR = "./temp_cache_files"
os.makedirs(CLIENT_CACHE_DIR, exist_ok=True)


class DSFSClientStub:
    def __init__(self, host="localhost", port=50051):
        self.host = host
        self.port = port
        self.sock = None
        self.timeout = 5.0

        self.active_handles = {}
        self.cache_metadata = {}
        self.connect()

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

    def connect(self):
        if self.sock:
            self.sock.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))

    def _call_rpc(self, method, **kwargs):
        request = {"method": method, "args": kwargs}
        try:
            send_msg(self.sock, request)
            response = recv_msg(self.sock)
            if not response:
                raise ConnectionError("Server disconnected.")
            if not response.get("success"):
                raise Exception(f"RPC {method} failed: {response.get('error_message')}")
            return response

        except (socket.timeout, socket.error, EOFError, InvalidToken) as e:
            print(f"####[NETWORK RECOVERY] {e} detected. Reconnecting to server...")
            self.connect()
            send_msg(self.sock, request)
            response = recv_msg(self.sock)
            if not response or not response.get("success"):
                raise Exception(f"Retry failed: {response.get('error_message') if response else 'No response'}")
            return response

    def create(self, filename):
        response = self._call_rpc("create", filename=filename)
        return response["file_handle"]

    def test_version_number(self, filename):
        response = self._call_rpc("test_version_number", filename=filename)
        return response["version"]

    def open(self, filename, mode="r"):
        if mode not in ["r", "w"]:
            raise ValueError("Mode must be 'r' or 'w'")

        fetch_data = True
        local_version = 0

        if filename in self.cache_metadata:
            local_version = self.cache_metadata[filename]["version"]
            server_version = self.test_version_number(filename)

            if server_version == local_version:
                cached_path = self.cache_metadata[filename]["path"]
                if os.path.exists(cached_path):
                    fetch_data = False
                    print(f"=-->[CACHE HIT] {filename} is up-to-date (v{local_version}). \nSkipping network download.")
                else:
                    print(f"=-->[CACHE MISS] {filename} metadata exists but cache file missing. Fetching...")
            else:
                print(f"=-->[CACHE MISS] {filename} outdated \n(Local: v{local_version}, Server: v{server_version}). Fetching...")

        response = self._call_rpc("open", filename=filename, mode=mode, fetch_data=fetch_data)
        handle = response["file_handle"]
        server_version = response["server_version"]

        local_path = self._cache_path(filename, server_version)

        if fetch_data:
            with open(local_path, "wb") as f:
                f.write(response["file_data"])
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

        file_info = self.active_handles.pop(file_handle)
        local_path = file_info["path"]
        filename = file_info["filename"]

        with open(local_path, "rb") as f:
            final_data = f.read()

        response = self._call_rpc("close", file_handle=file_handle, file_data=final_data)

        if "new_version" in response:
            new_version = response["new_version"]
            new_path = self._cache_path(filename, new_version)

            if os.path.abspath(local_path) != os.path.abspath(new_path):
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.replace(local_path, new_path)

            self.cache_metadata[filename] = {"version": new_version, "path": new_path}
            self._delete_old_cached_versions(filename, keep_path=new_path)

        return response["success"]

    def disconnect(self):
        if self.sock:
            self.sock.close()
            self.sock = None