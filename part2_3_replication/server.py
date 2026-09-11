import os
import threading
import time
import grpc
from concurrent import futures
import fs_pb2
import fs_pb2_grpc

BASE_DIR = "./server_data"
os.makedirs(BASE_DIR, exist_ok=True)

class DSFSServicer(fs_pb2_grpc.FileSystemServicer):
    def __init__(self):
        self.open_files = {}  
        self.file_versions = {} 
        self.file_locks = {}
        self.notifications = {}
        self.next_handle = 1
        self.state_lock = threading.Lock()

    def _get_full_path(self, filename):
        return os.path.join(BASE_DIR, filename)

    def _push_notification_unlocked(self, client_id, message):
        if not client_id:
            return
        self.notifications.setdefault(client_id, []).append(message)

    def _cleanup_expired_lock_unlocked(self, filename):
        lock_info = self.file_locks.get(filename)
        if not lock_info:
            return

        if time.time() < lock_info["expires_at"]:
            return

        timer = lock_info.get("timer")
        if timer is not None:
            timer.cancel()

        self.file_locks.pop(filename, None)
        owner = lock_info.get("owner")
        self._push_notification_unlocked(owner, f"Lock on '{filename}' timed out and was released automatically.")

    def _is_locked_unlocked(self, filename):
        self._cleanup_expired_lock_unlocked(filename)
        return filename in self.file_locks

    def _auto_release_lock(self, filename, owner):
        with self.state_lock:
            lock_info = self.file_locks.get(filename)
            if not lock_info:
                return
            if lock_info.get("owner") != owner:
                return
            if time.time() < lock_info["expires_at"]:
                return

            self.file_locks.pop(filename, None)
            self._push_notification_unlocked(owner, f"Lock on '{filename}' timed out and was released automatically.")

    def TestVersionNumber(self, request, context):
        with self.state_lock:
            version = self.file_versions.get(request.filename, 1) 
        return fs_pb2.TestVersionResponse(version=version, success=True, error_message="")

    def Create(self, request, context):
        filepath = self._get_full_path(request.filename)
        if os.path.exists(filepath):
            return fs_pb2.CreateResponse(file_handle=-1, success=False, error_message="File already exists")
        
        try:
            open(filepath, 'wb').close()
            with self.state_lock:
                self.file_versions[request.filename] = 1
            return fs_pb2.CreateResponse(file_handle=0, success=True, error_message="")
        except Exception as e:
            return fs_pb2.CreateResponse(file_handle=-1, success=False, error_message=str(e))

    def Open(self, request, context):
        if request.mode not in ['r', 'w']:
            return fs_pb2.OpenResponse(file_handle=-1, success=False, error_message="Mode must be 'r' or 'w'")
            
        filepath = self._get_full_path(request.filename)
        
        try:
            if not os.path.exists(filepath) and request.mode == 'r':
                return fs_pb2.OpenResponse(file_handle=-1, success=False, error_message="File not found")
                
            file_data = b""
            if request.fetch_data and os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    file_data = f.read()

            with self.state_lock:
                if request.mode == 'w' and self._is_locked_unlocked(request.filename):
                    return fs_pb2.OpenResponse(file_handle=-1, success=False, error_message=f"File '{request.filename}' is currently locked for writes")

                handle = self.next_handle
                self.open_files[handle] = {"filename": request.filename, "mode": request.mode}
                self.next_handle += 1
                
                if request.filename not in self.file_versions and os.path.exists(filepath):
                    self.file_versions[request.filename] = 1
                server_version = self.file_versions.get(request.filename, 1)

            return fs_pb2.OpenResponse(file_handle=handle, file_data=file_data, server_version=server_version, success=True)
        except Exception as e:
            return fs_pb2.OpenResponse(file_handle=-1, success=False, error_message=str(e))

    def Close(self, request, context):
        with self.state_lock:
            file_info = self.open_files.get(request.file_handle)

            if not file_info:
                return fs_pb2.CloseResponse(success=False, error_message="Invalid file handle")

            if file_info['mode'] == 'w' and self._is_locked_unlocked(file_info['filename']):
                return fs_pb2.CloseResponse(success=False, error_message=f"File '{file_info['filename']}' is currently locked for writes")

            self.open_files.pop(request.file_handle, None)

        try:
            filename = file_info['filename']
            with self.state_lock:
                current_version = self.file_versions.get(filename, 1)
            
            new_version = 0
            if file_info['mode'] == 'w':
                filepath = self._get_full_path(filename)
                with open(filepath, 'wb') as f:
                    f.write(request.file_data)
                
                with self.state_lock:
                    self.file_versions[filename] = current_version + 1
                    new_version = self.file_versions[filename]
                    
            return fs_pb2.CloseResponse(success=True, new_version=new_version)
        except Exception as e:
            return fs_pb2.CloseResponse(success=False, error_message=str(e))

    def LockFile(self, request, context):
        filename = request.filename
        timeout_seconds = request.timeout_seconds
        client_id = request.client_id

        if timeout_seconds <= 0:
            return fs_pb2.LockFileResponse(success=False, error_message="timeout_seconds must be > 0", expires_at_epoch_ms=0)

        filepath = self._get_full_path(filename)
        if not os.path.exists(filepath):
            return fs_pb2.LockFileResponse(success=False, error_message="File not found", expires_at_epoch_ms=0)

        with self.state_lock:
            if self._is_locked_unlocked(filename):
                lock_info = self.file_locks[filename]
                remaining = max(0, int(lock_info["expires_at"] - time.time()))
                return fs_pb2.LockFileResponse(success=False, error_message=f"File '{filename}' is already locked for writes (about {remaining}s remaining)", expires_at_epoch_ms=int(lock_info["expires_at"] * 1000))

            expires_at = time.time() + timeout_seconds
            timer = threading.Timer(timeout_seconds, self._auto_release_lock, args=(filename, client_id))
            timer.daemon = True

            self.file_locks[filename] = {"owner": client_id, "expires_at": expires_at, "timer": timer}

            timer.start()

            return fs_pb2.LockFileResponse(success=True, error_message="", expires_at_epoch_ms=int(expires_at * 1000))

    def UnlockFile(self, request, context):
        filename = request.filename
        client_id = request.client_id

        with self.state_lock:
            self._cleanup_expired_lock_unlocked(filename)
            lock_info = self.file_locks.get(filename)

            if not lock_info:
                return fs_pb2.UnlockFileResponse(success=False, error_message=f"File '{filename}' is not locked")

            owner = lock_info.get("owner")
            if owner != client_id:
                return fs_pb2.UnlockFileResponse(success=False, error_message=f"Only lock owner can unlock file '{filename}'")

            timer = lock_info.get("timer")
            if timer is not None:
                timer.cancel()

            self.file_locks.pop(filename, None)
            self._push_notification_unlocked(owner, f"Lock on '{filename}' was released explicitly.")

            return fs_pb2.UnlockFileResponse(success=True, error_message="")

    def GetLockNotifications(self, request, context):
        with self.state_lock:
            messages = self.notifications.pop(request.client_id, [])
        return fs_pb2.LockNotificationResponse(messages=messages, success=True, error_message="")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fs_pb2_grpc.add_FileSystemServicer_to_server(DSFSServicer(), server)
    server.add_insecure_port('[::]:50051')
    print("[STARTING] gRPC DSFS Server listening on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()