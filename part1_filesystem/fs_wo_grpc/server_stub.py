import socket
import pickle
import struct
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib
from cryptography.fernet import Fernet

PASSPHRASE = b'VERY_SECRET_KEY_BETWEEN_SERVER_AND_ALL_CLIENTS'
SECRET_KEY = base64.urlsafe_b64encode(hashlib.sha256(PASSPHRASE).digest())
cipher = Fernet(SECRET_KEY)

def send_msg(sock, msg_dict):
    raw_bytes = pickle.dumps(msg_dict)
    encrypted_bytes = cipher.encrypt(raw_bytes)
    length = struct.pack('!Q', len(encrypted_bytes))
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
    msglen = struct.unpack('!Q', raw_msglen)[0]
    encrypted_data = recvall(sock, msglen)
    if not encrypted_data:
        return None
    raw_bytes = cipher.decrypt(encrypted_data)
    return pickle.loads(raw_bytes)

class DSFSRPCServer:
    def __init__(self, implementation, host='0.0.0.0', port=50051):
        self.implementation = implementation
        self.host = host
        self.port = port

    def handle_client(self, conn, addr):
        print(f"=-->[NEW CONNECTION] Worker node {addr} connected.")
        conn.settimeout(150) 
        
        try:
            while True:
                request = recv_msg(conn)
                if request is None:
                    break 
                
                method = request.get("method")
                args = request.get("args", {})
                
                if method == "open":
                    response = self.implementation.open_file(**args)
                elif method == "create":
                    response = self.implementation.create_file(**args)
                elif method == "close":
                    response = self.implementation.close_file(**args)
                elif method == "test_version_number":
                    response = self.implementation.test_version_number(**args)
                else:
                    response = {"success": False, "error_message": f"Unknown method: {method}"}
                
                send_msg(conn, response)

        except Exception as e:
            print(f"=-->[DISCONNECT] Connection with {addr} closed: {e}")

        finally:
            conn.close()

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        print(f"=-->[STARTING] Secure DSFS Server listening on {self.host}:{self.port}")
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            try:
                while True:
                    conn, addr = server_socket.accept()
                    executor.submit(self.handle_client, conn, addr)
            except KeyboardInterrupt:
                print("\n=-->[SHUTDOWN] Server is stopping.")
            finally:
                server_socket.close()