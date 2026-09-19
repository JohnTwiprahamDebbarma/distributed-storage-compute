"""
Client stub for the Raft KV store (mini-etcd).

Reuses the same leader-redirect + failover + idempotency machinery as the
file-system Raft client (raft_client_stub.py): every mutating call carries a
(client_id, seq_num) so a retry after a failover is de-duplicated by the server,
and the stub transparently follows NOT_LEADER redirects and retries transient
errors with exponential backoff.

Example:
    client = RaftKVClient([("localhost", 50051),
                           ("localhost", 50052),
                           ("localhost", 50053)])
    client.put("city", "Kolkata")
    client.get("city")                       # b"Kolkata"
    client.cas("city", "Kolkata", "Delhi")   # True (compare-and-swap)
    client.cas_version("city", 2, "Mumbai")  # (True, 3): swap only at version 2
    client.get("city", linearizable=True)    # b"Mumbai"
"""

import time
import uuid

import grpc

import kv_pb2
import kv_pb2_grpc


class RaftKVClient:
    def __init__(self, nodes: list, max_retries: int = 5, timeout: float = 10.0):
        self.nodes = list(nodes)          # [(host, port), …]
        self.max_retries = max_retries
        self.timeout = timeout

        self.client_id = str(uuid.uuid4())
        self.seq_num = 0

        self._idx = 0
        self.channel = None
        self.stub = None
        self._connect(0)

    # Connection management
    def _connect(self, idx: int):
        if self.channel:
            try:
                self.channel.close()
            except Exception:
                pass
        host, port = self.nodes[idx % len(self.nodes)]
        self._idx = idx % len(self.nodes)
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = kv_pb2_grpc.KVServiceStub(self.channel)

    def _connect_to_address(self, address: str):
        if self.channel:
            try:
                self.channel.close()
            except Exception:
                pass
        self.channel = grpc.insecure_channel(address)
        self.stub = kv_pb2_grpc.KVServiceStub(self.channel)

    def _next_node(self):
        self._connect((self._idx + 1) % len(self.nodes))

    def _next_seq(self) -> int:
        self.seq_num += 1
        return self.seq_num

    # RPC execution with leader-redirect + failover
    def _execute(self, method_name: str, request):
        last_error = None
        for attempt in range(self.max_retries + 1):
            try:
                rpc_call = getattr(self.stub, method_name)
                response = rpc_call(request, timeout=self.timeout)

                if hasattr(response, "success") and not response.success:
                    msg = getattr(response, "error_message", "")
                    if msg.startswith("NOT_LEADER:"):
                        parts = msg.split(":", 2)
                        leader_addr = parts[2] if len(parts) > 2 and parts[2] else None
                        if leader_addr:
                            self._connect_to_address(leader_addr)
                        else:
                            self._next_node()
                        time.sleep(0.3)
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
                    self._next_node()
                    time.sleep(backoff)
                else:
                    raise Exception(f"gRPC error: {e.details()}")

        raise Exception(f"RPC failed after {self.max_retries} retries: {last_error}")

    # Public KV API
    def put(self, key: str, value) -> int:
        """Set key=value. Returns the new version of the key."""
        if isinstance(value, str):
            value = value.encode("utf-8")
        req = kv_pb2.PutRequest(key=key, value=value,
                                client_id=self.client_id, seq_num=self._next_seq())
        return self._execute("Put", req).version

    def get(self, key: str, linearizable: bool = False):
        """Return the value bytes for key, or None if absent."""
        req = kv_pb2.GetRequest(key=key, linearizable=linearizable)
        resp = self._execute("Get", req)
        return resp.value if resp.found else None

    def get_versioned(self, key: str, linearizable: bool = False):
        """Return (value, version, found)."""
        req = kv_pb2.GetRequest(key=key, linearizable=linearizable)
        resp = self._execute("Get", req)
        return (resp.value, resp.version, resp.found)

    def delete(self, key: str) -> bool:
        """Delete key. Returns whether it existed."""
        req = kv_pb2.DeleteRequest(key=key,
                                   client_id=self.client_id, seq_num=self._next_seq())
        return self._execute("Delete", req).existed

    def cas(self, key: str, expected, new_value) -> bool:
        """Set key=new_value only if its current value == expected. Returns swapped."""
        if isinstance(expected, str):
            expected = expected.encode("utf-8")
        if isinstance(new_value, str):
            new_value = new_value.encode("utf-8")
        req = kv_pb2.CasRequest(key=key, expected=expected, new_value=new_value,
                                expect_absent=False,
                                client_id=self.client_id, seq_num=self._next_seq())
        return self._execute("Cas", req).swapped

    def cas_version(self, key: str, expected_version: int, new_value):
        """Set key=new_value only if its current version == expected_version.

        Returns (swapped, version): the new version if it swapped, otherwise the
        key's current version (0 if absent). Unlike cas() this cannot be fooled
        by a value that changed and then changed back (the ABA problem).
        """
        if expected_version <= 0:
            raise ValueError("expected_version must be >= 1 (use put_if_absent for new keys)")
        if isinstance(new_value, str):
            new_value = new_value.encode("utf-8")
        req = kv_pb2.CasRequest(key=key, new_value=new_value,
                                expected_version=expected_version,
                                client_id=self.client_id, seq_num=self._next_seq())
        resp = self._execute("Cas", req)
        return resp.swapped, resp.version

    def put_if_absent(self, key: str, value) -> bool:
        """Create key=value only if it is currently absent. Returns whether it was set."""
        if isinstance(value, str):
            value = value.encode("utf-8")
        req = kv_pb2.CasRequest(key=key, new_value=value, expect_absent=True,
                                client_id=self.client_id, seq_num=self._next_seq())
        return self._execute("Cas", req).swapped

    def close(self):
        if self.channel:
            self.channel.close()
