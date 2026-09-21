"""
Key-value state machine for the Raft-replicated store (mini-etcd).

This is the deterministic state machine that Raft applies committed log entries
to. It is intentionally free of any gRPC / protobuf dependency so it can be unit
tested in isolation (see test_kv_state_machine.py) and so the same logic runs
identically on every replica.

A committed Raft log entry is a dict with (at least) `op_type`, `filename` (the
key), and `data` (bytes), plus the `client_id` / `seq_num` of the request that
created it. The RaftKVServicer wires `RaftNode.apply_fn` to
`KVStateMachine.apply`, so every replica applies the same sequence of entries and
converges to the same store.

I also keep the client session table for de-duplicating retried writes here.
Only apply() - that is, the replicated log - ever changes it, so every replica
holds the same table, and a write retried against a *new* leader after failover
is recognised as a duplicate and answered from the table instead of being
applied twice (exactly-once, as in section 6.3 of the Raft dissertation).
"""

import base64
import json
import threading
from collections import OrderedDict

# Upper bound on remembered client sessions. I evict the least recently active
# client first, in *log order*, so every replica evicts exactly the same session
# at exactly the same point and the tables never diverge.
MAX_SESSIONS = 10_000


def _encode_result(result: dict) -> dict:
    """JSON-safe copy of an apply() result (bytes values become {"b64": ...})."""
    return {k: {"b64": base64.b64encode(v).decode()} if isinstance(v, (bytes, bytearray)) else v
            for k, v in result.items()}


def _decode_result(blob: dict) -> dict:
    return {k: base64.b64decode(v["b64"]) if isinstance(v, dict) and "b64" in v else v
            for k, v in blob.items()}


class KVStateMachine:
    def __init__(self, max_sessions: int = MAX_SESSIONS):
        # key (str) -> {"value": bytes, "version": int}
        self.store = {}
        # deleted key -> its last version. I keep it so a re-created key
        # continues from there and a key's versions never repeat.
        self.tombstones = {}
        # client_id -> {"seq": int, "result": dict}, least recently active first
        self.sessions = OrderedDict()
        self.max_sessions = max_sessions
        self.lock = threading.Lock()

    # Apply a committed log entry. Returns a result dict that the leader hands
    # back to the waiting client RPC.
    def apply(self, entry: dict) -> dict:
        op = entry["op_type"]
        if op == "_snapshot":
            self.restore(entry["snapshot_data"])
            return {"success": True}

        client_id = entry.get("client_id", "")
        seq_num = entry.get("seq_num", 0)
        tracked = bool(client_id) and seq_num > 0
        with self.lock:
            if tracked:
                cached = self._session_result(client_id, seq_num)
                if cached is not None:
                    return cached   # a retry of an already-applied write
            result = self._apply_op(op, entry)
            if tracked:
                self._remember(client_id, seq_num, result)
            return result

    def _apply_op(self, op: str, entry: dict) -> dict:
        """Apply one operation to the store. Call with self.lock held."""
        key = entry.get("filename", "")
        data = entry.get("data", b"") or b""

        if op == "put":
            return {"success": True, "version": self._write(key, data)}

        if op == "delete":
            cur = self.store.pop(key, None)
            if cur is not None:
                self.tombstones[key] = cur["version"]
            return {"success": True, "existed": cur is not None}

        if op == "cas":
            spec = json.loads(data.decode("utf-8"))
            expect_absent = spec.get("expect_absent", False)
            expected_version = spec.get("expected_version", 0)
            expected = base64.b64decode(spec.get("expected", "") or "")
            new_value = base64.b64decode(spec.get("new", "") or "")
            cur = self.store.get(key)
            if expect_absent:
                condition = cur is None
            elif expected_version > 0:
                # I compare versions rather than values here: a value that
                # changed and changed back (A -> B -> A) has a new version, so
                # this can't be fooled by the ABA problem the value comparison
                # below is exposed to.
                condition = cur is not None and cur["version"] == expected_version
            else:
                condition = cur is not None and cur["value"] == expected
            if condition:
                version = self._write(key, new_value)
                return {"success": True, "swapped": True,
                        "current": new_value, "version": version}
            return {"success": True, "swapped": False,
                    "current": cur["value"] if cur else b"",
                    "version": cur["version"] if cur else 0}

        # noop and anything else: nothing to apply
        return {"success": True}

    def _write(self, key: str, value: bytes) -> int:
        """Store value under key and return its new version. Call with self.lock held."""
        cur = self.store.get(key)
        version = (cur["version"] if cur else self.tombstones.pop(key, 0)) + 1
        self.store[key] = {"value": value, "version": version}
        return version

    # Client sessions (replicated de-duplication)
    def _session_result(self, client_id: str, seq_num: int):
        """Result for an already-applied (client_id, seq_num), else None.
        Call with self.lock held; never modifies the table."""
        session = self.sessions.get(client_id)
        if session is None or seq_num > session["seq"]:
            return None
        if seq_num == session["seq"]:
            return dict(session["result"])
        # Older than the client's latest request: the client has already moved on
        # (it sends one request at a time), so nobody is waiting for this answer.
        return {"success": True, "stale": True}

    def _remember(self, client_id: str, seq_num: int, result: dict):
        """Record a client's latest applied request. Call with self.lock held."""
        self.sessions[client_id] = {"seq": seq_num, "result": dict(result)}
        self.sessions.move_to_end(client_id)
        while len(self.sessions) > self.max_sessions:
            self.sessions.popitem(last=False)

    def lookup_session(self, client_id: str, seq_num: int):
        """Result of an already-applied (client_id, seq_num), else None.

        Lets the servicer answer a retry without appending it to the log again.
        The authoritative check is the one in apply(): a duplicate that has not
        been applied yet at lookup time is caught there instead.
        """
        if not client_id or seq_num <= 0:
            return None
        with self.lock:
            return self._session_result(client_id, seq_num)

    # Direct read of applied state (used to serve Get on the leader).
    def get(self, key: str):
        with self.lock:
            e = self.store.get(key)
            if e is None:
                return (None, 0, False)
            return (e["value"], e["version"], True)

    # Serialize / restore the whole state machine (for Raft snapshots).
    def snapshot(self) -> bytes:
        with self.lock:
            blob = {
                "store": {k: {"value": base64.b64encode(v["value"]).decode(),
                              "version": v["version"]}
                          for k, v in self.store.items()},
                "tombstones": dict(self.tombstones),
                # stored as a list so the eviction order survives the round trip
                "sessions": [[cid, s["seq"], _encode_result(s["result"])]
                             for cid, s in self.sessions.items()],
            }
        return json.dumps(blob).encode("utf-8")

    def restore(self, data):
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8")
        blob = json.loads(data) if data else {}
        with self.lock:
            self.store = {k: {"value": base64.b64decode(v["value"]),
                              "version": v["version"]}
                          for k, v in blob.get("store", {}).items()}
            self.tombstones = dict(blob.get("tombstones", {}))
            self.sessions = OrderedDict(
                (cid, {"seq": seq, "result": _decode_result(res)})
                for cid, seq, res in blob.get("sessions", []))
