"""
Key-value state machine for the Raft-replicated store (mini-etcd).

This is the deterministic state machine that Raft applies committed log entries
to. It is intentionally free of any gRPC / protobuf dependency so it can be unit
tested in isolation (see test_kv_state_machine.py) and so the same logic runs
identically on every replica.

A committed Raft log entry is a dict with (at least) `op_type`, `filename` (the
key), and `data` (bytes). The RaftKVServicer wires `RaftNode.apply_fn` to
`KVStateMachine.apply`, so every replica applies the same sequence of entries and
converges to the same store.
"""

import base64
import json
import threading


class KVStateMachine:
    def __init__(self):
        # key (str) -> {"value": bytes, "version": int}
        self.store = {}
        self.lock = threading.Lock()

    # Apply a committed log entry. Returns a result dict that the leader hands
    # back to the waiting client RPC.
    def apply(self, entry: dict) -> dict:
        op = entry["op_type"]
        key = entry.get("filename", "")
        data = entry.get("data", b"") or b""

        if op == "put":
            with self.lock:
                prev = self.store.get(key)
                version = (prev["version"] + 1) if prev else 1
                self.store[key] = {"value": data, "version": version}
            return {"success": True, "version": version}

        if op == "delete":
            with self.lock:
                existed = key in self.store
                self.store.pop(key, None)
            return {"success": True, "existed": existed}

        if op == "cas":
            spec = json.loads(data.decode("utf-8"))
            expect_absent = spec.get("expect_absent", False)
            expected = base64.b64decode(spec.get("expected", "") or "")
            new_value = base64.b64decode(spec.get("new", "") or "")
            with self.lock:
                cur = self.store.get(key)
                if expect_absent:
                    condition = cur is None
                else:
                    condition = cur is not None and cur["value"] == expected
                if condition:
                    version = (cur["version"] + 1) if cur else 1
                    self.store[key] = {"value": new_value, "version": version}
                    return {"success": True, "swapped": True,
                            "current": new_value, "version": version}
                return {"success": True, "swapped": False,
                        "current": cur["value"] if cur else b"",
                        "version": cur["version"] if cur else 0}

        if op == "_snapshot":
            self.restore(entry["snapshot_data"])
            return {"success": True}

        # noop and anything else: nothing to apply
        return {"success": True}

    # Direct read of applied state (used to serve Get on the leader).
    def get(self, key: str):
        with self.lock:
            e = self.store.get(key)
            if e is None:
                return (None, 0, False)
            return (e["value"], e["version"], True)

    # Serialize / restore the whole store (for Raft snapshots).
    def snapshot(self) -> bytes:
        with self.lock:
            blob = {k: {"value": base64.b64encode(v["value"]).decode(),
                        "version": v["version"]}
                    for k, v in self.store.items()}
        return json.dumps(blob).encode("utf-8")

    def restore(self, data):
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8")
        blob = json.loads(data) if data else {}
        with self.lock:
            self.store = {k: {"value": base64.b64decode(v["value"]),
                              "version": v["version"]}
                          for k, v in blob.items()}
