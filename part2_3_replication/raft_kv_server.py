"""
Raft-replicated key-value store (mini-etcd).

Runs one gRPC server exposing two services on a single port:
  1. KVService   - client-facing Put / Get / Delete / Cas      (kv.proto)
  2. RaftService - inter-node Raft consensus RPCs              (raft.proto)

The consensus engine (raft_node.RaftNode) and the inter-node Raft gRPC plumbing
(RaftGRPCServicer, _build_peer_stubs) are reused UNCHANGED from the file-system
build (raft_server.py); only the state machine (kv_state_machine.KVStateMachine)
and the client-facing API differ. This is the whole point of separating Raft
from its state machine: the same consensus core replicates a file system or a
key-value store depending only on what you plug into apply_fn.

Usage:
    python raft_kv_server.py --node_id 0 --config nodes_config.json
"""

import argparse
import json
import logging
import sys
import threading
import time
from concurrent import futures

import grpc

import raft_pb2
import raft_pb2_grpc
import kv_pb2
import kv_pb2_grpc
from raft_node import RaftNode, NotLeaderError
from raft_server import RaftGRPCServicer, _build_peer_stubs  # reused Raft plumbing
from kv_state_machine import KVStateMachine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("raft_kv_server")


class RaftKVServicer(kv_pb2_grpc.KVServiceServicer):
    """Client-facing KV API on top of a RaftNode + a KVStateMachine."""

    def __init__(self, raft: RaftNode, sm: KVStateMachine):
        self.raft = raft
        self.sm = sm
        self.dedup = {}          # client_id -> (seq_num, response)
        self.lock = threading.Lock()
        # Committed entries flow into the state machine.
        self.raft.apply_fn = self.sm.apply

    # Helpers
    def _redirect(self) -> str:
        lid, addr = self.raft.get_leader_info()
        return f"NOT_LEADER:{lid}:{addr or ''}"

    def _dedup_get(self, client_id, seq_num):
        if not client_id or seq_num <= 0:
            return None
        with self.lock:
            if client_id in self.dedup:
                last_seq, resp = self.dedup[client_id]
                if seq_num <= last_seq:
                    return resp
        return None

    def _dedup_put(self, client_id, seq_num, response):
        if client_id and seq_num > 0:
            with self.lock:
                self.dedup[client_id] = (seq_num, response)

    # Mutations -> Raft log
    def Put(self, request, context):
        cached = self._dedup_get(request.client_id, request.seq_num)
        if cached is not None:
            return cached
        if not self.raft.is_leader():
            return kv_pb2.PutResponse(success=False, error_message=self._redirect())
        try:
            result = self.raft.append_entry(
                "put", request.key, request.value,
                request.client_id, request.seq_num)
            resp = kv_pb2.PutResponse(success=True, version=result.get("version", 0))
            self._dedup_put(request.client_id, request.seq_num, resp)
            return resp
        except NotLeaderError as e:
            return kv_pb2.PutResponse(
                success=False,
                error_message=f"NOT_LEADER:{e.leader_id}:{e.leader_address or ''}")
        except Exception as e:
            return kv_pb2.PutResponse(success=False, error_message=str(e))

    def Delete(self, request, context):
        cached = self._dedup_get(request.client_id, request.seq_num)
        if cached is not None:
            return cached
        if not self.raft.is_leader():
            return kv_pb2.DeleteResponse(success=False, error_message=self._redirect())
        try:
            result = self.raft.append_entry(
                "delete", request.key, b"",
                request.client_id, request.seq_num)
            resp = kv_pb2.DeleteResponse(success=True, existed=result.get("existed", False))
            self._dedup_put(request.client_id, request.seq_num, resp)
            return resp
        except NotLeaderError as e:
            return kv_pb2.DeleteResponse(
                success=False,
                error_message=f"NOT_LEADER:{e.leader_id}:{e.leader_address or ''}")
        except Exception as e:
            return kv_pb2.DeleteResponse(success=False, error_message=str(e))

    def Cas(self, request, context):
        cached = self._dedup_get(request.client_id, request.seq_num)
        if cached is not None:
            return cached
        if not self.raft.is_leader():
            return kv_pb2.CasResponse(success=False, error_message=self._redirect())
        import base64
        # The condition + new value travel together through one log entry, so the
        # compare-and-swap is evaluated atomically in the state machine at apply
        # time (it MUST go through the log - two clients cannot both win a CAS).
        cmd = json.dumps({
            "expected": base64.b64encode(request.expected).decode(),
            "new": base64.b64encode(request.new_value).decode(),
            "expect_absent": request.expect_absent,
        }).encode()
        try:
            result = self.raft.append_entry(
                "cas", request.key, cmd,
                request.client_id, request.seq_num)
            resp = kv_pb2.CasResponse(
                success=True,
                swapped=result.get("swapped", False),
                current=result.get("current", b"") or b"",
                version=result.get("version", 0))
            self._dedup_put(request.client_id, request.seq_num, resp)
            return resp
        except NotLeaderError as e:
            return kv_pb2.CasResponse(
                success=False,
                error_message=f"NOT_LEADER:{e.leader_id}:{e.leader_address or ''}")
        except Exception as e:
            return kv_pb2.CasResponse(success=False, error_message=str(e))

    # Reads -> leader
    def Get(self, request, context):
        # Reads are served by the leader. With linearizable=True we first commit a
        # no-op read barrier: it cannot commit without a current majority, which
        # proves this node is still leader and its applied state reflects every
        # acknowledged write (a simple ReadIndex). Without it a partitioned
        # "zombie" leader could briefly serve a stale value - fast, but only
        # sequentially consistent.
        if not self.raft.is_leader():
            return kv_pb2.GetResponse(success=False, error_message=self._redirect())
        try:
            if request.linearizable:
                self.raft.append_entry("noop", "", b"", "", 0)
            value, version, found = self.sm.get(request.key)
            return kv_pb2.GetResponse(
                success=True, found=found, value=value or b"", version=version)
        except NotLeaderError as e:
            return kv_pb2.GetResponse(
                success=False,
                error_message=f"NOT_LEADER:{e.leader_id}:{e.leader_address or ''}")
        except Exception as e:
            return kv_pb2.GetResponse(success=False, error_message=str(e))


def serve(node_id: int, node_configs: list):
    my_cfg = next(nc for nc in node_configs if nc["id"] == node_id)
    port = my_cfg["port"]
    raft_dir = my_cfg.get("raft_dir", f"./raft_state_node{node_id}")
    peers = {nc["id"]: f"{nc['host']}:{nc['port']}"
             for nc in node_configs if nc["id"] != node_id}

    logger.info(f"Starting KV node {node_id} on port {port} (peers: {peers})")

    raft = RaftNode(node_id=node_id, peers=peers, persist_dir=raft_dir)

    def _wire_stubs():
        time.sleep(1.0)
        raft.peer_stubs = _build_peer_stubs(node_id, node_configs)
        logger.info(f"[{node_id}] Peer stubs ready: {list(raft.peer_stubs)}")

    threading.Thread(target=_wire_stubs, daemon=True).start()

    sm = KVStateMachine()
    kv_servicer = RaftKVServicer(raft, sm)
    raft_servicer = RaftGRPCServicer(raft, node_configs)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    kv_pb2_grpc.add_KVServiceServicer_to_server(kv_servicer, server)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_servicer, server)
    server.add_insecure_port(f"[::]:{port}")

    server.start()
    logger.info(f"[node {node_id}] KV + Raft gRPC server listening on port {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"[node {node_id}] Shutting down…")
        server.stop(grace=3)


def main():
    parser = argparse.ArgumentParser(description="Raft KV store (mini-etcd)")
    parser.add_argument("--node_id", type=int, required=True,
                        help="Unique ID for this node (0, 1, 2, …)")
    parser.add_argument("--config", type=str, default="nodes_config.json",
                        help="Path to cluster config JSON")
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = json.load(f)
    node_configs = cfg["nodes"]
    if args.node_id not in [nc["id"] for nc in node_configs]:
        print(f"ERROR: node_id {args.node_id} not in config", file=sys.stderr)
        sys.exit(1)

    serve(args.node_id, node_configs)


if __name__ == "__main__":
    main()
