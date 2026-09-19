"""
Raft-replicated DSFS gRPC server

Starts a single gRPC server that exposes two services on one port:
  1. FileSystem  - the existing client-facing file-system API (fs.proto)
  2. RaftService    - inter-node Raft consensus RPCs              (raft.proto)

Usage:
    python raft_server.py --node_id 0 --config nodes_config.json
"""

import argparse
import json
import logging
import os
import sys
import threading
import time

import grpc
from concurrent import futures # for gRPC server thread pool

import fs_pb2
import fs_pb2_grpc
import raft_pb2
import raft_pb2_grpc
from raft_node import RaftNode, NotLeaderError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("raft_server")


def abort_if_isolated(raft: RaftNode, context):
    """Fault injection: answer like an unreachable host while the node is isolated.

    context.abort() raises, so the RPC handler that calls this stops here and the
    caller sees UNAVAILABLE - the same error a crashed or partitioned node gives.
    """
    if raft.isolated:
        context.abort(grpc.StatusCode.UNAVAILABLE, "node is isolated (fault injection)")


# DSFS servicer - wraps RaftNode for client-facing operations
class RaftDSFSServicer(fs_pb2_grpc.FileSystemServicer):
    """
    Replaces the single-node DSFSServicer from server.py.

    Reads -> served locally from committed state (any node).
    Writes -> forwarded through Raft log (leader only; non-leaders redirect).
    """

    def __init__(self, raft: RaftNode, base_dir: str):
        self.raft = raft
        self.base_dir = base_dir
        os.makedirs(base_dir, exist_ok=True)

        # In-memory state (rebuilt from Raft log on restart)
        self.file_versions = {}   # filename -> version
        self.open_files = {}   # handle   -> {filename, mode}
        self.next_handle = 1
        self.dedup_cache = {}   # client_id -> (seq_num, response)
        self.lock = threading.Lock()

        # Wire apply callback into RaftNode 
        self.raft.apply_fn = self._apply_entry

    # State-machine apply function (called by RaftNode applier thread)
    def _apply_entry(self, entry: dict):
        """Apply a committed Raft log entry to the file-system state machine."""
        op      = entry["op_type"]
        fname   = entry["filename"]
        data    = entry["data"]   # bytes (already decoded by raft_node)

        if op == "create":
            path = self._path(fname)
            if not os.path.exists(path):
                open(path, "wb").close()
            with self.lock:
                self.file_versions.setdefault(fname, 1)
            return {"success": True}

        elif op == "write":
            path = self._path(fname)
            with open(path, "wb") as f:
                f.write(data)
            with self.lock:
                self.file_versions[fname] = self.file_versions.get(fname, 1) + 1
                new_ver = self.file_versions[fname]
            logger.info(f"[SM] write applied: {fname} -> v{new_ver}")
            return {"new_version": new_ver}

        elif op == "_snapshot":
            # Restore state machine from snapshot bytes
            snap = json.loads(entry["snapshot_data"])
            with self.lock:
                self.file_versions = snap.get("file_versions", {})
            # Restore files from snapshot blobs (base64-encoded)
            import base64
            for fname, b64 in snap.get("file_blobs", {}).items():
                path = self._path(fname)
                with open(path, "wb") as f:
                    f.write(base64.b64decode(b64))
            logger.info("[SM] Snapshot installed")
            return {"success": True}

        # noop - nothing to apply
        return {"success": True}

    # Helpers
    def _path(self, filename: str) -> str:
        return os.path.join(self.base_dir, filename)

    def _check_idempotent(self, client_id, seq_num):
        if not client_id or seq_num <= 0:
            return None
        with self.lock:
            if client_id in self.dedup_cache:
                last_seq, resp = self.dedup_cache[client_id]
                if seq_num <= last_seq:
                    logger.info(f"[DEDUP] dup client={client_id} seq={seq_num}")
                    return resp
        return None

    def _cache_resp(self, client_id, seq_num, response):
        if client_id and seq_num > 0:
            with self.lock:
                self.dedup_cache[client_id] = (seq_num, response)

    def _redirect_error(self):
        lid, addr = self.raft.get_leader_info()
        msg = f"NOT_LEADER:{lid}:{addr or ''}"
        return msg

    # gRPC RPCs
    def TestVersionNumber(self, request, context):
        abort_if_isolated(self.raft, context)
        with self.lock:
            version = self.file_versions.get(request.filename, 1)
        return fs_pb2.TestVersionResponse(version=version, success=True)

    def Create(self, request, context):
        abort_if_isolated(self.raft, context)
        cached = self._check_idempotent(request.client_id, request.seq_num)
        if cached is not None:
            return cached

        if not self.raft.is_leader():
            return fs_pb2.CreateResponse(
                file_handle=-1, success=False,
                error_message=self._redirect_error())

        if os.path.exists(self._path(request.filename)):
            return fs_pb2.CreateResponse(
                file_handle=-1, success=False,
                error_message="File already exists")

        try:
            self.raft.append_entry(
                op_type="create", filename=request.filename,
                data=b"", client_id=request.client_id,
                seq_num=request.seq_num)
            resp = fs_pb2.CreateResponse(file_handle=0, success=True)
            self._cache_resp(request.client_id, request.seq_num, resp)
            return resp
        except NotLeaderError as e:
            return fs_pb2.CreateResponse(
                file_handle=-1, success=False,
                error_message=f"NOT_LEADER:{e.leader_id}:{e.leader_address or ''}")
        except Exception as e:
            return fs_pb2.CreateResponse(
                file_handle=-1, success=False, error_message=str(e))

    def Open(self, request, context):
        abort_if_isolated(self.raft, context)
        if request.mode not in ("r", "w"):
            return fs_pb2.OpenResponse(
                file_handle=-1, success=False,
                error_message="Mode must be 'r' or 'w'")

        path = self._path(request.filename)
        if not os.path.exists(path) and request.mode == "r":
            return fs_pb2.OpenResponse(
                file_handle=-1, success=False, error_message="File not found")

        try:
            file_data = b""
            if request.fetch_data and os.path.exists(path):
                with open(path, "rb") as f:
                    file_data = f.read()

            with self.lock:
                handle = self.next_handle
                self.next_handle += 1
                self.open_files[handle] = {
                    "filename": request.filename, "mode": request.mode}
                if request.filename not in self.file_versions and os.path.exists(path):
                    self.file_versions[request.filename] = 1
                version = self.file_versions.get(request.filename, 1)

            return fs_pb2.OpenResponse(
                file_handle=handle, file_data=file_data,
                server_version=version, success=True)
        except Exception as e:
            return fs_pb2.OpenResponse(
                file_handle=-1, success=False, error_message=str(e))

    def Close(self, request, context):
        abort_if_isolated(self.raft, context)
        cached = self._check_idempotent(request.client_id, request.seq_num)
        if cached is not None:
            return cached

        with self.lock:
            file_info = self.open_files.pop(request.file_handle, None)

        if not file_info:
            return fs_pb2.CloseResponse(
                success=False, error_message="Invalid file handle")

        filename = file_info["filename"]

        if file_info["mode"] != "w":
            # Read-only close - no replication needed
            resp = fs_pb2.CloseResponse(success=True, new_version=0)
            self._cache_resp(request.client_id, request.seq_num, resp)
            return resp

        # Write path - must go through Raft
        if not self.raft.is_leader():
            return fs_pb2.CloseResponse(
                success=False,
                error_message=self._redirect_error())

        try:
            result = self.raft.append_entry(
                op_type="write", filename=filename,
                data=request.file_data,
                client_id=request.client_id,
                seq_num=request.seq_num)
            new_ver = result.get("new_version", 0) if isinstance(result, dict) else 0
            resp = fs_pb2.CloseResponse(success=True, new_version=new_ver)
            self._cache_resp(request.client_id, request.seq_num, resp)
            return resp
        except NotLeaderError as e:
            return fs_pb2.CloseResponse(
                success=False,
                error_message=f"NOT_LEADER:{e.leader_id}:{e.leader_address or ''}")
        except Exception as e:
            return fs_pb2.CloseResponse(
                success=False, error_message=str(e))


# Raft gRPC servicer - handles inter-node RPCs
class RaftGRPCServicer(raft_pb2_grpc.RaftServiceServicer):

    def __init__(self, raft: RaftNode, node_configs: list):
        self.raft = raft
        self.node_configs = node_configs  # list of {id, host, port}

    def RequestVote(self, request, context):
        abort_if_isolated(self.raft, context)
        args = {
            "term": request.term,
            "candidate_id": request.candidate_id,
            "last_log_index": request.last_log_index,
            "last_log_term": request.last_log_term,
        }
        reply = self.raft.handle_request_vote(args)
        return raft_pb2.RequestVoteReply(**reply)

    def AppendEntries(self, request, context):
        abort_if_isolated(self.raft, context)
        entries = []
        for e in request.entries:
            entries.append({
                "term": e.term,
                "index": e.index,
                "op_type": e.op_type,
                "filename": e.filename,
                "data": e.data.hex(),
                "client_id": e.client_id,
                "seq_num": e.seq_num,
            })
        args = {
            "term":request.term,
            "leader_id": request.leader_id,
            "prev_log_index": request.prev_log_index,
            "prev_log_term":request.prev_log_term,
            "entries": entries,
            "leader_commit":request.leader_commit,
        }
        reply = self.raft.handle_append_entries(args)
        return raft_pb2.AppendEntriesReply(**reply)

    def InstallSnapshot(self, request, context):
        abort_if_isolated(self.raft, context)
        args = {
            "term": request.term,
            "leader_id": request.leader_id,
            "last_included_index":request.last_included_index,
            "last_included_term": request.last_included_term,
            "snapshot_data":request.snapshot_data,
        }
        reply = self.raft.handle_install_snapshot(args)
        return raft_pb2.InstallSnapshotReply(**reply)

    def GetLeader(self, request, context):
        abort_if_isolated(self.raft, context)
        lid, addr = self.raft.get_leader_info()
        if lid is None:
            return raft_pb2.GetLeaderReply(has_leader=False)
        if addr is None and lid == self.raft.node_id:
            # This node is the leader - find own address from config
            for nc in self.node_configs:
                if nc["id"] == lid:
                    addr = f"{nc['host']}:{nc['port']}"
                    break
        return raft_pb2.GetLeaderReply(
            leader_id=lid, leader_address=addr or "", has_leader=True)

    # Admin RPCs - deliberately NOT blocked while isolated: they model an
    # out-of-band management channel, so a dashboard can still watch (and heal)
    # a node that the rest of the cluster cannot reach.
    def GetStatus(self, request, context):
        s = self.raft.status()
        return raft_pb2.GetStatusReply(
            node_id=s["node_id"], state=s["state"], term=s["term"],
            has_leader=s["leader_id"] is not None,
            leader_id=s["leader_id"] if s["leader_id"] is not None else 0,
            commit_index=s["commit_index"], last_applied=s["last_applied"],
            last_log_index=s["last_log_index"], last_log_term=s["last_log_term"],
            isolated=s["isolated"], match_index=s["match_index"])

    def SetIsolated(self, request, context):
        self.raft.set_isolated(request.isolated, request.heal_after_seconds)
        return raft_pb2.SetIsolatedReply(isolated=self.raft.isolated)


# Bootstrap helpers
def _build_peer_stubs(node_id: int, node_configs: list) -> dict:
    """Create gRPC stubs to every *other* Raft node."""
    stubs = {}
    for nc in node_configs:
        if nc["id"] == node_id:
            continue
        addr = f"{nc['host']}:{nc['port']}"
        channel = grpc.insecure_channel(addr)
        stubs[nc["id"]] = raft_pb2_grpc.RaftServiceStub(channel)
    return stubs


def serve(node_id: int, node_configs: list):
    my_cfg = next(nc for nc in node_configs if nc["id"] == node_id)
    port = my_cfg["port"]
    base_dir = my_cfg.get("data_dir", f"./server_data_node{node_id}")
    raft_dir = my_cfg.get("raft_dir", f"./raft_state_node{node_id}")

    peers = {nc["id"]: f"{nc['host']}:{nc['port']}"
             for nc in node_configs if nc["id"] != node_id}

    logger.info(f"Starting node {node_id} on port {port}")
    logger.info(f"  data_dir: {base_dir}")
    logger.info(f"  raft_dir: {raft_dir}")
    logger.info(f"  peers: {peers}")

    # Create RaftNode (loads persisted state if any)
    raft = RaftNode(node_id=node_id, peers=peers, persist_dir=raft_dir)

    # Wire gRPC peer stubs after a short delay (peers may not be up yet)
    def _wire_stubs():
        time.sleep(1.0)
        raft.peer_stubs = _build_peer_stubs(node_id, node_configs)
        logger.info(f"[{node_id}] Peer stubs ready: {list(raft.peer_stubs)}")

    threading.Thread(target=_wire_stubs, daemon=True).start()

    # Build servicers
    dsfs_servicer = RaftDSFSServicer(raft, base_dir)
    raft_servicer = RaftGRPCServicer(raft, node_configs)

    # Start gRPC server with both services
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=20))
    fs_pb2_grpc.add_FileSystemServicer_to_server(dsfs_servicer, server)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(raft_servicer, server)
    server.add_insecure_port(f"[::]:{port}")

    server.start()
    logger.info(f"[node {node_id}] gRPC server listening on port {port}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"[node {node_id}] Shutting down…")
        server.stop(grace=3)

# Entry point
def main():
    parser = argparse.ArgumentParser(description="Raft DSFS File Server")
    parser.add_argument("--node_id", type=int, required=True,
                        help="Unique ID for this node (0, 1, 2, …)")
    parser.add_argument("--config", type=str, default="nodes_config.json",help="Path to cluster config JSON (default: nodes_config.json)")
    args = parser.parse_args()

    if not os.path.exists(args.config):
        print(f"ERROR: Config file '{args.config}' not found.", file=sys.stderr)
        sys.exit(1)

    with open(args.config) as f:
        cfg = json.load(f)

    node_configs = cfg["nodes"]
    ids = [nc["id"] for nc in node_configs]
    if args.node_id not in ids:
        print(f"ERROR: node_id {args.node_id} not in config {ids}",
              file=sys.stderr)
        sys.exit(1)

    serve(args.node_id, node_configs)

if __name__ == "__main__":
    main()
