#!/usr/bin/env python3
"""
Distributed SGD coordinator (parameter server) for logistic regression.

Owns the global model. Each epoch it collects one gradient per worker (each
computed over that worker's data shards at the current weights), aggregates them
into a single average gradient, takes a gradient-descent step, and CHECKPOINTS the
model into the Raft KV store (Part 3) so training resumes after a crash -- the
etcd-style coordination the storage layer was built for.

    python ml_coordinator.py --num-workers 4 --epochs 40
    python ml_coordinator.py --num-workers 4 --epochs 40 \
        --kv-nodes localhost:51051,localhost:51052,localhost:51053     # durable checkpoints
    python ml_coordinator.py --restore --kv-nodes ...                  # resume after a crash
"""

import argparse
import glob
import json
import os
import sys
import threading
import time
from concurrent import futures
from pathlib import Path

import grpc

# The KV client lives in ../part2_3_replication; append (don't insert) so nothing
# here shadows that package. Optional: without it, the coordinator falls back to a local file.
_KV_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "part2_3_replication"))
if _KV_DIR not in sys.path:
    sys.path.append(_KV_DIR)
try:
    from kv_client_stub import RaftKVClient
except Exception:
    RaftKVClient = None

PORT = 50052
DATA_DIR = Path(__file__).resolve().parent / "ml_data"
MODEL_FILE = Path(__file__).resolve().parent / "model.json"
METHOD_SERVICE = "mlcoord.MLCoordinator"


def _ser(obj):
    return json.dumps(obj).encode("utf-8")


def _deser(data):
    return json.loads(data.decode("utf-8")) if data else {}


class MLCoordinator:
    def __init__(self, num_workers=4, learning_rate=0.3, max_epochs=40, kv_nodes=None):
        self.num_workers = num_workers
        self.lr = learning_rate
        self.max_epochs = max_epochs
        self.lock = threading.Lock()

        self.shards = sorted(os.path.basename(p)
                             for p in glob.glob(str(DATA_DIR / "shard_*.csv")))
        self.dim = self._infer_dim()
        self.weights = [0.0] * self.dim
        self.epoch = 0

        self.slots = {}            # worker_id -> slot index (registration order)
        self.assignments = {}      # worker_id -> [shard filenames]
        self.epoch_contrib = {}    # worker_id -> (gradient, sample_count) this epoch
        self.terminate = False

        self.kv = None
        self.kv_key = "ml/model"
        if kv_nodes and RaftKVClient is not None:
            try:
                self.kv = RaftKVClient(kv_nodes)
                print(f"MLCoordinator: checkpointing model to Raft KV store ({kv_nodes})")
            except Exception as e:
                print(f"MLCoordinator: KV store unavailable ({e}); local checkpoints only")
                self.kv = None
        elif kv_nodes and RaftKVClient is None:
            print("MLCoordinator: kv_client_stub not importable (run "
                  "part2_3_replication/setup_raft.sh); local checkpoints only")

    def _infer_dim(self):
        try:
            with open(DATA_DIR / self.shards[0]) as f:
                cols = f.readline().strip().split(",")
            return (len(cols) - 1) + 1     # features (drop label) + bias
        except Exception:
            return 1

    def _contributors(self):
        return [w for w, sh in self.assignments.items() if sh]

    # Heartbeat RPC (generic JSON unary-unary)
    def handle_heartbeat(self, req):
        wid = req.get("worker_id")
        with self.lock:
            if self.terminate:
                return self._response(wid)

            if wid not in self.slots:
                slot = len(self.slots)
                self.slots[wid] = slot
                self.assignments[wid] = self.shards[slot::self.num_workers] if self.shards else []
                print(f"registered worker {wid} (slot {slot}) -> shards {self.assignments[wid]}")

            if req.get("has_gradient") and req.get("epoch") == self.epoch:
                self.epoch_contrib.setdefault(
                    wid, (req.get("gradient", []), int(req.get("sample_count", 0))))

            self._maybe_step()
            return self._response(wid)

    def _response(self, wid):
        return {"shards": self.assignments.get(wid, []),
                "epoch": self.epoch, "weights": self.weights,
                "terminate": self.terminate}

    def _maybe_step(self):
        # Bulk-synchronous: step only once every worker is registered and every
        # shard-owning worker has reported a gradient for the current epoch.
        if len(self.slots) < self.num_workers:
            return
        contributors = self._contributors()
        if not contributors or not all(w in self.epoch_contrib for w in contributors):
            return

        total = [0.0] * self.dim
        count = 0
        for w in contributors:
            grad, n = self.epoch_contrib[w]
            if len(grad) == self.dim:
                for j in range(self.dim):
                    total[j] += grad[j]
                count += n
        if count > 0:
            self.weights = [self.weights[j] - self.lr * (total[j] / count)
                            for j in range(self.dim)]

        self.epoch += 1
        self.epoch_contrib = {}
        self._checkpoint()

        if self.epoch % 10 == 0 or self.epoch >= self.max_epochs:
            print(f"epoch {self.epoch}/{self.max_epochs}  "
                  f"weights={[round(w, 3) for w in self.weights]}")

        if self.epoch >= self.max_epochs:
            self.terminate = True
            self._write_model()
            print("training complete")

    # Durable model checkpoint (Raft KV store)
    def _checkpoint(self):
        if self.kv is None:
            return
        try:
            self.kv.put(self.kv_key,
                        json.dumps({"weights": self.weights, "epoch": self.epoch}).encode("utf-8"))
        except Exception as e:
            print("checkpoint to KV failed:", e)

    def restore(self):
        if self.kv is None:
            print("MLCoordinator: no KV store configured; starting fresh")
            return
        try:
            raw = self.kv.get(self.kv_key)
        except Exception as e:
            print("restore from KV failed:", e)
            return
        if raw:
            d = json.loads(raw.decode("utf-8"))
            with self.lock:
                self.weights = d.get("weights", self.weights)
                self.epoch = d.get("epoch", 0)
            print(f"MLCoordinator: restored model from KV store (resuming at epoch {self.epoch})")

    def _write_model(self):
        try:
            with open(MODEL_FILE, "w") as f:
                json.dump({"weights": self.weights, "epoch": self.epoch, "dim": self.dim}, f)
        except OSError as e:
            print("write model file failed:", e)


def serve():
    ap = argparse.ArgumentParser(description="Distributed SGD coordinator (parameter server)")
    ap.add_argument("--num-workers", type=int, default=4)
    ap.add_argument("--epochs", type=int, default=40)
    ap.add_argument("--lr", type=float, default=0.3)
    ap.add_argument("--kv-nodes", type=str, default=None,
                    help="Comma-separated Raft KV store addresses for durable model checkpoints")
    ap.add_argument("--restore", action="store_true",
                    help="Resume from the model checkpointed in the KV store")
    args = ap.parse_args()

    kv_nodes = None
    if args.kv_nodes:
        kv_nodes = []
        for hp in args.kv_nodes.split(","):
            hp = hp.strip()
            if hp:
                host, port = hp.rsplit(":", 1)
                kv_nodes.append((host, int(port)))

    coord = MLCoordinator(num_workers=args.num_workers, learning_rate=args.lr,
                          max_epochs=args.epochs, kv_nodes=kv_nodes)
    if args.restore:
        coord.restore()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    handler = grpc.unary_unary_rpc_method_handler(
        lambda req, ctx: _ser(coord.handle_heartbeat(req)),
        request_deserializer=_deser, response_serializer=lambda x: x)
    server.add_generic_rpc_handlers(
        (grpc.method_handlers_generic_handler(METHOD_SERVICE, {"Heartbeat": handler}),))
    server.add_insecure_port(f"[::]:{PORT}")
    server.start()
    print(f"MLCoordinator on :{PORT} | shards={coord.shards} dim={coord.dim} "
          f"workers={args.num_workers} epochs={args.epochs} lr={args.lr}")

    try:
        while not coord.terminate:
            time.sleep(0.3)
        time.sleep(1.5)   # let workers receive the terminate signal
        print("MLCoordinator exiting")
    except KeyboardInterrupt:
        pass
    server.stop(0)


if __name__ == "__main__":
    serve()
