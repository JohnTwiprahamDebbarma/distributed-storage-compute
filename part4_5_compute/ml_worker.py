#!/usr/bin/env python3
"""
Distributed SGD worker.

Each worker owns a partition of the data shards. Every epoch it computes the
logistic-regression gradient over its shards at the coordinator's current model
weights and reports it in a heartbeat; the coordinator aggregates the gradients
from all workers into one global step. This is the classic data-parallel /
parameter-server pattern (bulk-synchronous).

    python ml_worker.py <worker_id>
"""

import grpc
import json
import math
import os
import sys
import time

COORDINATOR_ADDR = "localhost:50052"
HEARTBEAT_RATE = 0.2                       # seconds
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ml_data")
METHOD = "/mlcoord.MLCoordinator/Heartbeat"


def _ser(obj):
    return json.dumps(obj).encode("utf-8")


def _deser(data):
    return json.loads(data.decode("utf-8")) if data else {}


def sigmoid(z):
    if z < -700:
        return 0.0
    if z > 700:
        return 1.0
    return 1.0 / (1.0 + math.exp(-z))


class Worker:
    def __init__(self, worker_id):
        self.worker_id = str(worker_id)
        self.shards = []
        self.data = []            # list of (augmented_x, y)
        self.loaded = set()
        self.pending = None       # (epoch, gradient, sample_count) awaiting report
        self.channel = grpc.insecure_channel(COORDINATOR_ADDR)
        self.rpc = self.channel.unary_unary(METHOD, request_serializer=_ser,
                                             response_deserializer=_deser)

    def _load_shards(self):
        for s in self.shards:
            if s in self.loaded:
                continue
            path = os.path.join(DATA_DIR, s)
            try:
                with open(path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split(",")
                        x = [float(v) for v in parts[:-1]] + [1.0]   # augment bias term
                        y = int(parts[-1])
                        self.data.append((x, y))
                self.loaded.add(s)
            except OSError as e:
                print(f"worker {self.worker_id}: failed to load {s}: {e}")

    def _gradient(self, weights):
        dim = len(weights)
        grad = [0.0] * dim
        for x, y in self.data:
            z = sum(w * xi for w, xi in zip(weights, x))
            err = sigmoid(z) - y
            for j in range(dim):
                grad[j] += err * x[j]
        return grad, len(self.data)

    def run(self):
        print(f"worker {self.worker_id} starting")
        known_epoch = -1
        while True:
            req = {"worker_id": self.worker_id, "has_gradient": False,
                   "epoch": -1, "gradient": [], "sample_count": 0}
            if self.pending is not None:
                pe, pg, pc = self.pending
                req.update({"has_gradient": True, "epoch": pe,
                            "gradient": pg, "sample_count": pc})
            try:
                resp = self.rpc(req, timeout=10)
            except grpc.RpcError:
                time.sleep(HEARTBEAT_RATE)
                continue

            if resp.get("terminate"):
                print(f"worker {self.worker_id}: training complete, exiting")
                return

            new_shards = resp.get("shards", [])
            if new_shards and new_shards != self.shards:
                self.shards = new_shards
                self._load_shards()
                print(f"worker {self.worker_id}: assigned {self.shards} "
                      f"({len(self.data)} samples)")

            g_epoch = resp.get("epoch", 0)
            weights = resp.get("weights")
            # Compute this worker's gradient once per epoch, at the epoch's weights.
            if weights and self.shards and g_epoch != known_epoch:
                grad, n = self._gradient(weights)
                self.pending = (g_epoch, grad, n)
                known_epoch = g_epoch

            time.sleep(HEARTBEAT_RATE)


if __name__ == "__main__":
    wid = sys.argv[1] if len(sys.argv) > 1 else "1"
    Worker(wid).run()
