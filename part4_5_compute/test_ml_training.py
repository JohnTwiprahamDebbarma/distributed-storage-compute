#!/usr/bin/env python3
"""
End-to-end test for the distributed SGD trainer.

Generates a synthetic dataset, starts the coordinator and N workers as real
processes, lets them train to completion, then loads the final model and asserts
it classifies the training data well above chance. This exercises the whole
data-parallel loop over the network -- registration, per-epoch gradient
aggregation, weight updates, and termination.

    python test_ml_training.py
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE / "ml_data"
MODEL_FILE = HERE / "model.json"
PY = sys.executable

NUM_WORKERS = 3
NUM_SHARDS = 4
EPOCHS = 40
ACC_THRESHOLD = 0.85


def load_all_data():
    data = []
    for shard in sorted(DATA_DIR.glob("shard_*.csv")):
        with open(shard) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(",")
                x = [float(v) for v in parts[:-1]] + [1.0]
                data.append((x, int(parts[-1])))
    return data


def accuracy(weights, data):
    correct = 0
    for x, y in data:
        z = sum(w * xi for w, xi in zip(weights, x))
        correct += (1 if z > 0 else 0) == y
    return correct / len(data)


def main():
    # 1. fresh dataset
    sys.path.insert(0, str(HERE))
    from ml_datamaker import make_dataset
    make_dataset(num_samples=3000, num_features=4, num_shards=NUM_SHARDS, seed=7)
    if MODEL_FILE.exists():
        MODEL_FILE.unlink()

    procs = []
    try:
        # 2. coordinator
        procs.append(subprocess.Popen(
            [PY, "-u", str(HERE / "ml_coordinator.py"),
             "--num-workers", str(NUM_WORKERS), "--epochs", str(EPOCHS), "--lr", "0.3"],
            cwd=str(HERE)))
        time.sleep(1.5)   # let it bind the port

        # 3. workers
        for i in range(1, NUM_WORKERS + 1):
            procs.append(subprocess.Popen(
                [PY, "-u", str(HERE / "ml_worker.py"), str(i)], cwd=str(HERE)))

        # 4. wait for the coordinator to finish and write model.json
        deadline = time.time() + 120
        while time.time() < deadline:
            if MODEL_FILE.exists() and procs[0].poll() is not None:
                break
            time.sleep(0.5)
        else:
            print("[FAIL] training did not complete within the timeout")
            return 1
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    # 5. evaluate
    if not MODEL_FILE.exists():
        print("[FAIL] no model.json produced")
        return 1
    model = json.loads(MODEL_FILE.read_text())
    acc = accuracy(model["weights"], load_all_data())
    print(f"\nFinal training accuracy: {acc:.3f}  (trained for {model['epoch']} epochs "
          f"across {NUM_WORKERS} workers / {NUM_SHARDS} shards)")
    if acc >= ACC_THRESHOLD:
        print(f"[PASS] model converged (accuracy >= {ACC_THRESHOLD})")
        return 0
    print(f"[FAIL] accuracy {acc:.3f} below threshold {ACC_THRESHOLD}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
