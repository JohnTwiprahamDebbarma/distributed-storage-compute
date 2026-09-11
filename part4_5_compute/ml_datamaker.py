#!/usr/bin/env python3
"""
Generate a synthetic binary-classification dataset for distributed logistic-regression
training, sharded across files (one shard per worker partition).

Each shard line is `x1,x2,...,xd,label`. Labels come from a fixed "true" separating
hyperplane with a little label noise, so the trained model should recover a high
accuracy -- which is what test_ml_training.py asserts.
"""

import argparse
import json
import math
import random
from pathlib import Path

DATA_DIR_DEFAULT = "ml_data"


def make_dataset(num_samples=4000, num_features=4, num_shards=4,
                 out_dir=DATA_DIR_DEFAULT, noise=0.03, seed=42):
    rng = random.Random(seed)
    true_w = [rng.uniform(-2.0, 2.0) for _ in range(num_features)]
    true_b = rng.uniform(-1.0, 1.0)

    base = Path(__file__).resolve().parent / out_dir
    base.mkdir(parents=True, exist_ok=True)
    for p in base.glob("shard_*.csv"):
        try:
            p.unlink()
        except OSError:
            pass

    shard_files = [open(base / f"shard_{i}.csv", "w") for i in range(num_shards)]
    try:
        for i in range(num_samples):
            x = [rng.gauss(0.0, 1.0) for _ in range(num_features)]
            z = sum(w * xi for w, xi in zip(true_w, x)) + true_b
            y = 1 if z > 0.0 else 0
            if rng.random() < noise:          # flip a few labels -> not perfectly separable
                y = 1 - y
            row = ",".join(f"{v:.6f}" for v in x) + f",{y}\n"
            shard_files[i % num_shards].write(row)
    finally:
        for f in shard_files:
            f.close()

    with open(base / "meta.json", "w") as f:
        json.dump({"num_features": num_features, "num_shards": num_shards,
                   "num_samples": num_samples, "true_w": true_w, "true_b": true_b}, f)

    print(f"Created {num_shards} shards, {num_samples} samples, {num_features} features "
          f"in {base}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Synthetic logistic-regression dataset (sharded)")
    ap.add_argument("--samples", type=int, default=4000)
    ap.add_argument("--features", type=int, default=4)
    ap.add_argument("--shards", type=int, default=4)
    ap.add_argument("--out-dir", default=DATA_DIR_DEFAULT)
    ap.add_argument("--noise", type=float, default=0.03)
    args = ap.parse_args()
    make_dataset(args.samples, args.features, args.shards, args.out_dir, args.noise)
