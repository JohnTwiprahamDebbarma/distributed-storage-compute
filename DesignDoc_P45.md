# Distributed ML Training: Design Document (Parts 4-5, Compute)

## 1) Introduction

In this part I built the compute layer that runs on top of my storage and consensus
layers: a **distributed, data-parallel trainer** for logistic regression. It follows the
classic **parameter-server / bulk-synchronous** pattern: a coordinator owns the global
model, and workers compute gradients over their data shards in parallel each epoch. I
checkpoint the model into my Raft key-value store (Part 3) so training survives a
coordinator crash. The code lives in `part4_5_compute/`.

## 2) Components

1. **Coordinator (`ml_coordinator.py`)**: the parameter server. I keep the global model
   weights, assign shards to workers, aggregate one gradient per worker each epoch into a
   single averaged gradient, take a gradient-descent step, and checkpoint the model.
2. **Workers (`ml_worker.py`)**: each worker owns a partition of the data shards. Every
   epoch it computes the logistic-regression gradient over its shards at the coordinator's
   current weights and reports it in a heartbeat.
3. **Raft KV store (Part 3)**: where I durably, replicably store the model checkpoint
   (`ml/model`), instead of a local file that a single crash could lose.

## 3) Data and model

- I generate a synthetic dataset (`ml_datamaker.py`) from a fixed "true" separating
  hyperplane with a little label noise, sharded into `shard_*.csv` files.
- The model is logistic regression: weights of dimension `d + 1` (features plus a bias
  term I append as a constant `1` feature).

## 4) Heartbeat protocol

I reuse the same generic JSON-over-gRPC heartbeat style as the rest of the project. Each
worker sends:

- `worker_id`, `has_gradient`, `epoch` (the epoch the gradient was computed for),
  `gradient`, `sample_count`.

The coordinator replies with:

- `shards` (this worker's assigned shard files), `epoch` (current global epoch),
  `weights` (current model), `terminate`.

## 5) Training loop (bulk-synchronous)

1. On a worker's first heartbeat I register it and assign it shards
   (`shards[slot :: num_workers]`, so every shard is owned by exactly one worker).
2. A worker computes its gradient **once per epoch**, at the weights the coordinator
   advertised for that epoch, and reports it.
3. When every shard-owning worker has reported a gradient for the current epoch, I
   aggregate: `avg_grad = sum(gradients) / sum(sample_counts)`, then step
   `weights = weights - learning_rate * avg_grad`, advance the epoch, and checkpoint.
4. Workers see the new epoch's weights on their next heartbeat and recompute. Stale
   gradients (tagged with an old epoch) are ignored.
5. I stop after `max_epochs` and write the final model to `model.json`.

This is full-batch gradient descent parallelised across workers: deterministic and, because
logistic-regression loss is convex, it converges reliably.

## 6) Checkpoint and recovery

Every epoch the coordinator writes `{weights, epoch}` to the Raft KV store under
`ml/model`, in one atomic `Put` replicated to a majority. If the coordinator crashes, I
restart it with `--restore` and it resumes from the checkpointed epoch instead of
retraining from scratch. Because the checkpoint lives in the replicated KV store rather
than on one local disk, it survives the loss of any single node. That is the etcd-style
coordination my storage layer was built to provide.

## 7) Running it

```bash
cd part4_5_compute
make data                          # generate the sharded dataset
make run-coordinator WORKERS=3     # in one terminal
make run-worker WORKERS=3          # in another terminal
# durable checkpoints (start the KV store first; see part2_3_replication):
make run-coordinator-kv WORKERS=3
```

## 8) Testing

`test_ml_training.py` (run with `make train`) generates a dataset, starts the coordinator
and workers as real processes, trains to completion, then loads the final model and asserts
its accuracy on the training data clears a threshold, which exercises the whole data-parallel
loop over the network. In my runs it reaches ~0.97 accuracy. I separately verified that the
coordinator checkpoints and restores the model through the Raft KV store.

## 9) Why this design

- **The workload is pluggable.** The coordinator/worker framework (assign work, heartbeat,
  aggregate, checkpoint) is independent of *what* is computed; logistic-regression SGD is
  one job plugged into it. Distributed PageRank, k-means, or an inverted-index build would
  slot into the same engine.
- **The checkpoint is the point of the storage layer.** Putting the model in my replicated
  KV store is what makes the training fault-tolerant, and it ties the compute layer to the
  consensus layer beneath it.
