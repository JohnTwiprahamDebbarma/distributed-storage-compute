PYTHON ?= python3
WORKERS ?= 3
SHARDS  ?= 4
EPOCHS  ?= 40

.PHONY: all data run-coordinator run-worker train clean help

all: help

data:
	$(PYTHON) ml_datamaker.py --shards $(SHARDS) --samples 4000 --features 4

run-coordinator:
	$(PYTHON) ml_coordinator.py --num-workers $(WORKERS) --epochs $(EPOCHS)

# Durable model checkpoints in the Raft KV store (start it first: see part2_3_replication):
#   make run-coordinator-kv WORKERS=3
run-coordinator-kv:
	$(PYTHON) ml_coordinator.py --num-workers $(WORKERS) --epochs $(EPOCHS) \
		--kv-nodes localhost:51051,localhost:51052,localhost:51053

run-worker:
	@echo "Starting $(WORKERS) worker(s)..."
	@i=1; \
	while [ $$i -le $(WORKERS) ]; do \
		$(PYTHON) ml_worker.py $$i & \
		i=$$((i+1)); \
	done; \
	wait

train:
	$(PYTHON) test_ml_training.py

clean:
	rm -rf __pycache__ ml_data model.json

help:
	@echo "Makefile targets:"
	@echo "  make data              Generate a synthetic sharded dataset (SHARDS=N)"
	@echo "  make run-coordinator   Start the parameter-server coordinator (WORKERS=N EPOCHS=E)"
	@echo "  make run-coordinator-kv  ... with durable model checkpoints in the Raft KV store"
	@echo "  make run-worker        Start N SGD workers (WORKERS=N)"
	@echo "  make train             Run the end-to-end training test (starts everything, asserts convergence)"
	@echo "  make clean             Remove generated data/model/pycache"
