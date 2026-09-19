#!/bin/bash
# Start a 3-node replica cluster in the background.
# Logs go to server_<port>.log
# Run from inside fs/replication/
set -e
cd "$(dirname "$0")"

# Clean up old data directories (we can comment this out to persist across runs)
rm -rf data_50050 data_50051 data_50052

# Populate the primary's data directory with input files if they exist
mkdir -p data_50050/server_data
if [ -d "../server_data" ]; then
    cp ../server_data/* data_50050/server_data/ 2>/dev/null || true
fi

PYTHON=./venv/bin/python

echo "Starting Server 0 (initial primary) on :50050 ..."
$PYTHON server.py \
    --addr  localhost:50050 \
    --peers localhost:50051,localhost:50052 \
    --data-dir ./data_50050 \
    > server_50050.log 2>&1 &
PID0=$!

echo "Starting Server 1 (backup) on :50051 ..."
$PYTHON server.py \
    --addr  localhost:50051 \
    --peers localhost:50050,localhost:50052 \
    --data-dir ./data_50051 \
    > server_50051.log 2>&1 &
PID1=$!

echo "Starting Server 2 (backup) on :50052 ..."
$PYTHON server.py \
    --addr  localhost:50052 \
    --peers localhost:50050,localhost:50051 \
    --data-dir ./data_50052 \
    > server_50052.log 2>&1 &
PID2=$!

echo "Cluster started.  PIDs: $PID0  $PID1  $PID2"
echo "Logs: server_50050.log  server_50051.log  server_50052.log"
echo ""
echo "To stop: kill $PID0 $PID1 $PID2"
echo "PIDs saved to cluster.pids"
echo "$PID0 $PID1 $PID2" > cluster.pids
