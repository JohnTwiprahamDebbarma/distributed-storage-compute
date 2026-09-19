#!/usr/bin/env bash
# Start 3 Raft nodes locally (same machine, different ports)
#
# Each node's stdout/stderr goes to logs/nodeN.log
# PIDs are saved to .cluster_pids for stop_cluster.sh

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p logs

echo "=== Starting Raft DSFS cluster (3 nodes) ==="

start_node() {
    local id=$1
    echo "  Starting node $id (port $((50051 + id)))..."
    python raft_server.py --node_id "$id" --config nodes_config.json \
        > "logs/node${id}.log" 2>&1 &
    echo $! >> .cluster_pids
}

# Clear previous PIDs
> .cluster_pids

start_node 0
start_node 1
start_node 2

echo ""
echo "Cluster started!  Node PIDs saved to .cluster_pids"
echo "  Node 0 -> localhost:50051   (logs/node0.log)"
echo "  Node 1 -> localhost:50052   (logs/node1.log)"
echo "  Node 2 -> localhost:50053   (logs/node2.log)"
echo ""
echo "Check the node0 log: tail -f logs/node0.log . Wait ~4 seconds for leader election, then run the client. To run client test file: python raft_client_test.py ."
echo "To stop:  ./stop_cluster.sh"
