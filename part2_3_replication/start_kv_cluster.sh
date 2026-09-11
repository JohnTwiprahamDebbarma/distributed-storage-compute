#!/usr/bin/env bash
# Start 3 Raft KV-store nodes locally (mini-etcd), same machine, different ports.
# Each node's stdout/stderr goes to logs/kv_nodeN.log; PIDs -> .kv_cluster_pids
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Optional first argument: cluster config file (default nodes_config.json on
# ports 5005x). Use nodes_config_kv.json (ports 5105x) to run alongside the
# Part 4/5 compute app: ./start_kv_cluster.sh nodes_config_kv.json
CONFIG="${1:-nodes_config.json}"

mkdir -p logs
> .kv_cluster_pids

echo "=== Starting Raft KV cluster (mini-etcd, 3 nodes) from $CONFIG ==="

start_node() {
    local id=$1
    echo "  node $id   (config $CONFIG, logs/kv_node${id}.log)"
    python raft_kv_server.py --node_id "$id" --config "$CONFIG" \
        > "logs/kv_node${id}.log" 2>&1 &
    echo $! >> .kv_cluster_pids
}

start_node 0
start_node 1
start_node 2

echo ""
echo "Cluster started. Wait ~4s for a leader (grep 'BECAME LEADER' logs/kv_node0.log),"
echo "then run:  python test_kv_cluster.py"
echo "Stop with: kill \$(cat .kv_cluster_pids)"
