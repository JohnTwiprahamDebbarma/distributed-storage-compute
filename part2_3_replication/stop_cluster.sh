#!/usr/bin/env bash
# Kill all nodes started by start_cluster.sh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f .cluster_pids ]; then
    echo "No .cluster_pids file found. Nothing to stop."
    exit 0
fi

echo "=== Stopping Raft DSFS cluster ==="
while read -r pid; do
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        echo "  Killed PID $pid"
    else
        echo "  PID $pid already dead"
    fi
done < .cluster_pids

rm -f .cluster_pids
echo "Done."
