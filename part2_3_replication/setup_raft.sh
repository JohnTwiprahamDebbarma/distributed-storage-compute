#!/usr/bin/env bash
# Install required dependencies and generate Raft protobuf stubs
set -e

echo "=== Installing Python dependencies ==="
pip3 install grpcio grpcio-tools protobuf

echo ""
echo "=== Generating protobuf stubs for fs.proto ==="
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. fs.proto

echo ""
echo "=== Generating protobuf stubs for raft.proto ==="
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto

echo ""
echo "=== Generating protobuf stubs for kv.proto (mini-etcd KV store) ==="
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kv.proto

# echo ""

echo ""
echo "=== Creating server data directories ==="
mkdir -p server_data_node0 server_data_node1 server_data_node2
mkdir -p raft_state_node0  raft_state_node1  raft_state_node2

echo ""
echo "Setup complete!"
echo "Next: run ./start_cluster.sh to launch the 3-node Raft cluster."
