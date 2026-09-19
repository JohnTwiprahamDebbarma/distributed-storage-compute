#!/bin/bash
# Run this ONCE from inside fs/replication/ to generate the gRPC Python stubs.
# Requires: pip install grpcio-tools
set -e
cd "$(dirname "$0")"
./venv/bin/python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. replication.proto
echo "Generated replication_pb2.py and replication_pb2_grpc.py"
