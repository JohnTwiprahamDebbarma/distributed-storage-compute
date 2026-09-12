
## 1. Run - Local (Same Machine)

### Step 1 - One-time setup

```bash
cd fs/
chmod +x setup_raft.sh start_cluster.sh stop_cluster.sh
./setup_raft.sh
```
- Please note that here `fs` means File System (Part 1). In this repository the code for Parts 2-3 lives in the `part2_3_replication/` directory.

This installs `grpcio`, `grpcio-tools`, and generates `raft_pb2.py` /
`raft_pb2_grpc.py` from `raft.proto`.

### Step 2 - Seed input files (if needed)

Assumption: Input files already exist on the server.
Copy them into all three server data directories:

```bash
cp input.txt server_data_node0/
cp input.txt server_data_node1/
cp input.txt server_data_node2/
```

*(Or just place them in `server_data_node0/`; after the first write the leader
will propagate to followers automatically.)*

### Step 3 - Start the cluster

```bash
./start_cluster.sh
```

Wait ~4 seconds for Raft to elect a leader, then check:

```bash
tail -f logs/node0.log   # look for "*** BECAME LEADER ***"
```

### Step 4 - Run the test client

```bash
python raft_client_test.py
```

### Step 5 - Stop the cluster

```bash
./stop_cluster.sh
```


## 2. Multi-Machine Deployment

### Step 1 - Edit `nodes_config.json`

Replace `localhost` with actual IP addresses:

```json
{
  "nodes": [
    { "id": 0, "host": "192.168.1.10", "port": 50051,
      "data_dir": "./server_data_node0", "raft_dir": "./raft_state_node0" },
    { "id": 1, "host": "192.168.1.11", "port": 50051,
      "data_dir": "./server_data_node0", "raft_dir": "./raft_state_node0" },
    { "id": 2, "host": "192.168.1.12", "port": 50051,
      "data_dir": "./server_data_node0", "raft_dir": "./raft_state_node0" }
  ]
}
```

Note: each machine uses node_id=0 **locally** so `data_dir` / `raft_dir`
names can be the same; what differs is the `host` (external IP) and
the `id` field.

### Step 2 - Copy files to each machine

```bash
scp -r fs/ user@192.168.1.10:~/dsfs/
scp -r fs/ user@192.168.1.11:~/dsfs/
scp -r fs/ user@192.168.1.12:~/dsfs/
```

### Step 3 - Run setup on each machine

```bash
ssh user@192.168.1.10 "cd ~/dsfs && ./setup_raft.sh"
ssh user@192.168.1.11 "cd ~/dsfs && ./setup_raft.sh"
ssh user@192.168.1.12 "cd ~/dsfs && ./setup_raft.sh"
```

### Step 4 - Start each node with its ID

```bash
# On machine 1 (192.168.1.10)
python raft_server.py --node_id 0 --config nodes_config.json

# On machine 2 (192.168.1.11)
python raft_server.py --node_id 1 --config nodes_config.json

# On machine 3 (192.168.1.12)
python raft_server.py --node_id 2 --config nodes_config.json
```

### Step 5 - Point the client at all three machines

```python
from raft_client_stub import RaftDSFSClientStub
client = RaftDSFSClientStub(nodes=[
    ("192.168.1.10", 50051),
    ("192.168.1.11", 50051),
    ("192.168.1.12", 50051),
])
```
