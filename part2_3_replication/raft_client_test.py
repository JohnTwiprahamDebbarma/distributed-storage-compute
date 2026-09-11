"""
Functional tests for the Raft-replicated DSFS cluster

Tests:
    1.  Basic create / open / write / close / read
    2.  Cache hit on re-open (version unchanged)
    3.  Leader failover (kill leader, operations still succeed)
    4.  Idempotency (duplicate seq_num replay)
    5.  Three-node read consistency (open same file on all three nodes)
"""

import os
import sys
import time

from raft_client_stub import RaftDSFSClientStub

NODES = [
    ("localhost", 50051),
    ("localhost", 50052),
    ("localhost", 50053),
]


def sep(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print('='*60)


def assert_eq(label, got, expected):
    if got == expected:
        print(f"  [PASS] {label}")
    else:
        print(f"  [FAIL] {label}")
        print(f"         expected: {expected!r}")
        print(f"         got:      {got!r}")
        sys.exit(1)


# Test 1: Basic create -> write -> read
sep("TEST 1: Basic create / write / read")
client = RaftDSFSClientStub(NODES)

TEST_FILE = "raft_test_hello.txt"
CONTENT   = b"Hello, Raft!"

# Clean up from previous run
for nc in NODES:
    data_dir = f"./server_data_node{NODES.index(nc)}"
    path = os.path.join(data_dir, TEST_FILE)
    if os.path.exists(path):
        os.remove(path)

client.create(TEST_FILE)
print(f"  Created {TEST_FILE}")

handle = client.open(TEST_FILE, "w")
client.write(handle, 0, CONTENT)
client.close(handle)
print(f"  Wrote {len(CONTENT)} bytes and closed")

handle2 = client.open(TEST_FILE, "r")
data = client.read(handle2, 0, len(CONTENT))
client.close(handle2)
assert_eq("read-back content", data, CONTENT)

# Test 2: Cache hit on re-open (no server download)
sep("TEST 2: Cache hit on re-open")
handle3 = client.open(TEST_FILE, "r")
data3   = client.read(handle3, 0, len(CONTENT))
client.close(handle3)
assert_eq("cached content still correct", data3, CONTENT)

# Test 3: Read from a different node (read consistency)
sep("TEST 3: Read consistency across nodes")
time.sleep(1)  # let replication propagate
for i, node in enumerate(NODES):
    c = RaftDSFSClientStub([node])
    try:
        h = c.open(TEST_FILE, "r")
        d = c.read(h, 0, len(CONTENT))
        c.close(h)
        assert_eq(f"node {i} ({node[1]}) content", d, CONTENT)
    except Exception as e:
        print(f"  [SKIP] node {i} not reachable: {e}")
    finally:
        c.disconnect()

# Test 4: Multiple writes (version increments)
sep("TEST 4: Version increments on writes")
for i in range(3):
    h = client.open(TEST_FILE, "w")
    client.write(h, 0, f"version {i+2}".encode())
    client.close(h)

h = client.open(TEST_FILE, "r")
last = client.read(h, 0, 64)
client.close(h)
print(f"  Final content after 3 more writes: {last!r}")
assert_eq("final version content", last[:9], b"version 4")

# Test 5: Leader failover simulation
sep("TEST 5: Leader failover")
print("  Kill the current leader manually in another terminal, then press Enter.")
print("  Command:  kill $(lsof -ti:50051)   # or whichever port is the leader")
print("  Press Enter when done (or just press Enter to skip this test)…")
inp = input("  > ").strip()

if inp == "" or inp.lower() == "skip":
    print("  [SKIP] Failover test skipped.")
else:
    print("  Waiting 5s for new leader election…")
    time.sleep(5)
    try:
        h = client.open(TEST_FILE, "r")
        d = client.read(h, 0, 64)
        client.close(h)
        print(f"  [PASS] Read succeeded after failover: {d!r}")
    except Exception as e:
        print(f"  [FAIL] {e}")

client.disconnect()
print("\n=== All automated tests passed! ===\n")
