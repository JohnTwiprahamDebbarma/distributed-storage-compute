import argparse
import time
import sys
import os

from client_stub import ReplicatedDSFSClientStub

SEPARATOR = "-" * 60
PASS = "[ PASS ]"
FAIL = "[ FAIL ]"


def check(label: str, condition: bool, detail: str = ""):
    status = PASS if condition else FAIL
    msg = f"{status} {label}"
    if detail:
        msg += f"  ({detail})"
    print(msg)
    if not condition:
        sys.exit(1)


def run_tests(servers: list):
    print(SEPARATOR)
    print("Replicated DSFS Client Tests")
    print(f"Servers: {servers}")
    print(SEPARATOR)

    client = ReplicatedDSFSClientStub(server_addresses=servers, cache_dir="./repl_cache")

    # Test 1 - Create a file on the primary
    print("\n[Test 1] Create file")
    try:
        fh = client.create("test_repl.txt")
        check("Create returned handle 0", fh == 0)
    except Exception as e:
        check("Create succeeded", False, str(e))

    # Test 2 - Open for write and close (flush to server + replicas)
    print("\n[Test 2] Open-write-close")
    try:
        fh = client.open("test_repl.txt", mode="w")
        check("Open-write handle > 0", fh > 0, f"handle={fh}")
        client.write(fh, 0, "Hello, replicated world!\n")
        client.close(fh)
        check("Write-close succeeded", True)
    except Exception as e:
        check("Write-close succeeded", False, str(e))

    # Test 3 - Open for read and verify data
    print("\n[Test 3] Open-read-verify")
    try:
        fh   = client.open("test_repl.txt", mode="r")
        data = client.read(fh, 0, 100)
        client.close(fh)
        check("Data matches", data == b"Hello, replicated world!\n",
              f"got: {data!r}")
    except Exception as e:
        check("Read succeeded", False, str(e))

    # Test 4 - Cache hit (second open should not fetch)
    print("\n[Test 4] Cache hit")
    try:
        fh   = client.open("test_repl.txt", mode="r")
        data = client.read(fh, 0, 100)
        client.close(fh)
        check("Cache hit data correct", data == b"Hello, replicated world!\n")
    except Exception as e:
        check("Cache hit", False, str(e))

    # Test 5 - Version increment after write
    print("\n[Test 5] Version increments")
    try:
        v_before = client.test_version_number("test_repl.txt")
        fh       = client.open("test_repl.txt", mode="w")
        client.write(fh, 0, "Updated content\n")
        client.close(fh)
        v_after  = client.test_version_number("test_repl.txt")
        check("Version incremented", v_after == v_before + 1,
              f"{v_before} -> {v_after}")
    except Exception as e:
        check("Version increment", False, str(e))

    # Test 6 - Cache miss after version change
    print("\n[Test 6] Cache miss after remote write")
    try:
        # A second client simulates a different process
        client2 = ReplicatedDSFSClientStub(servers, cache_dir="./repl_cache2")
        fh      = client2.open("test_repl.txt", mode="w")
        client2.write(fh, 0, "Written by client2\n")
        client2.close(fh)
        client2.disconnect()

        # Original client should detect version change and re-fetch
        fh   = client.open("test_repl.txt", mode="r")
        data = client.read(fh, 0, 100)
        client.close(fh)
        check("Stale cache refreshed", data == b"Written by client2\n",
              f"got: {data!r}")
    except Exception as e:
        check("Cache miss after remote write", False, str(e))

    # Test 7 - Replica failover (manual step)
    print(f"\n[Test 7] Fault tolerance - primary failover")
    print("  ACTION REQUIRED: Kill the PRIMARY server process, then press Enter.")
    print(f"  Primary is currently: {client._primary_addr}")
    input("  Press Enter to continue ... ")

    try:
        # Rediscover primary
        client._primary_addr = client._discover_primary()
        print(f"  New primary discovered: {client._primary_addr}")

        fh = client.open("test_repl.txt", mode="r")
        data = client.read(fh, 0, 100)
        client.close(fh)
        check("Read after failover", b"client2" in data or b"Updated" in data,
              f"got: {data!r}")

        # Write to new primary
        fh = client.open("test_repl.txt", mode="w")
        client.write(fh, 0, "After failover\n")
        client.close(fh)
        check("Write after failover", True)
    except Exception as e:
        check("Failover read/write", False, str(e))

    # Test 8 - Recovered server catches up (manual step)
    print(f"\n[Test 8] Recovery - restart the killed server, then press Enter.")
    input("  Press Enter after restarting ... ")
    time.sleep(8)  # Give it time to sync

    try:
        # The recovered server should now have the latest file
        # Connect directly to the recovered server to verify
        old_primary_idx = servers.index(
            [s for s in servers if s != client._primary_addr][0]
        ) if client._primary_addr in servers else 0
        recovered_addr = [s for s in servers if s != client._primary_addr][0]

        import grpc
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        import fs_pb2, fs_pb2_grpc
        ch   = grpc.insecure_channel(recovered_addr)
        stub = fs_pb2_grpc.FileSystemStub(ch)

        req  = fs_pb2.OpenRequest(filename="test_repl.txt", mode="r", fetch_data=True)
        resp = stub.Open(req, timeout=3.0)
        check("Recovered server has latest data",
              resp.success and b"After failover" in resp.file_data,
              f"data: {resp.file_data!r}")
        ch.close()
    except Exception as e:
        check("Recovery sync", False, str(e))

    # Done
    print(f"\n{SEPARATOR}")
    print("All tests passed.")
    print(SEPARATOR)
    client.disconnect()


def main():
    parser = argparse.ArgumentParser(description="Replicated DSFS test client")
    parser.add_argument(
        "--servers",
        default="localhost:50050,localhost:50051,localhost:50052",
        help="Comma-separated list of server addresses",
    )
    args    = parser.parse_args()
    servers = [s.strip() for s in args.servers.split(",")]
    run_tests(servers)


if __name__ == "__main__":
    main()
