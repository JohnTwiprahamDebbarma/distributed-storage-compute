import os
import sys
import time
import threading

# fs/ must come before fs/replication/ only for fs_pb2 imports; but
# client_stub must resolve to fs/replication/client_stub, so insert
# fs/replication/ at position 0 (highest priority).
_REPL = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_FS   = os.path.abspath(os.path.join(_REPL, ".."))
sys.path.insert(0, _FS)    # makes fs_pb2, fs_pb2_grpc importable
sys.path.insert(0, _REPL)  # overrides: client_stub, replication_pb2 come from here
from client_stub import ReplicatedDSFSClientStub
import grpc
import fs_pb2
import fs_pb2_grpc


# Helpers
PASS_LABEL = "\033[92m[ PASS ]\033[0m"
FAIL_LABEL = "\033[91m[ FAIL ]\033[0m"
SKIP_LABEL = "\033[93m[ SKIP ]\033[0m"
INFO_LABEL = "\033[94m[ INFO ]\033[0m"


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.failures = []

    def record_pass(self, name):
        self.passed += 1
        print(f"  {PASS_LABEL} {name}")

    def record_fail(self, name, detail=""):
        self.failed += 1
        msg = f"  {FAIL_LABEL} {name}"
        if detail:
            msg += f"\n           detail: {detail}"
        print(msg)
        self.failures.append((name, detail))

    def record_skip(self, name, reason=""):
        self.skipped += 1
        print(f"  {SKIP_LABEL} {name}  [{reason}]")

    def summary(self):
        total = self.passed + self.failed + self.skipped
        bar = "=" * 62
        print(f"\n{bar}")
        print(f"  Results: {total} tests | "
              f"\033[92m{self.passed} passed\033[0m | "
              f"\033[91m{self.failed} failed\033[0m | "
              f"\033[93m{self.skipped} skipped\033[0m")
        if self.failures:
            print("  Failed tests:")
            for name, detail in self.failures:
                print(f"    - {name}: {detail}")
        print(bar)
        return self.failed == 0


def fresh_client(servers, cache_subdir="cache_main"):
    """Return a new ReplicatedDSFSClientStub with an isolated cache directory."""
    cache = os.path.join("/tmp", f"afs_test_{cache_subdir}")
    os.makedirs(cache, exist_ok=True)
    return ReplicatedDSFSClientStub(server_addresses=servers, cache_dir=cache)


def read_server_file(data_dir: str, filename: str) -> bytes | None:
    """Directly read a file from a server's data directory (local only)."""
    path = os.path.join(data_dir, filename)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        return f.read()


def open_read_direct(addr: str, filename: str) -> bytes | None:
    """Read a file directly from a server via gRPC (bypasses caching)."""
    try:
        ch   = grpc.insecure_channel(addr)
        stub = fs_pb2_grpc.FileSystemStub(ch)
        resp = stub.Open(
            fs_pb2.OpenRequest(filename=filename, mode="r", fetch_data=True),
            timeout=4.0,
        )
        ch.close()
        return resp.file_data if resp.success else None
    except Exception:
        return None


def section(title: str):
    print(f"\n{'-' * 62}")
    print(f"  {title}")
    print(f"{'-' * 62}")



# ClusterController interface
class ClusterController:
    """
    Abstract interface for fault injection.
    Subclasses implement kill/restart for local vs. distributed clusters.
    """

    def server_addrs(self) -> list:
        raise NotImplementedError

    def data_dir(self, addr: str) -> str | None:
        """Return local data directory for addr, or None if not accessible."""
        return None

    def kill(self, addr: str):
        raise NotImplementedError

    def restart(self, addr: str):
        raise NotImplementedError

    def is_local(self) -> bool:
        """True if data directories are accessible for direct inspection."""
        return False

    def wait_election(self):
        """Wait long enough for a new election to complete."""
        time.sleep(9)

    def wait_sync(self):
        """Wait long enough for a recovering server to finish syncing."""
        time.sleep(8)



# Group A - Basic DSFS functionality
def run_group_a(servers, r: TestResult):
    section("Group A - Basic DSFS functionality")

    # T01 - Create a new file
    c = fresh_client(servers, "a_t01")
    try:
        fh = c.create("a_file.txt")
        if fh == 0:
            r.record_pass("T01 Create file returns handle 0")
        else:
            r.record_fail("T01 Create file returns handle 0", f"got {fh}")
    except Exception as e:
        r.record_fail("T01 Create file returns handle 0", str(e))
    finally:
        c.disconnect()

    # T02 - Duplicate create must fail
    c = fresh_client(servers, "a_t02")
    try:
        c.create("a_dup.txt")
        try:
            c.create("a_dup.txt")
            r.record_fail("T02 Duplicate create rejected", "no exception raised")
        except Exception:
            r.record_pass("T02 Duplicate create rejected")
    except Exception as e:
        r.record_fail("T02 Duplicate create rejected", f"first create failed: {e}")
    finally:
        c.disconnect()

    # T03 - Open non-existent file in read mode must fail
    c = fresh_client(servers, "a_t03")
    try:
        c.open("no_such_file_xyz.txt", mode="r")
        r.record_fail("T03 Open missing file raises error", "no exception raised")
    except Exception:
        r.record_pass("T03 Open missing file raises error")
    finally:
        c.disconnect()

    # T04 - Write then read back exact bytes
    c = fresh_client(servers, "a_t04")
    CONTENT = b"Hello, DSFS replication!\n"
    try:
        c.create("a_rw.txt")
        fh = c.open("a_rw.txt", mode="w")
        c.write(fh, 0, CONTENT)
        c.close(fh)
        fh   = c.open("a_rw.txt", mode="r")
        data = c.read(fh, 0, len(CONTENT) + 10)
        c.close(fh)
        if data == CONTENT:
            r.record_pass("T04 Write then read back exact bytes")
        else:
            r.record_fail("T04 Write then read back exact bytes",
                          f"expected {CONTENT!r}, got {data!r}")
    except Exception as e:
        r.record_fail("T04 Write then read back exact bytes", str(e))
    finally:
        c.disconnect()

    # T05 - Version increments on every write
    c = fresh_client(servers, "a_t05")
    try:
        c.create("a_ver.txt")
        fh = c.open("a_ver.txt", mode="w")
        c.write(fh, 0, b"v1\n")
        c.close(fh)
        v1 = c.test_version_number("a_ver.txt")
        fh = c.open("a_ver.txt", mode="w")
        c.write(fh, 0, b"v2\n")
        c.close(fh)
        v2 = c.test_version_number("a_ver.txt")
        fh = c.open("a_ver.txt", mode="w")
        c.write(fh, 0, b"v3\n")
        c.close(fh)
        v3 = c.test_version_number("a_ver.txt")
        if v2 == v1 + 1 and v3 == v2 + 1:
            r.record_pass(f"T05 Version increments ({v1}->{v2}->{v3})")
        else:
            r.record_fail("T05 Version increments", f"got {v1},{v2},{v3}")
    except Exception as e:
        r.record_fail("T05 Version increments", str(e))
    finally:
        c.disconnect()

    # T06 - Multiple independent files
    c = fresh_client(servers, "a_t06")
    try:
        for i in range(5):
            c.create(f"a_multi_{i}.txt")
            fh = c.open(f"a_multi_{i}.txt", mode="w")
            c.write(fh, 0, f"file{i}\n".encode())
            c.close(fh)
        ok = True
        for i in range(5):
            fh   = c.open(f"a_multi_{i}.txt", mode="r")
            data = c.read(fh, 0, 20)
            c.close(fh)
            if data != f"file{i}\n".encode():
                ok = False
                break
        if ok:
            r.record_pass("T06 Multiple independent files")
        else:
            r.record_fail("T06 Multiple independent files", "content mismatch")
    except Exception as e:
        r.record_fail("T06 Multiple independent files", str(e))
    finally:
        c.disconnect()

    # T07 - Large file (1 MB)
    c = fresh_client(servers, "a_t07")
    LARGE = b"X" * (1024 * 1024)
    try:
        c.create("a_large.txt")
        fh = c.open("a_large.txt", mode="w")
        c.write(fh, 0, LARGE)
        c.close(fh)
        fh= c.open("a_large.txt", mode="r")
        data = c.read(fh, 0, len(LARGE) + 100)
        c.close(fh)
        if data == LARGE:
            r.record_pass("T07 Large file (1 MB) round-trip")
        else:
            r.record_fail("T07 Large file (1 MB) round-trip",
                          f"size mismatch: expected {len(LARGE)}, got {len(data)}")
    except Exception as e:
        r.record_fail("T07 Large file (1 MB) round-trip", str(e))
    finally:
        c.disconnect()

    # T08 - Write empty content
    c = fresh_client(servers, "a_t08")
    try:
        c.create("a_empty.txt")
        fh = c.open("a_empty.txt", mode="w")
        c.write(fh, 0, b"")
        c.close(fh)
        fh   = c.open("a_empty.txt", mode="r")
        data = c.read(fh, 0, 10)
        c.close(fh)
        if data == b"":
            r.record_pass("T08 Write empty content")
        else:
            r.record_fail("T08 Write empty content", f"expected b'', got {data!r}")
    except Exception as e:
        r.record_fail("T08 Write empty content", str(e))
    finally:
        c.disconnect()



# Group B - Client-side caching
def run_group_b(servers, r: TestResult):
    section("Group B - Client-side caching")

    # T09 - Cache hit: second open must not send a new Open+data RPC
    # Verified by checking that fetch_data was False (the printed output contains CACHE HIT)
    c = fresh_client(servers, "b_t09")
    try:
        c.create("b_cache.txt")
        fh = c.open("b_cache.txt", mode="w")
        c.write(fh, 0, b"cached\n")
        c.close(fh)

        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            fh = c.open("b_cache.txt", mode="r")
            c.read(fh, 0, 10)
            c.close(fh)
        output = buf.getvalue()
        if "CACHE HIT" in output:
            r.record_pass("T09 Cache hit on second open")
        else:
            r.record_fail("T09 Cache hit on second open",
                          "expected CACHE HIT in output")
    except Exception as e:
        r.record_fail("T09 Cache hit on second open", str(e))
    finally:
        c.disconnect()

    # T10 - Cache miss when server version advances (another client wrote)
    c1 = fresh_client(servers, "b_t10_c1")
    c2 = fresh_client(servers, "b_t10_c2")
    try:
        c1.create("b_miss.txt")
        fh = c1.open("b_miss.txt", mode="w")
        c1.write(fh, 0, b"original\n")
        c1.close(fh)

        # c1 now has a cached copy at v_old
        fh = c1.open("b_miss.txt", mode="r")
        c1.read(fh, 0, 10)
        c1.close(fh)

        # c2 writes a new version
        fh = c2.open("b_miss.txt", mode="w")
        c2.write(fh, 0, b"updated by c2\n")
        c2.close(fh)

        # c1 re-opens: should detect version mismatch and fetch fresh data
        import io, contextlib
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            fh   = c1.open("b_miss.txt", mode="r")
            data = c1.read(fh, 0, 50)
            c1.close(fh)
        output = buf.getvalue()
        if b"updated by c2" in data and "CACHE MISS" in output:
            r.record_pass("T10 Cache miss after remote write (stale version detected)")
        else:
            r.record_fail("T10 Cache miss after remote write",
                          f"data={data!r}, output={output!r}")
    except Exception as e:
        r.record_fail("T10 Cache miss after remote write", str(e))
    finally:
        c1.disconnect()
        c2.disconnect()

    # T11 - Cached file used across open calls when version unchanged
    c = fresh_client(servers, "b_t11")
    try:
        c.create("b_stable.txt")
        fh = c.open("b_stable.txt", mode="w")
        c.write(fh, 0, b"stable\n")
        c.close(fh)

        hits = 0
        import io, contextlib
        for _ in range(3):
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                fh = c.open("b_stable.txt", mode="r")
                c.read(fh, 0, 10)
                c.close(fh)
            if "CACHE HIT" in buf.getvalue():
                hits += 1
        if hits == 3:
            r.record_pass("T11 Cached file reused on repeated opens (no writes)")
        else:
            r.record_fail("T11 Cached file reused on repeated opens",
                          f"got {hits}/3 cache hits")
    except Exception as e:
        r.record_fail("T11 Cached file reused on repeated opens", str(e))
    finally:
        c.disconnect()



# Group C - Replication correctness
def run_group_c(servers, r: TestResult, ctrl: ClusterController):
    section("Group C - Replication correctness")

    CONTENT = b"Replicated data payload\n"

    # T12 - Data replicated to ALL backups right after write
    c = fresh_client(servers, "c_t12")
    try:
        c.create("c_repl.txt")
        fh = c.open("c_repl.txt", mode="w")
        c.write(fh, 0, CONTENT)
        c.close(fh)
        time.sleep(0.5)

        if ctrl.is_local():
            ok = True
            for addr in servers:
                d = ctrl.data_dir(addr)
                if d:
                    content = read_server_file(d, "c_repl.txt")
                    if content != CONTENT:
                        ok = False
                        r.record_fail("T12 Data replicated to all backups",
                                      f"{addr}: {content!r}")
                        break
            if ok:
                r.record_pass("T12 Data replicated to all backups (verified via data dirs)")
        else:
            # Remote: read directly via gRPC from each replica
            ok = True
            for addr in servers:
                data = open_read_direct(addr, "c_repl.txt")
                if data != CONTENT:
                    ok = False
                    r.record_fail("T12 Data replicated to all backups",
                                  f"{addr}: got {data!r}")
                    break
            if ok:
                r.record_pass("T12 Data replicated to all backups (verified via gRPC)")
    except Exception as e:
        r.record_fail("T12 Data replicated to all backups", str(e))
    finally:
        c.disconnect()

    # T13 - Version consistent across all replicas after write
    c = fresh_client(servers, "c_t13")
    try:
        c.create("c_ver.txt")
        fh = c.open("c_ver.txt", mode="w")
        c.write(fh, 0, b"v1\n")
        c.close(fh)
        time.sleep(0.3)

        versions = {}
        for addr in servers:
            try:
                ch   = grpc.insecure_channel(addr)
                stub = fs_pb2_grpc.FileSystemStub(ch)
                resp = stub.TestVersionNumber(
                    fs_pb2.TestVersionRequest(filename="c_ver.txt"), timeout=3.0
                )
                versions[addr] = resp.version if resp.success else -1
                ch.close()
            except Exception:
                versions[addr] = -1

        all_same = len(set(versions.values())) == 1
        if all_same:
            r.record_pass(f"T13 Version consistent across all replicas "
                          f"(v={list(versions.values())[0]})")
        else:
            r.record_fail("T13 Version consistent across all replicas",
                          str(versions))
    except Exception as e:
        r.record_fail("T13 Version consistent across all replicas", str(e))
    finally:
        c.disconnect()

    # T14 - Read from backup returns same data as primary
    c = fresh_client(servers, "c_t14")
    try:
        c.create("c_read_backup.txt")
        fh = c.open("c_read_backup.txt", mode="w")
        c.write(fh, 0, b"from primary\n")
        c.close(fh)
        time.sleep(0.3)

        primary = c._primary_addr
        backups = [s for s in servers if s != primary]
        if not backups:
            r.record_skip("T14 Read from backup matches primary",
                          "only one server in cluster")
        else:
            data_from_primary = open_read_direct(primary, "c_read_backup.txt")
            ok = True
            for bk in backups:
                data_from_backup = open_read_direct(bk, "c_read_backup.txt")
                if data_from_backup != data_from_primary:
                    ok = False
                    r.record_fail("T14 Read from backup matches primary",
                                  f"{bk}: {data_from_backup!r} vs {data_from_primary!r}")
                    break
            if ok:
                r.record_pass("T14 Read from backup matches primary")
    except Exception as e:
        r.record_fail("T14 Read from backup matches primary", str(e))
    finally:
        c.disconnect()



# Group D - Primary failure / failover
def run_group_d(servers, r: TestResult, ctrl: ClusterController):
    section("Group D - Primary failure / failover")

    c = fresh_client(servers, "d_setup")
    try:
        c.create("d_failover.txt")
        fh = c.open("d_failover.txt", mode="w")
        c.write(fh, 0, b"before failover\n")
        c.close(fh)
        old_primary = c._primary_addr
        time.sleep(0.5)   # let replication propagate to backups before killing primary
    except Exception as e:
        r.record_fail("D setup", str(e))
        c.disconnect()
        return
    finally:
        c.disconnect()

    print(f"\n  {INFO_LABEL} Killing primary ({old_primary}) ...")
    ctrl.kill(old_primary)
    ctrl.wait_election()

    # T15 - New primary elected after failure
    c = fresh_client(servers, "d_t15")
    try:
        new_primary = c._primary_addr
        if new_primary != old_primary:
            r.record_pass(f"T15 New primary elected ({old_primary} -> {new_primary})")
        else:
            # Try forcing rediscovery
            c._primary_addr = c._discover_primary()
            new_primary = c._primary_addr
            if new_primary != old_primary:
                r.record_pass(f"T15 New primary elected after rediscovery "
                              f"({old_primary} -> {new_primary})")
            else:
                r.record_fail("T15 New primary elected after failure",
                              f"still reporting {new_primary}")
    except Exception as e:
        r.record_fail("T15 New primary elected after failure", str(e))
    finally:
        c.disconnect()

    # T16 - Read succeeds after primary failure
    c = fresh_client(servers, "d_t16")
    try:
        fh   = c.open("d_failover.txt", mode="r")
        data = c.read(fh, 0, 50)
        c.close(fh)
        if b"before failover" in data:
            r.record_pass("T16 Read succeeds from new primary after failure")
        else:
            r.record_fail("T16 Read succeeds from new primary after failure",
                          f"got {data!r}")
    except Exception as e:
        r.record_fail("T16 Read succeeds from new primary after failure", str(e))
    finally:
        c.disconnect()

    # T17 - Write succeeds after primary failure
    c = fresh_client(servers, "d_t17")
    try:
        fh = c.open("d_failover.txt", mode="w")
        c.write(fh, 0, b"after failover\n")
        c.close(fh)
        fh   = c.open("d_failover.txt", mode="r")
        data = c.read(fh, 0, 50)
        c.close(fh)
        if b"after failover" in data:
            r.record_pass("T17 Write succeeds after primary failure")
        else:
            r.record_fail("T17 Write succeeds after primary failure",
                          f"got {data!r}")
    except Exception as e:
        r.record_fail("T17 Write succeeds after primary failure", str(e))
    finally:
        c.disconnect()

    # T18 - New primary chosen is lowest-address surviving server
    surviving = sorted(s for s in servers if s != old_primary)
    c = fresh_client(servers, "d_t18")
    try:
        actual_primary = c._primary_addr
        if actual_primary == surviving[0]:
            r.record_pass(f"T18 Lowest-address backup became primary ({actual_primary})")
        else:
            # The server might have been assigned different due to local hostname resolution
            r.record_fail("T18 Lowest-address backup became primary",
                          f"expected {surviving[0]}, got {actual_primary}")
    except Exception as e:
        r.record_fail("T18 Lowest-address backup became primary", str(e))
    finally:
        c.disconnect()

    # T19 - Multiple sequential writes to new primary
    c = fresh_client(servers, "d_t19")
    try:
        ok = True
        for i in range(3):
            fh = c.open("d_failover.txt", mode="w")
            c.write(fh, 0, f"write{i}\n".encode())
            c.close(fh)
            fh   = c.open("d_failover.txt", mode="r")
            data = c.read(fh, 0, 20)
            c.close(fh)
            if f"write{i}".encode() not in data:
                ok = False
                r.record_fail("T19 Sequential writes to new primary",
                              f"iteration {i}: got {data!r}")
                break
        if ok:
            r.record_pass("T19 Sequential writes to new primary (3 rounds)")
    except Exception as e:
        r.record_fail("T19 Sequential writes to new primary", str(e))
    finally:
        c.disconnect()

    return old_primary   # caller may restart for Group F tests



# Group E - Backup failure
def run_group_e(servers, r: TestResult, ctrl: ClusterController, alive_primary: str):
    section("Group E - Backup failure")

    backups = [s for s in servers if s != alive_primary]
    if not backups:
        r.record_skip("T20 Write with one backup down", "need >=2 servers")
        r.record_skip("T21 Read with one backup down",  "need >=2 servers")
        return

    victim_backup = backups[0]
    print(f"\n  {INFO_LABEL} Killing backup ({victim_backup}) ...")
    ctrl.kill(victim_backup)
    time.sleep(2)

    # T20 - Primary still accepts writes when one backup is down
    c = fresh_client(servers, "e_t20")
    try:
        c.create("e_backup_down.txt")
        fh = c.open("e_backup_down.txt", mode="w")
        c.write(fh, 0, b"written with backup down\n")
        c.close(fh)
        fh   = c.open("e_backup_down.txt", mode="r")
        data = c.read(fh, 0, 50)
        c.close(fh)
        if b"written with backup down" in data:
            r.record_pass("T20 Write succeeds with one backup down")
        else:
            r.record_fail("T20 Write succeeds with one backup down",
                          f"got {data!r}")
    except Exception as e:
        r.record_fail("T20 Write succeeds with one backup down", str(e))
    finally:
        c.disconnect()

    # T21 - Reads still served when one backup is down
    c = fresh_client(servers, "e_t21")
    try:
        fh   = c.open("e_backup_down.txt", mode="r")
        data = c.read(fh, 0, 50)
        c.close(fh)
        if b"written with backup down" in data:
            r.record_pass("T21 Read succeeds with one backup down")
        else:
            r.record_fail("T21 Read succeeds with one backup down",
                          f"got {data!r}")
    except Exception as e:
        r.record_fail("T21 Read succeeds with one backup down", str(e))
    finally:
        c.disconnect()

    print(f"\n  {INFO_LABEL} Restarting backup ({victim_backup}) ...")
    ctrl.restart(victim_backup)
    ctrl.wait_sync()

    return victim_backup   # returned so Group F can verify its sync



# Group F - Server recovery / re-sync
def run_group_f(servers, r: TestResult, ctrl: ClusterController,
                killed_primary: str | None, restarted_backup: str | None):
    section("Group F - Server recovery / re-sync")

    # T22 - Restarted backup has the data written while it was down
    if restarted_backup:
        data = open_read_direct(restarted_backup, "e_backup_down.txt")
        if data is not None and b"written with backup down" in data:
            r.record_pass(f"T22 Restarted backup synced data written during downtime")
        else:
            r.record_fail(f"T22 Restarted backup synced data written during downtime",
                          f"{restarted_backup}: got {data!r}")
    else:
        r.record_skip("T22 Restarted backup synced data", "backup was not restarted")

    # T23 - Restart the killed primary; it rejoins as backup and syncs
    if killed_primary:
        print(f"\n  {INFO_LABEL} Restarting old primary ({killed_primary}) ...")
        ctrl.restart(killed_primary)
        ctrl.wait_sync()

        # Verify it now has data written after it was killed (Group D wrote "after failover")
        data = open_read_direct(killed_primary, "d_failover.txt")
        if data is not None and b"write2" in data:
            r.record_pass("T23 Recovered old primary synced post-failover writes")
        else:
            r.record_fail("T23 Recovered old primary synced post-failover writes",
                          f"got {data!r}")

        # T24 - Recovered server reports correct role (backup, not primary)
        try:
            import replication_pb2, replication_pb2_grpc
            ch   = grpc.insecure_channel(killed_primary)
            stub = replication_pb2_grpc.ReplicationServiceStub(ch)
            resp = stub.GetClusterInfo(
                replication_pb2.GetClusterInfoRequest(), timeout=3.0
            )
            ch.close()
            if resp.my_role == "backup":
                r.record_pass("T24 Recovered server rejoins as backup (not primary)")
            else:
                r.record_fail("T24 Recovered server rejoins as backup",
                              f"role={resp.my_role}")
        except Exception as e:
            r.record_fail("T24 Recovered server rejoins as backup", str(e))
    else:
        r.record_skip("T23 Recovered old primary synced",  "primary was not killed")
        r.record_skip("T24 Recovered server rejoins as backup", "primary was not killed")



# Group G - Idempotency / duplicate request handling
def run_group_g(servers, r: TestResult):
    section("Group G - Idempotency / duplicate request handling")

    # T25 - Duplicate Create (same seq_num) returns same response, no double-create
    c = fresh_client(servers, "g_t25")
    try:
        import fs_pb2, fs_pb2_grpc
        ch   = grpc.insecure_channel(c._primary_addr)
        stub = fs_pb2_grpc.FileSystemStub(ch)

        req = fs_pb2.CreateRequest(
            filename="g_idem.txt",
            client_id=c.client_id,
            seq_num=999,
        )
        resp1 = stub.Create(req, timeout=4.0)
        resp2 = stub.Create(req, timeout=4.0)  # duplicate seq_num
        ch.close()

        if resp1.success and resp2.success and resp1.file_handle == resp2.file_handle:
            r.record_pass("T25 Duplicate Create (same seq_num) returns cached response")
        else:
            r.record_fail("T25 Duplicate Create (same seq_num)",
                          f"resp1={resp1}, resp2={resp2}")
    except Exception as e:
        r.record_fail("T25 Duplicate Create (same seq_num)", str(e))
    finally:
        c.disconnect()

    # T26 - Duplicate Close (same seq_num) returns cached response, no double-write
    c = fresh_client(servers, "g_t26")
    try:
        c.create("g_close_idem.txt")
        fh = c.open("g_close_idem.txt", mode="w")

        with open(c.active_handles[fh]["path"], "rb") as f:
            file_data = f.read()

        import fs_pb2, fs_pb2_grpc
        ch   = grpc.insecure_channel(c._primary_addr)
        stub = fs_pb2_grpc.FileSystemStub(ch)

        req = fs_pb2.CloseRequest(
            file_handle=fh,
            file_data=b"idempotent close\n",
            client_id=c.client_id,
            seq_num=888,
        )
        resp1 = stub.Close(req, timeout=4.0)
        req2  = fs_pb2.CloseRequest(
            file_handle=fh,
            file_data=b"DIFFERENT data\n",  # different data, same seq_num
            client_id=c.client_id,
            seq_num=888,
        )
        resp2 = stub.Close(req2, timeout=4.0)
        ch.close()

        v1 = c.test_version_number("g_close_idem.txt")
        fh2  = c.open("g_close_idem.txt", mode="r")
        data = c.read(fh2, 0, 50)
        c.close(fh2)

        if (resp1.success and resp2.success and
                b"idempotent close" in data and b"DIFFERENT" not in data):
            r.record_pass("T26 Duplicate Close (same seq_num) does not double-write")
        else:
            r.record_fail("T26 Duplicate Close (same seq_num)",
                          f"data={data!r} resp1={resp1.success} resp2={resp2.success}")
    except Exception as e:
        r.record_fail("T26 Duplicate Close (same seq_num)", str(e))
    finally:
        c.disconnect()



# Group H - Concurrent clients
def run_group_h(servers, r: TestResult):
    section("Group H - Concurrent clients")

    # T27 - Two clients write different files simultaneously; no interference
    errors = []

    def client_writer(name, content, errs):
        c = fresh_client(servers, f"h_t27_{name}")
        try:
            fname = f"h_concurrent_{name}.txt"
            c.create(fname)
            fh = c.open(fname, mode="w")
            c.write(fh, 0, content)
            c.close(fh)
            fh   = c.open(fname, mode="r")
            data = c.read(fh, 0, len(content) + 10)
            c.close(fh)
            if data != content:
                errs.append(f"{name}: expected {content!r}, got {data!r}")
        except Exception as e:
            errs.append(f"{name}: {e}")
        finally:
            c.disconnect()

    t1 = threading.Thread(target=client_writer,
                          args=("alpha", b"alpha payload\n", errors))
    t2 = threading.Thread(target=client_writer,
                          args=("beta",  b"beta payload\n",  errors))
    t1.start(); t2.start()
    t1.join();  t2.join()

    if not errors:
        r.record_pass("T27 Two concurrent writers to different files")
    else:
        r.record_fail("T27 Two concurrent writers to different files",
                      "; ".join(errors))

    # T28 - Two clients read the same file simultaneously
    c_setup = fresh_client(servers, "h_t28_setup")
    try:
        c_setup.create("h_shared_read.txt")
        fh = c_setup.open("h_shared_read.txt", mode="w")
        c_setup.write(fh, 0, b"shared read content\n")
        c_setup.close(fh)
    except Exception as e:
        r.record_fail("T28 Concurrent readers", f"setup failed: {e}")
        c_setup.disconnect()
        return
    finally:
        c_setup.disconnect()

    read_errors = []

    def client_reader(tag, errs):
        c = fresh_client(servers, f"h_t28_{tag}")
        try:
            fh = c.open("h_shared_read.txt", mode="r")
            data = c.read(fh, 0, 50)
            c.close(fh)
            if data != b"shared read content\n":
                errs.append(f"{tag}: got {data!r}")
        except Exception as e:
            errs.append(f"{tag}: {e}")
        finally:
            c.disconnect()

    readers = [threading.Thread(target=client_reader, args=(f"r{i}", read_errors))
               for i in range(4)]
    for t in readers: t.start()
    for t in readers: t.join()

    if not read_errors:
        r.record_pass("T28 Four concurrent readers get consistent data")
    else:
        r.record_fail("T28 Four concurrent readers",
                      "; ".join(read_errors))



# Group I - Edge cases
def run_group_i(servers, r: TestResult):
    section("Group I - Edge cases")

    # T29 - Read beyond end of file returns what is available
    c = fresh_client(servers, "i_t29")
    try:
        c.create("i_eof.txt")
        fh = c.open("i_eof.txt", mode="w")
        c.write(fh, 0, b"short\n")
        c.close(fh)
        fh   = c.open("i_eof.txt", mode="r")
        data = c.read(fh, 0, 10_000)   # request much more than exists
        c.close(fh)
        if data == b"short\n":
            r.record_pass("T29 Read past EOF returns available bytes only")
        else:
            r.record_fail("T29 Read past EOF", f"got {data!r}")
    except Exception as e:
        r.record_fail("T29 Read past EOF", str(e))
    finally:
        c.disconnect()

    # T30 - Read at non-zero offset
    c = fresh_client(servers, "i_t30")
    try:
        c.create("i_offset.txt")
        fh = c.open("i_offset.txt", mode="w")
        c.write(fh, 0, b"ABCDEFGHIJ\n")
        c.close(fh)
        fh   = c.open("i_offset.txt", mode="r")
        data = c.read(fh, 5, 3)   # bytes at positions 5,6,7 -> 'FGH'
        c.close(fh)
        if data == b"FGH":
            r.record_pass("T30 Read at non-zero offset")
        else:
            r.record_fail("T30 Read at non-zero offset", f"expected b'FGH', got {data!r}")
    except Exception as e:
        r.record_fail("T30 Read at non-zero offset", str(e))
    finally:
        c.disconnect()

    # T31 - Invalid file handle raises error
    c = fresh_client(servers, "i_t31")
    try:
        try:
            c.read(9999, 0, 10)
            r.record_fail("T31 Read with invalid handle raises error", "no exception")
        except Exception:
            r.record_pass("T31 Read with invalid handle raises error")
    finally:
        c.disconnect()

    # T32 - TestVersionNumber on non-existent file returns version 1 (default)
    c = fresh_client(servers, "i_t32")
    try:
        v = c.test_version_number("i_nonexistent_xyz.txt")
        if v >= 1:
            r.record_pass(f"T32 TestVersionNumber on non-existent file returns {v}")
        else:
            r.record_fail("T32 TestVersionNumber on non-existent file", f"got {v}")
    except Exception as e:
        r.record_fail("T32 TestVersionNumber on non-existent file", str(e))
    finally:
        c.disconnect()



# Master runner
def run_all(servers: list, ctrl: ClusterController,
            skip_fault_injection: bool = False) -> bool:
    """
    Run all test groups.

    skip_fault_injection=True skips Groups D-F (kill/restart tests).
    Useful for quick smoke tests or when fault injection is unavailable.
    """
    r = TestResult()

    run_group_a(servers, r)
    run_group_b(servers, r)
    run_group_c(servers, r, ctrl)
    run_group_g(servers, r)
    run_group_h(servers, r)
    run_group_i(servers, r)

    killed_primary    = None
    restarted_backup  = None

    if skip_fault_injection:
        section("Groups D-F - Fault injection (SKIPPED)")
        print(f"  {SKIP_LABEL} All fault-injection tests skipped (--no-fault flag)")
        r.skipped += 7   # approximate count
    else:
        killed_primary = run_group_d(servers, r, ctrl)
        surviving_primary = sorted(s for s in servers if s != killed_primary)[0]
        restarted_backup  = run_group_e(servers, r, ctrl, surviving_primary)
        run_group_f(servers, r, ctrl, killed_primary, restarted_backup)

    return r.summary()
