import argparse
import os
import sys
import subprocess
import time
import signal
import shutil

# Resolve paths regardless of where the script is called from
SCRIPT_DIR= os.path.dirname(os.path.abspath(__file__))
REPL_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
VENV_PYTHON = os.path.join(REPL_DIR, "venv", "bin", "python")
SERVER_PY= os.path.join(REPL_DIR, "server.py")

sys.path.insert(0, os.path.join(REPL_DIR, ".."))  # fs/ for fs_pb2
sys.path.insert(0, REPL_DIR)                       # fs/replication/ first

from tests.test_suite import ClusterController, run_all


# LocalClusterController
class LocalClusterController(ClusterController):
    """
    Manages three local server processes.
    Supports kill() / restart() for fault-injection tests.
    Also exposes data_dir() so tests can directly inspect server files.
    """
    def __init__(self, ports: list):
        self.ports = ports
        self.addrs = [f"localhost:{p}" for p in ports]
        self._procs = {}    # addr -> subprocess.Popen
        self._datadirs = {}   # addr -> str

    def server_addrs(self) -> list:
        return self.addrs

    def data_dir(self, addr: str) -> str | None:
        return self._datadirs.get(addr)

    def is_local(self) -> bool:
        return True

    def _kill_stale_on_ports(self):
        """Kill any processes already listening on our ports (stale from prior runs)."""
        for port in self.ports:
            try:
                result = subprocess.run(
                    ["lsof", "-t", f"-i:{port}"],
                    capture_output=True, text=True
                )
                pids = result.stdout.strip().split()
                for pid in pids:
                    try:
                        os.kill(int(pid), signal.SIGKILL)
                        print(f"  [CLEANUP] Killed stale process PID={pid} on port {port}")
                    except (ProcessLookupError, ValueError):
                        pass
            except FileNotFoundError:
                pass   # lsof not available; skip
        if any(self.ports):
            time.sleep(1)   # let ports fully release

    def start_all(self, input_dir: str | None = None):
        """Start all servers. Primary first, then backups (staggered)."""
        self._kill_stale_on_ports()

        for i, addr in enumerate(self.addrs):
            port = self.ports[i]
            datadir = os.path.join(REPL_DIR, f"test_data_{port}")
            os.makedirs(datadir, exist_ok=True)
            self._datadirs[addr] = datadir

            # Optionally seed the primary with input files
            if i == 0 and input_dir and os.path.isdir(input_dir):
                for fname in os.listdir(input_dir):
                    src = os.path.join(input_dir, fname)
                    dst = os.path.join(datadir, fname)
                    if os.path.isfile(src) and not os.path.exists(dst):
                        shutil.copy2(src, dst)

        # Start primary first so backups can sync from it
        self._start_one(self.addrs[0])
        time.sleep(2)                   # let primary gRPC port open

        for addr in self.addrs[1:]:
            self._start_one(addr)

        # Poll until every server is reachable AND reports a definite role.
        # This avoids races where tests start before gRPC servers are listening.
        self._wait_all_ready(timeout=30)

    def _start_one(self, addr: str):
        port = addr.split(":")[1]
        peers = [a for a in self.addrs if a != addr]
        datadir = self._datadirs[addr]
        log = open(os.path.join(REPL_DIR, f"server_{port}.log"), "w")
        cmd = [
            VENV_PYTHON, "-u", SERVER_PY,
            "--addr",     addr,
            "--peers",    ",".join(peers),
            "--data-dir", datadir,
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=log, stderr=log,
            preexec_fn=os.setsid,     # own process group for clean kill
        )
        self._procs[addr] = proc
        print(f"  [CLUSTER] Started {addr}  PID={proc.pid}")

    def _wait_all_ready(self, timeout: float = 30):
        """Block until all servers respond to GetClusterInfo with a known role."""
        import grpc
        import replication_pb2, replication_pb2_grpc

        deadline = time.time() + timeout
        pending = set(self.addrs)
        while pending and time.time() < deadline:
            still_pending = set()
            for addr in pending:
                try:
                    ch = grpc.insecure_channel(addr)
                    stub = replication_pb2_grpc.ReplicationServiceStub(ch)
                    resp = stub.GetClusterInfo(
                        replication_pb2.GetClusterInfoRequest(), timeout=2.0
                    )
                    ch.close()
                    if resp.success and resp.my_role in ("primary", "backup"):
                        print(f"  [CLUSTER] {addr} ready  role={resp.my_role}")
                    else:
                        still_pending.add(addr)
                except Exception:
                    still_pending.add(addr)
            pending = still_pending
            if pending:
                time.sleep(1)

        if pending:
            print(f"  [WARN] Servers not ready after {timeout}s: {pending}")
        else:
            # Extra pause so gRPC peer channels on the primary (created before backups existed) have time to exit TRANSIENT_FAILURE and reconnect.
            time.sleep(3)
            print("  [CLUSTER] All servers ready.")

    def stop_all(self):
        for addr, proc in list(self._procs.items()):
            self._kill_proc(addr, proc)
        self._procs.clear()

    def _kill_proc(self, addr: str, proc):
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        print(f"  [CLUSTER] Stopped {addr}  PID={proc.pid}")

    def kill(self, addr: str):
        proc = self._procs.get(addr)
        if proc:
            self._kill_proc(addr, proc)
            del self._procs[addr]

    def restart(self, addr: str):
        # Kill if still running (idempotent)
        if addr in self._procs:
            self.kill(addr)
        time.sleep(1)
        self._start_one(addr)
        # wait_sync() called by the test after restart

    def cleanup_data(self):
        for addr in self.addrs:
            d = self._datadirs.get(addr)
            if d and os.path.isdir(d):
                shutil.rmtree(d)
                print(f"  [CLEANUP] Removed {d}")

    # Timing overrides (local processes are fast)
    def wait_election(self):
        time.sleep(9)   # HEARTBEAT_TIMEOUT(6) + margin

    def wait_sync(self):
        time.sleep(6)



# Entry point
def main():
    parser = argparse.ArgumentParser(
        description="Automated local test runner for the replicated DSFS"
    )
    parser.add_argument(
        "--ports",
        default="50050,50051,50052",
        help="Comma-separated ports for the 3 replica servers (default: 50050,50051,50052)",
    )
    parser.add_argument(
        "--no-fault",
        action="store_true",
        help="Skip fault-injection tests (Groups D–F)",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Leave servers running and data directories intact after tests",
    )
    parser.add_argument(
        "--input-dir",
        default=None,
        help="Optional directory of input files to seed the primary with",
    )
    args  = parser.parse_args()
    ports = [int(p.strip()) for p in args.ports.split(",")]

    if len(ports) != 3:
        print("ERROR: exactly 3 ports required", file=sys.stderr)
        sys.exit(1)

    ctrl = LocalClusterController(ports)

    print("=" * 62)
    print("  Replicated DSFS – Local Test Runner")
    print(f"  Servers: {ctrl.addrs}")
    print("=" * 62)

    # ---- Start cluster ----
    print("\n[SETUP] Starting 3-node cluster …")
    ctrl.start_all(input_dir=args.input_dir)

    # ---- Run tests ----
    passed = False
    try:
        passed = run_all(
            servers=ctrl.addrs,
            ctrl=ctrl,
            skip_fault_injection=args.no_fault,
        )
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")
    finally:
        if not args.no_cleanup:
            print("\n[TEARDOWN] Stopping servers …")
            ctrl.stop_all()
            print("[TEARDOWN] Removing test data directories …")
            ctrl.cleanup_data()
        else:
            print("\n[INFO] --no-cleanup set; servers left running.")
            print(f"  Log files: {REPL_DIR}/server_*.log")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
