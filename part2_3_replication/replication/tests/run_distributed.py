import argparse
import os
import sys
import subprocess
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPL_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

sys.path.insert(0, os.path.join(REPL_DIR, ".."))  # fs/ for fs_pb2
sys.path.insert(0, REPL_DIR)                       # fs/replication/ first

from tests.test_suite import ClusterController, run_all, INFO_LABEL

# SSH-based ClusterController
class SSHClusterController(ClusterController):
    """
    Injects faults via SSH.
    kill() -> pkill -f 'server.py.*<port>'
    restart() -> nohup python server.py ... & (re-uses same args)
    """

    def __init__(self, addrs: list, ssh_user: str, ssh_key: str | None,
                 server_dir: str, data_dir_pattern: str,
                 election_wait: float = 10, sync_wait: float = 9):
        """
        addrs - list of "host:port"
        ssh_user - SSH username
        ssh_key - path to private key (None = default)
        server_dir - remote directory containing server.py (same on all machines)
        data_dir_pattern - remote data directory (same on all machines)
        """
        self.addrs= addrs
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key
        self.server_dir = server_dir
        self.data_dir_pattern= data_dir_pattern
        self._election_wait = election_wait
        self._sync_wait = sync_wait
        # Store startup args for restart
        self._start_cmds: dict = {}   # addr -> remote shell command string
        self._build_start_cmds()

    def _build_start_cmds(self):
        for addr in self.addrs:
            peers = [a for a in self.addrs if a != addr]
            host, port = addr.split(":")
            python_cmd = f"cd {self.server_dir} && nohup ./venv/bin/python -u server.py"
            python_cmd += f" --addr {addr}"
            python_cmd += f" --peers {','.join(peers)}"
            python_cmd += f" --data-dir {self.data_dir_pattern}"
            python_cmd += f" >> server_{port}.log 2>&1 &"
            self._start_cmds[addr] = python_cmd

    def _ssh(self, host: str, cmd: str) -> subprocess.CompletedProcess:
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no",
                   "-o", "ConnectTimeout=5"]
        if self.ssh_key:
            ssh_cmd += ["-i", self.ssh_key]
        ssh_cmd += [f"{self.ssh_user}@{host}", cmd]
        return subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=15)

    def server_addrs(self) -> list:
        return self.addrs

    def is_local(self) -> bool:
        return False

    def kill(self, addr: str):
        host, port = addr.split(":")
        result = self._ssh(host, f"pkill -f 'server.py.*{port}' || true")
        print(f"  [SSH KILL] {addr}  rc={result.returncode}")

    def restart(self, addr: str):
        host = addr.split(":")[0]
        cmd  = self._start_cmds[addr]
        result = self._ssh(host, cmd)
        print(f"  [SSH RESTART] {addr}  rc={result.returncode}")
        if result.stderr:
            print(f"    stderr: {result.stderr.strip()}")

    def wait_election(self):
        time.sleep(self._election_wait)

    def wait_sync(self):
        time.sleep(self._sync_wait)


# Manual (operator-driven) ClusterController
class ManualClusterController(ClusterController):
    """
    Pauses and prints instructions for the human operator.
    Used when no SSH credentials are available.
    """

    def __init__(self, addrs: list,
                 election_wait: float = 10, sync_wait: float = 9):
        self.addrs= addrs
        self._election_wait= election_wait
        self._sync_wait= sync_wait

    def server_addrs(self) -> list:
        return self.addrs

    def is_local(self) -> bool:
        return False

    def kill(self, addr: str):
        host, port = addr.split(":")
        print(f"\n ACTION REQUIRED:")
        print(f"     On machine '{host}', kill the server on port {port}:")
        print(f"       pkill -f 'server.py.*{port}'")
        input("     Press Enter once the server is stopped ... ")

    def restart(self, addr: str):
        host, port = addr.split(":")
        peers = [a for a in self.addrs if a != addr]
        print(f"\n ACTION REQUIRED:")
        print(f"     On machine '{host}', restart the server:")
        print(f"       cd fs/replication")
        print(f"       ./venv/bin/python server.py \\")
        print(f"           --addr {addr} \\")
        print(f"           --peers {','.join(peers)} \\")
        print(f"           --data-dir ./server_data &")
        input("     Press Enter once the server is running ... ")

    def wait_election(self):
        print(f"  {INFO_LABEL} Waiting {self._election_wait}s for election ...")
        time.sleep(self._election_wait)

    def wait_sync(self):
        print(f"  {INFO_LABEL} Waiting {self._sync_wait}s for sync ...")
        time.sleep(self._sync_wait)


# Connectivity pre-check
def check_connectivity(addrs: list) -> bool:
    """Verify all servers are reachable before starting tests."""
    import grpc
    import replication_pb2, replication_pb2_grpc
    all_ok = True
    for addr in addrs:
        try:
            ch   = grpc.insecure_channel(addr)
            stub = replication_pb2_grpc.ReplicationServiceStub(ch)
            resp = stub.GetClusterInfo(
                replication_pb2.GetClusterInfoRequest(), timeout=3.0
            )
            ch.close()
            role = resp.my_role if resp.success else "?"
            print(f"  YES  {addr}  role={role}  primary={resp.primary_address}")
        except Exception as e:
            print(f"  NO  {addr}  UNREACHABLE: {e}")
            all_ok = False
    return all_ok


# Entry point
def main():
    parser = argparse.ArgumentParser(
        description="Test runner for a multi-machine replicated DSFS cluster",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--servers", required=True,
        help="Comma-separated server addresses, e.g. host1:50050,host2:50050,host3:50050",
    )
    parser.add_argument(
        "--no-fault", action="store_true",
        help="Skip fault-injection tests (Groups D-F) - safe read-only run",
    )
    parser.add_argument(
        "--ssh-user", default=None,
        help="SSH username for automatic fault injection (skip for manual mode)",
    )
    parser.add_argument(
        "--ssh-key", default=None,
        help="Path to SSH private key (optional, uses default if omitted)",
    )
    parser.add_argument(
        "--server-dir", default="~/fs/replication",
        help="Remote directory containing server.py (default: ~/fs/replication)",
    )
    parser.add_argument(
        "--remote-data-dir", default="./server_data",
        help="Remote data directory used when restarting a server (default: ./server_data)",
    )
    parser.add_argument(
        "--election-wait", type=float, default=10.0,
        help="Seconds to wait for election after a kill (default: 10)",
    )
    parser.add_argument(
        "--sync-wait", type=float, default=9.0,
        help="Seconds to wait for sync after a restart (default: 9)",
    )
    args    = parser.parse_args()
    servers = [s.strip() for s in args.servers.split(",")]

    print("=" * 62)
    print("  Replicated DSFS - Distributed Test Runner")
    print(f"  Servers: {servers}")
    print("=" * 62)

    # ---- Pre-flight connectivity check ----
    print("\n[PRE-FLIGHT] Checking connectivity ...")
    if not check_connectivity(servers):
        print("\nERROR: One or more servers unreachable. Aborting.")
        sys.exit(1)
    print()

    # ---- Build controller ----
    if args.no_fault:
        ctrl = ManualClusterController(servers,
                                       args.election_wait, args.sync_wait)
    elif args.ssh_user:
        ctrl = SSHClusterController(
            addrs=servers,
            ssh_user=args.ssh_user,
            ssh_key=args.ssh_key,
            server_dir=args.server_dir,
            data_dir_pattern=args.remote_data_dir,
            election_wait=args.election_wait,
            sync_wait=args.sync_wait,
        )
        print(f"[INFO] SSH fault injection enabled ({args.ssh_user}@hosts)")
    else:
        ctrl = ManualClusterController(servers,
                                       args.election_wait, args.sync_wait)
        print("[INFO] No --ssh-user provided -> manual fault injection mode")

    # ---- Run tests ----
    passed = False
    try:
        passed = run_all(
            servers=servers,
            ctrl=ctrl,
            skip_fault_injection=args.no_fault,
        )
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")

    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
