"""
Gateway settings. I read them from environment variables so the same code runs
unchanged on a laptop, in Docker Compose and on a cloud VM.

    KV_NODES             node addresses in node-id order (default: localhost:50051-50053)
    KV_RPC_TIMEOUT       seconds one gRPC attempt may take (default 3)
    KV_REQUEST_DEADLINE  seconds an HTTP request may spend finding a leader (default 12)
    KV_MAX_VALUE_BYTES   largest value accepted (default 64 KiB)
    CORS_ORIGINS         browser origins allowed to call the API (default: the Vite dev server)
    STATUS_INTERVAL      seconds between live-status pushes on the WebSocket (default 0.5)
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # The i-th address is node i, matching nodes_config.json.
    kv_nodes: tuple[str, ...] = ("localhost:50051", "localhost:50052", "localhost:50053")
    rpc_timeout: float = 3.0
    # I made this long enough for a failover even when the first election is a
    # split vote: up to 4 s to time out, 2 s waiting for votes, then up to 4 s
    # more (raft_node.py's deliberately slow, easy-to-watch timings).
    request_deadline: float = 12.0
    max_value_bytes: int = 64 * 1024
    cors_origins: tuple[str, ...] = ("http://localhost:5173",)
    status_interval: float = 0.5


def _listed(raw: str | None, default: tuple[str, ...]) -> tuple[str, ...]:
    if not raw:
        return default
    return tuple(part.strip() for part in raw.split(",") if part.strip())


def load_settings() -> Settings:
    env, d = os.environ, Settings()
    return Settings(
        kv_nodes=_listed(env.get("KV_NODES"), d.kv_nodes),
        rpc_timeout=float(env.get("KV_RPC_TIMEOUT", d.rpc_timeout)),
        request_deadline=float(env.get("KV_REQUEST_DEADLINE", d.request_deadline)),
        max_value_bytes=int(env.get("KV_MAX_VALUE_BYTES", d.max_value_bytes)),
        cors_origins=_listed(env.get("CORS_ORIGINS"), d.cors_origins),
        status_interval=float(env.get("STATUS_INTERVAL", d.status_interval)),
    )
