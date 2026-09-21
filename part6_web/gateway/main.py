"""
The web gateway: an HTTP/JSON API in front of the Raft key-value cluster.

Layers, each depending only on the one below it:
    api/        controllers - HTTP in and out: routes, schemas, error responses
    services/   rules       - value limits, conditional writes, idempotency keys
    clients/    repository  - gRPC to the nodes: leader tracking, redirects, retries

Run it from part6_web/, with the cluster up (see README.md):
    fastapi dev gateway/main.py         # then open http://127.0.0.1:8000/docs
"""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from gateway.api import cluster, health, kv, problems
from gateway.config import Settings, load_settings
from gateway.services.kv_service import KVService

DESCRIPTION = """
A REST API over a 3-node Raft key-value store. Leader failover happens inside the
gateway: clients never see a redirect, only a slower response while a new leader
is elected.

* **Versions are ETags.** Send `If-Match: "<version>"` to write only if nobody
  changed the key since you read it (optimistic concurrency); `412` means someone did.
* **Retries are safe.** Send an `Idempotency-Key` with a write, and send the same
  key if you retry: the write is applied at most once, even across a failover.
* **Errors** are RFC 9457 problem details (`application/problem+json`). `503`
  means the write was not applied; `504` means it may have been, so retry with
  the same `Idempotency-Key`.
"""


def create_app(settings: Settings | None = None, cluster_client=None) -> FastAPI:
    """Build the app. Tests pass a fake cluster_client; otherwise it connects to KV_NODES."""
    settings = settings or load_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        client = cluster_client
        if client is None:
            # Imported here so the tests, which bring their own fake, need no gRPC stubs.
            from gateway.clients.raft_cluster import RaftCluster
            client = RaftCluster(settings.kv_nodes, rpc_timeout=settings.rpc_timeout,
                                 request_deadline=settings.request_deadline)
        app.state.settings = settings
        app.state.cluster = client
        app.state.kv_service = KVService(client, settings.max_value_bytes)
        try:
            yield
        finally:
            if cluster_client is None:
                client.close()

    app = FastAPI(title="Many-As-One gateway", version="1.0.0",
                  description=DESCRIPTION, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.cors_origins),
        allow_methods=["GET", "PUT", "DELETE"],
        allow_headers=["Content-Type", "If-Match", "If-None-Match", "Idempotency-Key"],
        # Browsers hide response headers from JavaScript unless they are listed here.
        expose_headers=["ETag", "Location", "Retry-After"],
    )
    problems.register(app)
    app.include_router(kv.router)
    app.include_router(cluster.router)
    app.include_router(health.router)
    return app


app = create_app()
