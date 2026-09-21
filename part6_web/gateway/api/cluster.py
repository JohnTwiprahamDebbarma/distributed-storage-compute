"""HTTP routes for the cluster: status, a live status stream, and fault injection."""

import asyncio
from typing import Annotated

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool

from gateway.api.dependencies import get_cluster
from gateway.api.schemas import ClusterOut, IsolationIn, IsolationOut, NodeOut, Problem
from gateway.domain import NodeStatus, NotFound, current_leader

router = APIRouter(prefix="/v1/cluster", tags=["cluster"])

Cluster = Annotated[object, Depends(get_cluster)]


def summarize(statuses: list[NodeStatus]) -> ClusterOut:
    leader = current_leader(statuses)
    return ClusterOut(
        leader_id=leader.node_id if leader else None,
        term=max((s.term for s in statuses if s.reachable), default=0),
        nodes=[NodeOut.model_validate(s, from_attributes=True) for s in statuses])


@router.get("", response_model=ClusterOut)
def cluster_status(cluster: Cluster):
    """Each node's role, term and log progress, as the node itself reports it."""
    return summarize(cluster.node_statuses())


@router.put("/nodes/{node_id}/isolation", response_model=IsolationOut,
            responses={404: {"model": Problem}, 503: {"model": Problem}})
def set_isolation(node_id: int, body: IsolationIn, cluster: Cluster):
    """Cut a node off from the cluster, or reconnect it (fault injection).

    An isolated node drops all Raft and client traffic, as if unplugged, and
    reconnects on its own after `heal_after_seconds`, so it can't stay cut off.
    """
    if not 0 <= node_id < cluster.node_count:
        raise NotFound(f"there is no node {node_id}")
    heal_after = body.heal_after_seconds if body.isolated else 0.0
    isolated = cluster.set_isolated(node_id, body.isolated, heal_after)
    return IsolationOut(node_id=node_id, isolated=isolated)


@router.websocket("/stream")
async def stream_status(websocket: WebSocket, cluster: Cluster):
    """Push the cluster status (same shape as GET /v1/cluster) every STATUS_INTERVAL seconds."""
    interval = websocket.app.state.settings.status_interval
    await websocket.accept()
    try:
        while True:
            # node_statuses() blocks on gRPC, so run it off the event loop.
            statuses = await run_in_threadpool(cluster.node_statuses)
            await websocket.send_json(summarize(statuses).model_dump(mode="json"))
            try:
                # The client never sends anything; waiting for a message is how
                # we notice promptly when it goes away.
                await asyncio.wait_for(websocket.receive_text(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    except WebSocketDisconnect:
        pass
