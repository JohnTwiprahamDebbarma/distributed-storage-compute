"""Liveness and readiness probes, for Docker health checks and load balancers."""

from typing import Annotated

from fastapi import APIRouter, Depends

from gateway.api.dependencies import get_cluster
from gateway.api.schemas import Problem
from gateway.domain import ClusterUnavailable, current_leader

router = APIRouter(tags=["health"])


@router.get("/healthz")
def healthz():
    """Liveness: the gateway process is up. Says nothing about the cluster."""
    return {"status": "ok"}


@router.get("/readyz", responses={503: {"model": Problem}})
def readyz(cluster: Annotated[object, Depends(get_cluster)]):
    """Readiness: there is a connected leader, so writes can succeed right now."""
    leader = current_leader(cluster.node_statuses())
    if leader is None:
        raise ClusterUnavailable("no connected leader right now")
    return {"status": "ready", "leader_id": leader.node_id, "term": leader.term}
