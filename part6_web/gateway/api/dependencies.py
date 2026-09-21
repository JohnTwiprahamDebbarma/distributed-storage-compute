"""
How routes get their collaborators (FastAPI dependency injection). I take both
from app.state, set up in main.py, so tests can hand the app a fake cluster.
HTTPConnection covers plain requests and WebSockets alike.
"""

from starlette.requests import HTTPConnection

from gateway.services.kv_service import KVService


def get_cluster(conn: HTTPConnection):
    return conn.app.state.cluster


def get_kv_service(conn: HTTPConnection) -> KVService:
    return conn.app.state.kv_service
