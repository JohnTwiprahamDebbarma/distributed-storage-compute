"""Request and response bodies of the HTTP API. FastAPI also builds /docs from these."""

from typing import Literal

from pydantic import BaseModel, Field


class ValueIn(BaseModel):
    value: str = Field(description="UTF-8 text to store", examples=["Kolkata"])


class EntryOut(BaseModel):
    key: str
    value: str = Field(description="The stored value; base64 when `encoding` is base64")
    encoding: Literal["utf-8", "base64"]
    version: int = Field(description="Grows on every write; also sent as the ETag header")


class WriteOut(BaseModel):
    key: str
    version: int


class NodeOut(BaseModel):
    node_id: int
    address: str
    reachable: bool
    state: str = Field(description="follower, candidate or leader; unknown if unreachable")
    term: int
    leader_id: int | None
    commit_index: int
    last_applied: int
    last_log_index: int
    last_log_term: int
    isolated: bool
    match_index: dict[int, int] = Field(
        description="On the leader: the highest log index known to be on each node")


class ClusterOut(BaseModel):
    leader_id: int | None = Field(description="The connected leader, if there is one")
    term: int = Field(description="The highest term any reachable node reports")
    nodes: list[NodeOut]


class IsolationIn(BaseModel):
    isolated: bool
    heal_after_seconds: float = Field(
        30.0, ge=5, le=300,
        description="An isolated node reconnects on its own after this long")


class IsolationOut(BaseModel):
    node_id: int
    isolated: bool


class Problem(BaseModel):
    """Every error, as RFC 9457 problem details (media type application/problem+json)."""
    type: str = "about:blank"
    title: str
    status: int
    detail: str | None = None
