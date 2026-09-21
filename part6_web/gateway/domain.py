"""
Plain data types and errors shared by every layer of the gateway.

I keep HTTP and gRPC out of this module so the service layer (and its tests)
can be written against these types alone. api/problems.py maps each error to
an HTTP status code.
"""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Entry:
    key: str
    value: bytes
    version: int


@dataclass(frozen=True)
class WriteResult:
    version: int
    created: bool = False


@dataclass(frozen=True)
class NodeStatus:
    """One node's own report of its Raft state (see RaftService.GetStatus)."""
    node_id: int
    address: str
    reachable: bool
    state: str = "unknown"          # "follower" | "candidate" | "leader", or "unknown"
    term: int = 0
    leader_id: int | None = None
    commit_index: int = 0
    last_applied: int = 0
    last_log_index: int = 0
    last_log_term: int = 0
    isolated: bool = False
    match_index: dict[int, int] = field(default_factory=dict)


def current_leader(statuses: list[NodeStatus]) -> NodeStatus | None:
    """The leader, judged only by nodes the rest of the cluster can reach.

    While partitioned, an isolated old leader still calls itself leader, so
    isolated nodes don't count; among the rest the highest term wins.
    """
    leaders = [s for s in statuses
               if s.reachable and not s.isolated and s.state == "leader"]
    return max(leaders, key=lambda s: s.term, default=None)


# Errors (str(error) is the human-readable detail)
class GatewayError(Exception):
    pass


class InvalidRequest(GatewayError):
    """The request is malformed (400)."""


class NotFound(GatewayError):
    """The key, or node, does not exist (404)."""


class PreconditionFailed(GatewayError):
    """A conditional write lost: the key is not in the state the client expected (412)."""

    def __init__(self, message: str, current_version: int = 0):
        super().__init__(message)
        self.current_version = current_version


class ValueTooLarge(GatewayError):
    """The value is over the configured size limit (413)."""


class StoreError(GatewayError):
    """A node answered with an unexpected error (502)."""


class ClusterUnavailable(GatewayError):
    """No leader could be reached in time, and nothing was applied (503)."""


class WriteOutcomeUnknown(GatewayError):
    """A write attempt failed after it may have reached the leader, so it may or
    may not have been applied (504)."""
