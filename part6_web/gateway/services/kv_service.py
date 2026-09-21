"""
The gateway's key-value rules (the "service" layer), independent of both HTTP
and gRPC: value limits, conditional writes, and turning an Idempotency-Key into
the (client_id, seq_num) identity the store de-duplicates on.
"""

import hashlib
import uuid

from gateway.domain import (Entry, InvalidRequest, NotFound, PreconditionFailed,
                            ValueTooLarge, WriteResult)


class KVService:
    def __init__(self, cluster, max_value_bytes: int):
        self.cluster = cluster
        self.max_value_bytes = max_value_bytes

    def get(self, key: str, linearizable: bool = False) -> Entry:
        found, value, version = self.cluster.get(key, linearizable)
        if not found:
            raise NotFound(f"key {key!r} does not exist")
        return Entry(key, value, version)

    def put(self, key: str, value: bytes, *, expected_version: int | None = None,
            create_only: bool = False, idempotency_key: str | None = None) -> WriteResult:
        """Write key=value; optionally only at a given version, or only if absent."""
        if len(value) > self.max_value_bytes:
            raise ValueTooLarge(
                f"value is {len(value)} bytes; the limit is {self.max_value_bytes}")
        if expected_version is not None and create_only:
            raise InvalidRequest("a write can require a version or require absence, not both")
        client_id, seq_num = self._identity(idempotency_key, "put", key, value,
                                            expected_version, create_only)

        if create_only:
            swapped, version = self.cluster.cas(key, value, client_id, seq_num,
                                                expect_absent=True)
            if not swapped:
                raise PreconditionFailed(f"key {key!r} already exists", version)
            return WriteResult(version, created=True)

        if expected_version is not None:
            if expected_version < 1:    # versions start at 1, so this never matches
                raise PreconditionFailed(f"key {key!r} is not at version {expected_version}")
            swapped, version = self.cluster.cas(key, value, client_id, seq_num,
                                                expected_version=expected_version)
            if not swapped:
                raise PreconditionFailed(
                    f"key {key!r} is at version {version}, not {expected_version}"
                    if version else f"key {key!r} does not exist", version)
            return WriteResult(version)

        return WriteResult(self.cluster.put(key, value, client_id, seq_num))

    def delete(self, key: str, *, idempotency_key: str | None = None) -> None:
        client_id, seq_num = self._identity(idempotency_key, "delete", key)
        if not self.cluster.delete(key, client_id, seq_num):
            raise NotFound(f"key {key!r} does not exist")

    @staticmethod
    def _identity(idempotency_key: str | None, *request) -> tuple[str, int]:
        """The (client_id, seq_num) the store de-duplicates this write under.

        Each HTTP request is its own client with seq_num 1. Requests handled at
        the same time therefore never race on one client's sequence numbers,
        and the gateway's own retries of a request are applied at most once.

        With an Idempotency-Key the client_id is derived from the key together
        with the request itself: retrying the same request returns the original
        result, while reusing a key for a *different* request makes a new one.
        """
        if idempotency_key is None:
            return f"gw-{uuid.uuid4().hex}", 1
        digest = hashlib.sha256(repr((idempotency_key, request)).encode()).hexdigest()
        return f"idem-{digest[:32]}", 1
