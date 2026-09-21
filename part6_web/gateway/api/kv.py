"""
HTTP routes for keys (the "controller" layer). They translate HTTP - paths,
headers, status codes, ETags - to and from calls on KVService, and nothing more.

I use a key's version as its ETag, so the store's compare-and-swap becomes
standard HTTP optimistic concurrency:
    GET  /v1/kv/city                          -> 200, ETag: "3"
    PUT  /v1/kv/city   If-Match: "3"          -> 200 if still at version 3, else 412
    PUT  /v1/kv/city   If-None-Match: *       -> 201 only if the key doesn't exist yet
    GET  /v1/kv/city   If-None-Match: "3"     -> 304 if unchanged (like the file cache in part 1)
"""

import base64
import re
from typing import Annotated, Literal
from urllib.parse import quote

from fastapi import APIRouter, Depends, Header, Path, Response

from gateway.api.dependencies import get_kv_service
from gateway.api.schemas import EntryOut, Problem, ValueIn, WriteOut
from gateway.domain import InvalidRequest
from gateway.services.kv_service import KVService

router = APIRouter(prefix="/v1/kv", tags=["keys"])

Key = Annotated[str, Path(min_length=1, max_length=512,
                          description="Any text; slashes are allowed, e.g. ml/model")]
Service = Annotated[KVService, Depends(get_kv_service)]
IdempotencyKey = Annotated[str | None, Header(
    max_length=255,
    description="Send the same value when retrying a write, so it is applied at most once")]

_ERRORS = {code: {"model": Problem} for code in (400, 404, 412, 413, 503, 504)}


@router.get("/{key:path}", response_model=EntryOut,
            responses={304: {"description": "Not Modified: the cached copy is current"},
                       **_ERRORS})
def read_key(key: Key, response: Response, service: Service,
             consistency: Literal["leader", "linearizable"] = "leader",
             if_none_match: Annotated[str | None, Header()] = None):
    """Read a key.

    `consistency=linearizable` first commits a read barrier through the Raft log,
    so the value reflects every write acknowledged before the read started.
    """
    entry = service.get(key, linearizable=(consistency == "linearizable"))
    # I send no-cache, meaning "cache this but revalidate first"; If-None-Match
    # makes revalidating cheap.
    headers = {"ETag": _etag(entry.version), "Cache-Control": "no-cache"}
    if if_none_match is not None and _matches(if_none_match, entry.version):
        return Response(status_code=304, headers=headers)
    response.headers.update(headers)
    try:
        value, encoding = entry.value.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:  # written as raw bytes by another client of the store
        value, encoding = base64.b64encode(entry.value).decode(), "base64"
    return EntryOut(key=key, value=value, encoding=encoding, version=entry.version)


@router.put("/{key:path}", response_model=WriteOut,
            responses={201: {"model": WriteOut, "description": "Created (If-None-Match: *)"},
                       **_ERRORS})
def write_key(key: Key, body: ValueIn, response: Response, service: Service,
              if_match: Annotated[str | None, Header()] = None,
              if_none_match: Annotated[str | None, Header()] = None,
              idempotency_key: IdempotencyKey = None):
    """Write a key.

    - `If-Match: "<version>"`: write only if the key is still at that version (else 412).
    - `If-None-Match: *`: write only if the key doesn't exist yet (201 Created, else 412).
    """
    result = service.put(key, body.value.encode("utf-8"),
                         expected_version=_parse_if_match(if_match),
                         create_only=_parse_create_only(if_none_match),
                         idempotency_key=idempotency_key)
    response.headers["ETag"] = _etag(result.version)
    if result.created:
        response.status_code = 201
        response.headers["Location"] = f"/v1/kv/{quote(key)}"
    return WriteOut(key=key, version=result.version)


@router.delete("/{key:path}", status_code=204, responses=_ERRORS)
def delete_key(key: Key, service: Service,
               if_match: Annotated[str | None, Header()] = None,
               idempotency_key: IdempotencyKey = None):
    """Delete a key (404 if it doesn't exist)."""
    # I reject If-Match here rather than ignore it: ignoring a precondition could
    # delete something the client meant to protect.
    if if_match is not None:
        raise InvalidRequest("If-Match is not supported on DELETE yet")
    service.delete(key, idempotency_key=idempotency_key)
    return Response(status_code=204)


# ETag helpers
_STRONG_ETAG = re.compile(r'"([0-9]+)"|([0-9]+)')


def _etag(version: int) -> str:
    return f'"{version}"'


def _parse_if_match(header: str | None) -> int | None:
    """If-Match: "3" -> 3. One strong ETag only."""
    if header is None:
        return None
    tag = header.strip()
    if tag == "*":
        raise InvalidRequest('If-Match: * is not supported; send the ETag from a read, e.g. "3"')
    if tag.startswith("W/"):
        raise InvalidRequest("If-Match needs a strong ETag; a weak one (W/...) never matches")
    match = _STRONG_ETAG.fullmatch(tag)
    if not match:
        raise InvalidRequest(f"unrecognised ETag {header!r}; expected a quoted version like \"3\"")
    return int(match.group(1) or match.group(2))


def _parse_create_only(header: str | None) -> bool:
    """If-None-Match: * on a write means "only if the key doesn't exist yet"."""
    if header is None:
        return False
    if header.strip() != "*":
        raise InvalidRequest("on a write, only If-None-Match: * (create only) is supported")
    return True


def _matches(if_none_match: str, version: int) -> bool:
    """Whether the client's cached copy is current (weak comparison, RFC 9110 §13.1.2)."""
    for tag in if_none_match.split(","):
        tag = tag.strip()
        if tag == "*" or tag.removeprefix("W/").strip('"') == str(version):
            return True
    return False
