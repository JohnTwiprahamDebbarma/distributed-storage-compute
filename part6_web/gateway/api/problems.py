"""
Error responses. I give every error the API returns - the gateway's own,
FastAPI's validation errors, unknown routes - one JSON shape, RFC 9457
"problem details":

    {"type": "about:blank", "title": "Precondition Failed", "status": 412,
     "detail": "key 'city' is at version 4, not 3", "current_version": 4}
"""

from http import HTTPStatus

from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from gateway.domain import (ClusterUnavailable, InvalidRequest, NotFound, PreconditionFailed,
                            StoreError, ValueTooLarge, WriteOutcomeUnknown)

_STATUS = {
    InvalidRequest: 400,
    NotFound: 404,
    PreconditionFailed: 412,
    ValueTooLarge: 413,
    StoreError: 502,
    ClusterUnavailable: 503,
    WriteOutcomeUnknown: 504,
}

_RETRY_HINT = ("The write may or may not have been applied. Retrying it with the same "
               "Idempotency-Key is safe: that returns the first attempt's result if it "
               "was applied, and applies the write otherwise.")


def problem(status: int, detail: str | None = None, headers: dict | None = None,
            **extra) -> JSONResponse:
    body = {"type": "about:blank", "title": HTTPStatus(status).phrase, "status": status}
    if detail:
        body["detail"] = detail
    body.update(extra)
    return JSONResponse(jsonable_encoder(body), status_code=status, headers=headers,
                        media_type="application/problem+json")


def register(app: FastAPI) -> None:
    for error_type, status in _STATUS.items():
        app.add_exception_handler(error_type, _domain_handler(status))
    app.add_exception_handler(RequestValidationError, _validation_error)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.add_exception_handler(Exception, _unexpected_error)


def _domain_handler(status: int):
    async def handle(request, exc):
        headers, extra = {}, {}
        if isinstance(exc, PreconditionFailed) and exc.current_version:
            # Sending the current version lets the client re-read and retry.
            headers["ETag"] = f'"{exc.current_version}"'
            extra["current_version"] = exc.current_version
        elif isinstance(exc, ClusterUnavailable):
            headers["Retry-After"] = "1"
        elif isinstance(exc, WriteOutcomeUnknown):
            extra["hint"] = _RETRY_HINT
        return problem(status, str(exc), headers or None, **extra)
    return handle


async def _validation_error(request, exc: RequestValidationError):
    return problem(422, "The request is not valid.", errors=exc.errors())


async def _http_error(request, exc: StarletteHTTPException):
    detail = exc.detail if isinstance(exc.detail, str) else None
    return problem(exc.status_code, detail, getattr(exc, "headers", None))


async def _unexpected_error(request, exc: Exception):
    return problem(500, "Unexpected error in the gateway.")
