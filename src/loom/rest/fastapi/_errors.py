"""One error body for the whole API.

FastAPI answers validation failures with its own shape — a list of dicts
carrying ``type``/``loc``/``msg``/``input``/``url`` — while the framework
answers everything else with ``code``/``message``/``trace_id``.  A client
therefore had to parse two formats, the failing *input* was echoed back, and a
link to the Pydantic documentation advertised the internals.

Registering these handlers makes every error of the application look the same,
correlatable by ``trace_id`` and free of internal detail.

Internal module: consumed by :func:`loom.rest.fastapi.app.create_fastapi_app`.
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.requests import Request
from starlette.responses import Response

from loom.core.errors.codes import ErrorCode
from loom.core.tracing import get_trace_id
from loom.rest.errors import ErrorField
from loom.rest.fastapi.response import MsgspecJSONResponse

_VALIDATION_MESSAGE = "Request validation failed"
_FALLBACK_CODE = "http_error"
_BODY_LOCATION = "body"


def register_error_handlers(app: FastAPI) -> None:
    """Normalise validation and HTTP errors to the framework error body.

    Args:
        app: Application whose exception handlers are being configured.
    """
    app.add_exception_handler(RequestValidationError, _handle_validation_error)
    app.add_exception_handler(HTTPException, _handle_http_exception)


async def _handle_validation_error(request: Request, exc: Exception) -> Response:
    """Answer a request-validation failure with the framework error body."""
    del request
    violations = _violations(exc) if isinstance(exc, RequestValidationError) else ()
    return MsgspecJSONResponse(
        status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
        content={
            "detail": {
                ErrorField.CODE: ErrorCode.RULE_VIOLATIONS.value,
                ErrorField.MESSAGE: _VALIDATION_MESSAGE,
                ErrorField.TRACE_ID: get_trace_id(),
                ErrorField.VIOLATIONS: list(violations),
            }
        },
    )


async def _handle_http_exception(request: Request, exc: Exception) -> Response:
    """Answer an ``HTTPException`` with the framework error body."""
    del request
    if not isinstance(exc, HTTPException):  # pragma: no cover - registered per type
        raise exc
    return MsgspecJSONResponse(
        status_code=exc.status_code,
        content={"detail": _detail(exc)},
        headers=dict(exc.headers) if exc.headers else None,
    )


def _detail(exc: HTTPException) -> dict[str, Any]:
    """Return the error body of *exc*, already normalised or wrapped as one."""
    detail = exc.detail
    if isinstance(detail, dict) and ErrorField.CODE in detail:
        return {ErrorField.TRACE_ID: get_trace_id(), **detail}
    return {
        ErrorField.CODE: _code_for(exc.status_code),
        ErrorField.MESSAGE: detail if isinstance(detail, str) else str(detail),
        ErrorField.TRACE_ID: get_trace_id(),
    }


def _code_for(status: int) -> str:
    """Derive a stable machine-readable code from an HTTP status."""
    try:
        return HTTPStatus(status).phrase.lower().replace(" ", "_")
    except ValueError:
        return _FALLBACK_CODE


def _violations(exc: RequestValidationError) -> tuple[dict[str, str], ...]:
    """Project Pydantic errors onto the framework's ``field``/``message`` pairs.

    The offending value and the documentation URL are deliberately dropped:
    echoing input back is a reflection primitive, and the URL advertises which
    validation stack is running.
    """
    return tuple(
        {
            ErrorField.FIELD.value: _field_of(error.get("loc", ())),
            ErrorField.MESSAGE.value: str(error.get("msg", "")),
        }
        for error in exc.errors()
    )


def _field_of(location: Any) -> str:
    """Render a Pydantic error location as a dotted field path."""
    if not isinstance(location, (list, tuple)):
        return str(location)
    parts = [str(part) for part in location if str(part) != _BODY_LOCATION]
    return ".".join(parts) if parts else _BODY_LOCATION
