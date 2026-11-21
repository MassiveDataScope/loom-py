"""One error body for the whole API.

FastAPI answers validation failures with its own shape — a list of dicts
carrying ``type``/``loc``/``msg``/``input``/``url`` — while the framework
answers everything else with ``code``/``message``/``trace_id``.  A client
therefore had to parse two formats, the failing *input* was echoed back, and a
link to the Pydantic documentation advertised the internals.

Registering these handlers makes every error of the application look the same,
correlatable by ``trace_id`` and free of internal detail.  The module also
builds the ``400`` the router runtime and the SQL endpoint raise for a malformed
body and the generic ``500`` they return for an unexpected failure.

Internal module: consumed by :func:`loom.rest.fastapi.app.create_fastapi_app`,
the router runtime and the SQL endpoint.
"""

from __future__ import annotations

import logging
from http import HTTPStatus
from typing import Any, Final, cast

from fastapi import HTTPException
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from starlette.applications import Starlette
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ExceptionHandler

from loom.core.errors import BODY_FIELD, BoundaryValidationError
from loom.core.errors.boundary import PassThrough, boundary
from loom.core.errors.codes import ErrorCode
from loom.core.errors.failure import INTERNAL_ERROR_MESSAGE, Failure
from loom.core.tracing import get_trace_id
from loom.rest._body import malformed_body_detail
from loom.rest.errors import ErrorField, HttpErrorMapper
from loom.rest.fastapi.response import MsgspecJSONResponse

_VALIDATION_MESSAGE = "Request validation failed"
_INTERNAL_ERROR_CODE = "internal_error"
_FALLBACK_CODE = "http_error"
_BODY_LOCATION = "body"

_logger = logging.getLogger(__name__)
_error_mapper = HttpErrorMapper()

STARLETTE_PASS_THROUGH: Final[PassThrough] = (StarletteHTTPException,)
"""Starlette's control flow, an ``HTTPException``: it crosses the ASGI boundary as it is."""


def internal_error_response() -> MsgspecJSONResponse:
    """Return the generic ``500`` that hides an unexpected failure's details.

    It carries the active trace id, or ``""`` when there is none.
    """
    return MsgspecJSONResponse(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        content={
            ErrorField.CODE: _INTERNAL_ERROR_CODE,
            ErrorField.MESSAGE: INTERNAL_ERROR_MESSAGE,
            ErrorField.TRACE_ID: get_trace_id() or "",
        },
    )


def malformed_body_error() -> HTTPException:
    """Return the ``400`` for a request body that is not well-formed JSON."""
    return HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=malformed_body_detail())


def register_error_handlers(app: Starlette) -> None:
    """Normalise validation and HTTP errors to the framework error body.

    Starlette's own ``HTTPException`` (the routing ``404`` and ``405``) answers
    with FastAPI's default handler, ``Allow`` header included, on a bare
    Starlette application too.

    Args:
        app: Application whose exception handlers are being configured.
    """
    app.add_exception_handler(RequestValidationError, boundary(_handle_validation_error))
    app.add_exception_handler(HTTPException, boundary(_handle_http_exception))
    app.add_exception_handler(
        StarletteHTTPException, boundary(cast(ExceptionHandler, http_exception_handler))
    )


async def _handle_validation_error(request: Request, exc: Exception) -> Response:
    """Answer a request-validation failure with the body the error mapper writes."""
    violations = _violations(exc) if isinstance(exc, RequestValidationError) else ()
    error = BoundaryValidationError(_VALIDATION_MESSAGE, violations, code=ErrorCode.RULE_VIOLATIONS)
    return await _handle_http_exception(request, _error_mapper.to_http(error))


async def _handle_http_exception(request: Request, exc: Exception) -> Response:
    """Answer an ``HTTPException`` with the framework error body."""
    del request
    if not isinstance(exc, HTTPException):  # pragma: no cover
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
    message = detail if isinstance(detail, str) else str(detail)
    if exc.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
        _logger.error("HTTP %d (trace_id=%s): %s", exc.status_code, get_trace_id(), message)
    return {
        ErrorField.CODE: _code_for(exc.status_code),
        ErrorField.MESSAGE: Failure.fixed_message(exc.status_code) or message,
        ErrorField.TRACE_ID: get_trace_id(),
    }


def _code_for(status: int) -> str:
    """Derive a stable machine-readable code from an HTTP status."""
    try:
        return HTTPStatus(status).phrase.lower().replace(" ", "_")
    except ValueError:
        return _FALLBACK_CODE


def _violations(exc: RequestValidationError) -> tuple[tuple[str, str], ...]:
    """Project Pydantic errors onto ``(field, message)`` pairs.

    The offending value and the documentation URL are deliberately dropped:
    echoing input back is a reflection primitive, and the URL advertises which
    validation stack is running.
    """
    return tuple(
        (_field_of(error.get("loc", ())), str(error.get("msg", ""))) for error in exc.errors()
    )


def _field_of(location: Any) -> str:
    """Render a Pydantic error location as a dotted field path."""
    if not isinstance(location, (list, tuple)):
        return str(location)
    parts = [str(part) for part in location if str(part) != _BODY_LOCATION]
    return ".".join(parts) if parts else BODY_FIELD
