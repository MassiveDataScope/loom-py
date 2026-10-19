"""HTTP error mapping for REST endpoints."""

from __future__ import annotations

from enum import StrEnum
from http import HTTPStatus
from typing import Any, ClassVar

from fastapi import HTTPException

from loom.core.engine.post_commit import PostCommitError
from loom.core.errors import (
    Forbidden,
    InsufficientAuthentication,
    LoomError,
    NotFound,
    RateLimited,
    RuleViolation,
    RuleViolations,
)
from loom.core.errors.codes import ErrorCode
from loom.core.model import BoundaryValidationError
from loom.core.tracing import get_trace_id
from loom.rest.constants import BEARER_CHALLENGE, RETRY_AFTER_HEADER, WWW_AUTHENTICATE_HEADER


class ErrorField(StrEnum):
    """Keys used in all HTTP error response bodies.

    Using ``StrEnum`` guarantees JSON serialisation produces plain strings
    while keeping references typo-proof and IDE-navigable.

    Example response body::

        {
            "code": "not_found",
            "message": "User with id=42 not found",
            "entity": "User",
            "id": 42,
            "trace_id": "abc-123"
        }
    """

    CODE = "code"
    MESSAGE = "message"
    TRACE_ID = "trace_id"
    ENTITY = "entity"
    ID = "id"
    VIOLATIONS = "violations"
    FIELD = "field"
    COMMITTED = "committed"
    REASON = "reason"
    MISSING = "missing"


class HttpErrorMapper:
    """Maps ``LoomError`` subclasses to FastAPI ``HTTPException`` instances.

    Uses the ``code`` discriminator on each ``LoomError`` to select the
    appropriate HTTP status code.  The response body always includes
    ``code``, ``message``, and ``trace_id``.  Additional fields are added
    per error type:

    - :class:`~loom.core.errors.NotFound` → ``entity``, ``id``
    - :class:`~loom.core.errors.Forbidden` → ``reason`` and ``missing``, each only
      when the refusal carries it
    - :class:`~loom.core.errors.RuleViolation` → ``field``
    - :class:`~loom.core.errors.RuleViolations` → ``violations``
    - :class:`~loom.core.model.BoundaryValidationError` → ``violations``
      (``422``; a schema failure, distinct from a rule failure; ``400`` under
      ``query_validation``, since RFC 9110 §15.5.21 defines ``422`` over
      request content and a query string is not content)
    - :class:`~loom.core.errors.RateLimited` → ``429`` with a ``Retry-After``
      header carrying its whole seconds
    - :class:`~loom.core.engine.post_commit.PostCommitError` → ``committed``
      (``500``; ``true`` when a unit of work committed, so a retry would
      repeat the write, ``false`` when the execution held none)

    Unknown error codes default to ``500 Internal Server Error``.

    Example::

        mapper = HttpErrorMapper()
        try:
            ...
        except LoomError as exc:
            raise mapper.to_http(exc) from exc
    """

    _STATUS: ClassVar[dict[str, HTTPStatus]] = {
        ErrorCode.NOT_FOUND: HTTPStatus.NOT_FOUND,
        ErrorCode.UNAUTHENTICATED: HTTPStatus.UNAUTHORIZED,
        ErrorCode.INSUFFICIENT_AUTHENTICATION: HTTPStatus.UNAUTHORIZED,
        ErrorCode.FORBIDDEN: HTTPStatus.FORBIDDEN,
        ErrorCode.CONFLICT: HTTPStatus.CONFLICT,
        ErrorCode.RULE_VIOLATIONS: HTTPStatus.UNPROCESSABLE_ENTITY,
        ErrorCode.RULE_VIOLATION: HTTPStatus.UNPROCESSABLE_ENTITY,
        ErrorCode.BOUNDARY_VALIDATION: HTTPStatus.UNPROCESSABLE_ENTITY,
        ErrorCode.QUERY_VALIDATION: HTTPStatus.BAD_REQUEST,
        ErrorCode.UNSUPPORTED_FORMAT: HTTPStatus.BAD_REQUEST,
        ErrorCode.UNSUPPORTED_QUERY: HTTPStatus.BAD_REQUEST,
        ErrorCode.RATE_LIMITED: HTTPStatus.TOO_MANY_REQUESTS,
        ErrorCode.SYSTEM_ERROR: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.SERVICE_UNAVAILABLE: HTTPStatus.SERVICE_UNAVAILABLE,
        ErrorCode.POST_COMMIT_FAILURE: HTTPStatus.INTERNAL_SERVER_ERROR,
    }

    def to_http(self, error: LoomError) -> HTTPException:
        """Convert a ``LoomError`` to an ``HTTPException``.

        Args:
            error: Domain or system error raised by the UseCase pipeline.

        Returns:
            ``HTTPException`` with the appropriate status code and a
            structured detail body keyed by :class:`ErrorField`.  A ``401``
            also carries the ``WWW-Authenticate`` challenge required by
            RFC 9110 §11.6.1, and a :class:`~loom.core.errors.RateLimited`
            the ``Retry-After`` delay of RFC 9110 §10.2.3.
        """
        status = self._STATUS.get(error.code, HTTPStatus.INTERNAL_SERVER_ERROR)
        detail: dict[str, Any] = {
            ErrorField.CODE: error.code,
            ErrorField.MESSAGE: error.message,
            ErrorField.TRACE_ID: get_trace_id(),
        }

        if isinstance(error, NotFound):
            detail[ErrorField.ENTITY] = error.entity
            detail[ErrorField.ID] = error.id

        if isinstance(error, Forbidden):
            if error.reason is not None:
                detail[ErrorField.REASON] = error.reason
            if error.missing:
                detail[ErrorField.MISSING] = list(error.missing)

        if isinstance(error, RuleViolation):
            detail[ErrorField.FIELD] = error.field

        if isinstance(error, RuleViolations):
            detail[ErrorField.VIOLATIONS] = [
                {ErrorField.FIELD: v.field, ErrorField.MESSAGE: v.message} for v in error.violations
            ]

        if isinstance(error, BoundaryValidationError):
            detail[ErrorField.VIOLATIONS] = [
                {ErrorField.FIELD: field, ErrorField.MESSAGE: message}
                for field, message in error.violations
            ]

        if isinstance(error, PostCommitError):
            detail[ErrorField.COMMITTED] = error.committed

        return HTTPException(status_code=status, detail=detail, headers=_headers(error, status))


def _headers(error: LoomError, status: HTTPStatus) -> dict[str, str] | None:
    if isinstance(error, InsufficientAuthentication):
        return {WWW_AUTHENTICATE_HEADER: _step_up_challenge(error)}
    if status is HTTPStatus.UNAUTHORIZED:
        return dict(BEARER_CHALLENGE)
    if isinstance(error, RateLimited):
        return {RETRY_AFTER_HEADER: str(error.retry_after)}
    return None


def _step_up_challenge(error: InsufficientAuthentication) -> str:
    """Build the RFC 9470 §3 challenge; the description keeps only RFC 6750 §3 characters."""
    description = "".join(
        char for char in error.message if " " <= char <= "~" and char not in '"\\'
    )
    params = [
        'error="insufficient_user_authentication"',
        f'error_description="{description}"',
    ]
    if error.max_age is not None:
        params.append(f'max_age="{error.max_age}"')
    if error.acr_values:
        params.append(f'acr_values="{" ".join(error.acr_values)}"')
    return f"{BEARER_CHALLENGE[WWW_AUTHENTICATE_HEADER]} {', '.join(params)}"
