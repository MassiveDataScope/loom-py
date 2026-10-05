"""HTTP error mapping for REST endpoints."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, ClassVar

from fastapi import HTTPException

from loom.core.engine.post_commit import PostCommitError
from loom.core.errors import LoomError, NotFound, RuleViolations
from loom.core.errors.codes import ErrorCode
from loom.core.model import BoundaryValidationError
from loom.core.tracing import get_trace_id
from loom.rest.constants import BEARER_CHALLENGE


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


class HttpErrorMapper:
    """Maps ``LoomError`` subclasses to FastAPI ``HTTPException`` instances.

    Uses the ``code`` discriminator on each ``LoomError`` to select the
    appropriate HTTP status code.  The response body always includes
    ``code``, ``message``, and ``trace_id``.  Additional fields are added
    per error type:

    - :class:`~loom.core.errors.NotFound` → ``entity``, ``id``
    - :class:`~loom.core.errors.RuleViolations` → ``violations``
    - :class:`~loom.core.model.BoundaryValidationError` → ``violations``
      (``422``; a schema failure, distinct from a rule failure)
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

    _STATUS: ClassVar[dict[str, int]] = {
        ErrorCode.NOT_FOUND: 404,
        ErrorCode.UNAUTHENTICATED: 401,
        ErrorCode.FORBIDDEN: 403,
        ErrorCode.CONFLICT: 409,
        ErrorCode.RULE_VIOLATIONS: 422,
        ErrorCode.RULE_VIOLATION: 422,
        ErrorCode.BOUNDARY_VALIDATION: 422,
        ErrorCode.UNSUPPORTED_FORMAT: 400,
        ErrorCode.UNSUPPORTED_QUERY: 400,
        ErrorCode.SYSTEM_ERROR: 500,
        ErrorCode.SERVICE_UNAVAILABLE: 503,
        ErrorCode.POST_COMMIT_FAILURE: 500,
    }

    def to_http(self, error: LoomError) -> HTTPException:
        """Convert a ``LoomError`` to an ``HTTPException``.

        Args:
            error: Domain or system error raised by the UseCase pipeline.

        Returns:
            ``HTTPException`` with the appropriate status code and a
            structured detail body keyed by :class:`ErrorField`.  A ``401``
            also carries the ``WWW-Authenticate`` challenge required by
            RFC 9110 §11.6.1.
        """
        status = self._STATUS.get(error.code, 500)
        detail: dict[str, Any] = {
            ErrorField.CODE: error.code,
            ErrorField.MESSAGE: error.message,
            ErrorField.TRACE_ID: get_trace_id(),
        }

        if isinstance(error, NotFound):
            detail[ErrorField.ENTITY] = error.entity
            detail[ErrorField.ID] = error.id

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

        return HTTPException(status_code=status, detail=detail, headers=_challenge(status))


def _challenge(status: int) -> dict[str, str] | None:
    """Return the ``WWW-Authenticate`` header a ``401`` must carry, else ``None``."""
    return dict(BEARER_CHALLENGE) if status == 401 else None
