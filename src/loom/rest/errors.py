"""HTTP error mapping for REST endpoints."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from enum import StrEnum
from http import HTTPStatus
from typing import Any, ClassVar

from fastapi import HTTPException

from loom.core.errors import (
    BoundaryValidationError,
    Forbidden,
    InsufficientAuthentication,
    LoomError,
    NotFound,
    PostCommitError,
    RateLimited,
    RuleViolation,
    RuleViolations,
)
from loom.core.errors.codes import ErrorCode
from loom.core.tracing import get_trace_id
from loom.rest.constants import BEARER_CHALLENGE, RETRY_AFTER_HEADER, WWW_AUTHENTICATE_HEADER

_DESCRIPTION_MAX_LENGTH = 256
_INTERNAL_ERROR_MESSAGE = "An unexpected error occurred"
_UNAVAILABLE_MESSAGE = "The service is temporarily unavailable"
_logger = logging.getLogger(__name__)


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
    ``code``, ``message``, and ``trace_id``.  A ``5xx`` body carries a fixed
    message instead of the error's, which may name hosts, databases or
    stores.  Additional fields are added per error type:

    - :class:`~loom.core.errors.NotFound` → ``entity``, ``id``
    - :class:`~loom.core.errors.Forbidden` → ``reason`` and ``missing``, each only
      when the refusal carries it
    - :class:`~loom.core.errors.RuleViolation` → ``field``
    - :class:`~loom.core.errors.RuleViolations` → ``violations``
    - :class:`~loom.core.errors.BoundaryValidationError` → ``violations``
      (``422``; a schema failure, distinct from a rule failure; ``400`` under
      ``query_validation``, since RFC 9110 §15.5.21 defines ``422`` over
      request content and a query string is not content)
    - :class:`~loom.core.errors.RateLimited` → ``429`` with a ``Retry-After``
      header carrying its whole seconds
    - :class:`~loom.core.errors.InsufficientAuthentication` → ``401`` with the
      RFC 9470 step-up challenge (``error="insufficient_user_authentication"``,
      ``error_description``, and ``max_age`` / ``acr_values`` when given)
    - :class:`~loom.core.errors.PostCommitError` → ``committed``
      (``500``; ``true`` when a unit of work committed, so a retry would
      repeat the write, ``false`` when the execution held none)

    Unknown error codes default to ``500 Internal Server Error``.

    An application error may opt into the answer instead of raising
    ``HTTPException`` from domain code. Set these on the class or the
    instance; leaving them unset keeps the behaviour above:

    - ``http_status``: a 4xx or 5xx status (an :class:`~http.HTTPStatus` or
      its int), or ``None`` to keep the code table. Any other value is a
      misdeclaration.
    - ``public_detail``: extra fields merged into the detail. A key this
      mapper already set (``code``, ``message``, ``trace_id``, and the
      type-specific ones) is a misdeclaration and does not replace it.
    - ``public_message``: when ``True``, a 5xx shows the error's own
      message. The default ``False`` keeps the fixed 5xx text.

    A misdeclaration is logged and answered as ``500`` with the error's
    code, the fixed message and the trace id, and no extra fields.
    :meth:`to_http` does not raise for one.

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
        ErrorCode.FOREIGN_SESSION: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.INVOCATION_SCOPE_FOREIGN_TASK: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.STORES_NOT_COMMITTED: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.UOW_INSIDE_SCOPE: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.UNDECLARED_STORE_WRITE: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.UNCOMPOSED_STORE: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.NESTED_IDENTITY_MISMATCH: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.NESTED_ELEVATION: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.MALFORMED_JOB_MESSAGE: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.UNKNOWN_JOB: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.JOB_TIMEOUT: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.JOB_FAILED: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.JOB_BACKENDS_MIXED: HTTPStatus.INTERNAL_SERVER_ERROR,
        ErrorCode.BOUNDARY_TYPE_UNSUPPORTED: HTTPStatus.INTERNAL_SERVER_ERROR,
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
            the ``Retry-After`` delay of RFC 9110 §10.2.3. A misdeclared
            status or public detail is the fixed internal error instead,
            and this method does not raise for one.
        """
        try:
            return _convert(self, error)
        except _Misdeclared as failure:
            _logger.error(
                "Misdeclared HTTP error %s: %s (trace_id=%s)",
                type(error).__name__,
                failure.problem,
                get_trace_id(),
                exc_info=True,
            )
            return _internal_fallback(error)

    def _status_for(self, error: LoomError) -> HTTPStatus:
        """Return the error's declared status, or the one its code maps to."""
        declared = error.http_status
        if declared is None:
            return self._STATUS.get(error.code, HTTPStatus.INTERNAL_SERVER_ERROR)
        return _coerce_status(declared)


class _Misdeclared(Exception):
    """An error's HTTP declaration cannot be published."""

    def __init__(self, problem: str) -> None:
        self.problem = problem
        super().__init__(problem)


def _convert(mapper: HttpErrorMapper, error: LoomError) -> HTTPException:
    status = mapper._status_for(error)
    detail: dict[str, Any] = {
        ErrorField.CODE: error.code,
        ErrorField.MESSAGE: _public_message(
            status, error.message, reveal=error.public_message is True
        ),
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
        detail[ErrorField.VIOLATIONS] = _rule_violation_fields(error.violations)

    if isinstance(error, BoundaryValidationError):
        detail[ErrorField.VIOLATIONS] = _boundary_violation_fields(error.violations)

    if isinstance(error, PostCommitError):
        detail[ErrorField.COMMITTED] = error.committed

    _add_public_detail(detail, error)
    return HTTPException(status_code=status, detail=detail, headers=_headers(error, status))


def _internal_fallback(error: LoomError) -> HTTPException:
    """Answer a misdeclaration with the fixed internal body and nothing else."""
    return HTTPException(
        status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
        detail={
            ErrorField.CODE: error.code,
            ErrorField.MESSAGE: _INTERNAL_ERROR_MESSAGE,
            ErrorField.TRACE_ID: get_trace_id(),
        },
    )


def _coerce_status(declared: object) -> HTTPStatus:
    """Return *declared* as a 4xx or 5xx status.

    A bare int is accepted and normalised with :class:`HTTPStatus`, so
    ``401`` is :attr:`HTTPStatus.UNAUTHORIZED`. ``bool`` is not a status,
    and neither is an unassigned code.
    """
    if isinstance(declared, int) and declared >= HTTPStatus.BAD_REQUEST:
        try:
            return HTTPStatus(declared)
        except ValueError:
            pass
    raise _Misdeclared("http_status is not a registered 4xx or 5xx status")


def _rule_violation_fields(violations: tuple[RuleViolation, ...]) -> list[dict[str, Any]]:
    return [
        {ErrorField.FIELD: violation.field, ErrorField.MESSAGE: violation.message}
        for violation in violations
    ]


def _boundary_violation_fields(violations: tuple[tuple[str, str], ...]) -> list[dict[str, Any]]:
    return [{ErrorField.FIELD: field, ErrorField.MESSAGE: message} for field, message in violations]


def _add_public_detail(detail: dict[str, Any], error: LoomError) -> None:
    """Merge ``public_detail`` into *detail*.

    Owned keys are whatever this response already carries, including a
    type-specific field such as ``entity``. A public field must not replace
    one of them.
    """
    try:
        extra = error.public_detail()
    except Exception as exc:  # recovery: an application hook failed; answered as misdeclared
        raise _Misdeclared(f"public_detail raised {type(exc).__name__}") from None
    if not isinstance(extra, Mapping):
        raise _Misdeclared("public_detail did not return a mapping")
    overlap = _owned_overlap(detail, extra)
    if overlap:
        names = ", ".join(sorted(overlap))
        raise _Misdeclared(f"public_detail overrides a field the mapper owns: {names}")
    detail.update(extra)


def _owned_overlap(detail: Mapping[str, Any], extra: Mapping[str, Any]) -> set[str]:
    """Keys *extra* would replace. Compared as text, so enum keys match."""
    return set(detail) & set(extra)


def _public_message(status: int, message: str, *, reveal: bool = False) -> str:
    """Return *message*, or the fixed one a ``5xx`` answers with instead.

    *reveal* is the error's ``public_message`` opt-in. A ``5xx`` shows
    *message* only when it is true; the default keeps the fixed text.
    """
    if reveal or status < HTTPStatus.INTERNAL_SERVER_ERROR:
        return message
    if status == HTTPStatus.SERVICE_UNAVAILABLE:
        return _UNAVAILABLE_MESSAGE
    return _INTERNAL_ERROR_MESSAGE


def _headers(error: LoomError, status: HTTPStatus) -> dict[str, str] | None:
    if isinstance(error, InsufficientAuthentication):
        return {WWW_AUTHENTICATE_HEADER: _step_up_challenge(error)}
    if status is HTTPStatus.UNAUTHORIZED:
        return dict(BEARER_CHALLENGE)
    if isinstance(error, RateLimited):
        return {RETRY_AFTER_HEADER: str(error.retry_after)}
    return None


def _step_up_challenge(error: InsufficientAuthentication) -> str:
    """Build the RFC 9470 §3 challenge.

    The description replaces each character RFC 6750 §3 does not allow in it
    with a space and collapses runs of spaces; an empty result falls back to
    the default message. It is cut at ``_DESCRIPTION_MAX_LENGTH`` characters.
    """
    allowed = "".join(
        char if " " <= char <= "~" and char not in '"\\' else " " for char in error.message
    )
    description = (
        " ".join(allowed.split())[:_DESCRIPTION_MAX_LENGTH].rstrip()
        or InsufficientAuthentication.default_message
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
