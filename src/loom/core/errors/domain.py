"""Domain errors raised from use-case execution."""

from __future__ import annotations

import math
from collections.abc import Sequence
from typing import Any

from loom.core.errors.base import DomainError
from loom.core.errors.codes import ErrorCode


class NotFound(DomainError):
    """Raised when a requested entity does not exist.

    Emitted automatically by ``LoadStep`` when a repository returns
    ``None``. May also be raised explicitly inside ``execute`` for
    semantic not-found conditions.

    Args:
        entity: Name of the missing entity type (e.g. ``"User"``).
        id: Lookup key that yielded no result.

    Example::

        raise NotFound("User", id=42)
    """

    def __init__(self, entity: str, *, id: Any) -> None:
        self.entity = entity
        self.id = id
        super().__init__(f"{entity} with id={id!r} not found", code=ErrorCode.NOT_FOUND)


class Conflict(DomainError):
    """Raised when an operation conflicts with existing state.

    Args:
        message: Description of the conflicting condition.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.CONFLICT)


class RateLimited(DomainError):
    """Raised when the caller spent its budget and must wait before retrying.

    loom ships no limiter: the consumer decides who is limited and how much,
    and raises this when a caller exceeds it.

    Args:
        retry_after: Seconds the caller should wait. Kept as whole seconds,
            rounded up and at least ``1``.
        message: Human-readable description of the refusal.

    Raises:
        ValueError: ``retry_after`` is not finite.

    Example::

        raise RateLimited(retry_after=2.0)
    """

    def __init__(self, retry_after: float, message: str = "Too many requests.") -> None:
        if not math.isfinite(retry_after):
            raise ValueError(f"RateLimited.retry_after must be finite, got {retry_after!r}")
        super().__init__(message, code=ErrorCode.RATE_LIMITED)
        self.retry_after = max(1, math.ceil(retry_after))


BODY_FIELD = "body"
"""Field a violation names when it concerns the request body as a whole."""


class RuleViolation(DomainError):
    """Raised when a single business rule is violated.

    Args:
        field: The field that caused the violation.
        message: Human-readable description of the violation.

    Example::

        raise RuleViolation("email", "Disposable emails not allowed")
    """

    def __init__(self, field: str, message: str) -> None:
        self.field = field
        # Override message attr with just the violation message (not field-prefixed)
        # so adapters can access field and message independently.
        super().__init__(f"{field}: {message}", code=ErrorCode.RULE_VIOLATION)
        self.message = message


class RuleViolations(DomainError):
    """Raised when one or more business rules are violated.

    Accumulates all violations from rule evaluation instead of failing
    fast on the first violation.

    Args:
        violations: Sequence of individual rule violations.
    """

    def __init__(self, violations: Sequence[RuleViolation]) -> None:
        self.violations: tuple[RuleViolation, ...] = tuple(violations)
        messages = "; ".join(str(v) for v in self.violations)
        super().__init__(messages, code=ErrorCode.RULE_VIOLATIONS)


class UnsupportedQuery(DomainError):
    """Raised when a backend cannot serve a query because of its arguments.

    A capability the repository class does not declare is absent from DI and
    from the generated routes; this error covers the gaps that depend on a
    run-time argument instead, such as a lookup on a field the backend can
    only reach through a scan.

    The message names the model and the reason, never the backend: a client
    reads it, and which store serves a model is the server's business.
    ``backend`` and ``model`` stay on the error for the server log.

    Args:
        reason: What cannot be done with the given arguments.
        backend: Name of the persistence backend, when known where it is raised.
        model: Qualified name of the model being queried, when known.
    """

    def __init__(
        self, reason: str, *, backend: str | None = None, model: str | None = None
    ) -> None:
        self.backend = backend
        self.model = model
        self.reason = reason
        on_model = "" if model is None else f" on {model}"
        super().__init__(
            f"The query cannot be served{on_model}: {reason}", code=ErrorCode.UNSUPPORTED_QUERY
        )
