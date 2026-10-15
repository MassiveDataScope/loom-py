from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from loom.core.errors.codes import ErrorCode


class LoomError(Exception):
    """Base class for all framework errors.

    Carries a machine-readable ``code`` used by transport adapters to
    produce appropriate responses (HTTP status, dead-letter routing, etc.)
    without coupling the domain to any transport layer.

    Args:
        message: Human-readable description of the error.
        code: Machine-readable discriminator (e.g. ``"not_found"``).

    Example::

        raise NotFound("User", id=42)
    """

    def __init__(self, message: str, *, code: str) -> None:
        self.message = message
        self.code = code
        super().__init__(message)


class DomainError(LoomError):
    """Base class for all business-logic errors.

    Raised inside UseCase execution. Transport adapters catch and convert
    these to their own error representations.
    """


class SystemError(LoomError):
    """Base class for infrastructure and unexpected errors.

    Raised when a repository, external service, or internal subsystem
    fails. Transport adapters may retry on SystemError and dead-letter
    on DomainError.

    Args:
        message: Description of the system failure.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.SYSTEM_ERROR)


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


class Unauthenticated(DomainError):
    """Raised when an action requires a verified caller and there is none.

    Distinct from :class:`Forbidden`: the caller can fix an ``Unauthenticated``
    by presenting credentials, but cannot fix a ``Forbidden`` at all.  Transport
    adapters map it to ``401`` with an authentication challenge.

    Args:
        message: Description of the missing authentication.

    Example::

        raise Unauthenticated("This endpoint requires an authenticated caller.")
    """

    def __init__(self, message: str = "Authentication required") -> None:
        super().__init__(message, code=ErrorCode.UNAUTHENTICATED)


class Forbidden(DomainError):
    """Raised when an action is not permitted for the caller.

    Args:
        message: Description of the access restriction.
    """

    def __init__(self, message: str = "Forbidden") -> None:
        super().__init__(message, code=ErrorCode.FORBIDDEN)


class Conflict(DomainError):
    """Raised when an operation conflicts with existing state.

    Args:
        message: Description of the conflicting condition.
    """

    def __init__(self, message: str) -> None:
        super().__init__(message, code=ErrorCode.CONFLICT)


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
