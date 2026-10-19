from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from typing import Any, ClassVar

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


class InsufficientAuthentication(Unauthenticated):
    """Raised when the caller is authenticated, but not strongly or recently enough.

    The caller can fix it by authenticating again under the stated
    requirements (step-up).

    Args:
        message: Description of the missing authentication strength.
        max_age: Seconds within which the caller must have authenticated;
            ``None`` when recency is not the issue.
        acr_values: Authentication context classes acceptable for a retry,
            in order of preference; empty when any will do.

    Raises:
        ValueError: If ``message`` is not a ``str``, ``max_age`` is not a
            non-negative ``int``, ``acr_values`` is a single ``str`` or
            ``bytes``, or a value is not a ``str``, is empty, or holds a
            character outside visible ASCII, a double quote or a backslash.
        TypeError: If ``acr_values`` is not iterable.

    Example::

        raise InsufficientAuthentication(max_age=300, acr_values=("urn:mfa",))
    """

    default_message: ClassVar[str] = "A different authentication level is required"

    def __init__(
        self,
        message: str = default_message,
        *,
        max_age: int | None = None,
        acr_values: Iterable[str] = (),
    ) -> None:
        if not isinstance(message, str):
            raise ValueError("InsufficientAuthentication.message must be a str")
        if max_age is not None and (type(max_age) is not int or max_age < 0):
            raise ValueError("InsufficientAuthentication.max_age must be a non-negative int")
        if isinstance(acr_values, (str, bytes)):
            raise ValueError(
                "InsufficientAuthentication.acr_values takes an iterable of str values, "
                "not one str or bytes"
            )
        values = tuple(acr_values)
        if not all(
            isinstance(value, str)
            and value
            and all("!" <= char <= "~" and char not in '"\\' for char in value)
            for value in values
        ):
            raise ValueError(
                "InsufficientAuthentication.acr_values must be visible ASCII without "
                "whitespace, double quotes or backslashes"
            )
        super().__init__(message)
        self.code = ErrorCode.INSUFFICIENT_AUTHENTICATION
        self.max_age = max_age
        self.acr_values: tuple[str, ...] = values


class Forbidden(DomainError):
    """Raised when an action is not permitted for the caller.

    Args:
        message: Description of the access restriction.
        reason: Optional machine-readable cause a client can act on, such as
            ``"mfa_required"``. ``None`` when the refusal has no specific cause.
        missing: The permissions the caller lacks, in the order given; empty when
            the authorizer does not tell them.

    Both are keyword-only and public: the REST adapter puts them in the 403 body.
    Never put token claims, tenant data or anything the caller may not learn in them;
    leave them out where the refusal must not become an oracle for the policy.

    Raises:
        ValueError: If ``reason`` is not a non-blank string, ``missing`` is a single
            ``str`` or ``bytes``, or an item of ``missing`` is not a non-blank string.
    """

    def __init__(
        self,
        message: str = "Forbidden",
        *,
        reason: str | None = None,
        missing: Iterable[str] = (),
    ) -> None:
        if reason is not None and (not isinstance(reason, str) or not reason.strip()):
            raise ValueError("Forbidden.reason must be a non-blank string or None")
        if isinstance(missing, (str, bytes)):
            raise ValueError(
                "Forbidden.missing takes an iterable of permissions, not one str or bytes"
            )
        permissions = tuple(missing)
        if any(not isinstance(item, str) or not item.strip() for item in permissions):
            raise ValueError("Forbidden.missing items must be non-blank strings")
        super().__init__(message, code=ErrorCode.FORBIDDEN)
        self.reason = reason
        self.missing: tuple[str, ...] = permissions


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
