"""Authentication and authorization errors."""

from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar

from loom.core.errors.base import DomainError, InvalidType, InvalidValue, SystemError
from loom.core.errors.codes import ErrorCode


class Unauthenticated(DomainError):
    """Raised when an action requires a verified caller and there is none.

    Distinct from :class:`Forbidden`: the caller can fix an ``Unauthenticated``
    by presenting credentials, but cannot fix a ``Forbidden`` at all.  Transport
    adapters map it to ``401`` with an authentication challenge.

    Args:
        message: Description of the missing authentication.
        code: The subclass's own code; ``unauthenticated`` by default.

    Example::

        raise Unauthenticated("This endpoint requires an authenticated caller.")
    """

    def __init__(
        self,
        message: str = "Authentication required",
        *,
        code: ErrorCode = ErrorCode.UNAUTHENTICATED,
    ) -> None:
        super().__init__(message, code=code)


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
        InvalidType: If ``message`` is not a ``str``, ``max_age`` is not an
            ``int``, ``acr_values`` is a single ``str`` or ``bytes``, or a
            value is not a ``str``, or ``acr_values`` is not iterable.
        InvalidValue: If ``max_age`` is negative, or a value is empty or holds a
            character outside visible ASCII, a double quote or a backslash.

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
            raise InvalidType("InsufficientAuthentication.message must be a str")
        if max_age is not None and type(max_age) is not int:
            raise InvalidType("InsufficientAuthentication.max_age must be an int")
        if max_age is not None and max_age < 0:
            raise InvalidValue("InsufficientAuthentication.max_age must not be negative")
        if isinstance(acr_values, (str, bytes)):
            raise InvalidType(
                "InsufficientAuthentication.acr_values takes an iterable of str values, "
                "not one str or bytes"
            )
        values = tuple(acr_values)
        if not all(isinstance(value, str) for value in values):
            raise InvalidType("InsufficientAuthentication.acr_values items must be str")
        if not all(
            value and all("!" <= char <= "~" and char not in '"\\' for char in value)
            for value in values
        ):
            raise InvalidValue(
                "InsufficientAuthentication.acr_values must be visible ASCII without "
                "whitespace, double quotes or backslashes"
            )
        super().__init__(message, code=ErrorCode.INSUFFICIENT_AUTHENTICATION)
        self.max_age = max_age
        self.acr_values: tuple[str, ...] = values


class DelegationExpired(Unauthenticated):
    """Raised when work acts as a caller past the deadline of its delegation.

    Work a request causes acts as its caller for at most
    ``identity.max_delegation_seconds`` after the request started. The caller
    fixes it by sending a new request; long background work runs as a system
    identity instead.

    Args:
        message: Description of the refusal, with the supported paths.
    """

    default_message: ClassVar[str] = (
        "Authentication required: the delegation of this caller has expired. Work a request "
        "causes acts as its caller for at most identity.max_delegation_seconds after the "
        "request started; run later work as a system identity, switching explicitly with "
        "executor.run(UseCase, identity=...)."
    )

    def __init__(self, message: str = default_message) -> None:
        super().__init__(message)


class MissingCaller(Unauthenticated):
    """Raised when an execution declaring a caller-bound parameter carried no identity.

    Args:
        use_case: Qualified name of the use case.
        parameter: Name of the declaring parameter of ``execute``.
        marker: The marker it declares, as written (``Caller()``, ``Agent()``, ``Mcp()``).
    """

    def __init__(self, use_case: str, parameter: str, marker: str) -> None:
        super().__init__(
            f"{use_case}.execute declares '{parameter} = {marker}' but this execution carried "
            "no identity. The transport must pass identity=... to the executor (pass "
            "ANONYMOUS explicitly to run without a caller)."
        )
        self.use_case = use_case
        self.parameter = parameter
        self.marker = marker


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
        InvalidType: If ``reason`` is not a string, ``missing`` is a single
            ``str`` or ``bytes``, or an item of ``missing`` is not a string.
        InvalidValue: If ``reason`` or an item of ``missing`` is blank.
    """

    def __init__(
        self,
        message: str = "Forbidden",
        *,
        reason: str | None = None,
        missing: Iterable[str] = (),
    ) -> None:
        if reason is not None and not isinstance(reason, str):
            raise InvalidType("Forbidden.reason must be a string or None")
        if reason is not None and not reason.strip():
            raise InvalidValue("Forbidden.reason must be non-blank when given")
        if isinstance(missing, (str, bytes)):
            raise InvalidType(
                "Forbidden.missing takes an iterable of permissions, not one str or bytes"
            )
        permissions = tuple(missing)
        if not all(isinstance(item, str) for item in permissions):
            raise InvalidType("Forbidden.missing items must be strings")
        if any(not item.strip() for item in permissions):
            raise InvalidValue("Forbidden.missing items must be non-blank")
        super().__init__(message, code=ErrorCode.FORBIDDEN)
        self.reason = reason
        self.missing: tuple[str, ...] = permissions


class IdentityFixed(SystemError):
    """Raised by :func:`~loom.core.identity.set_identity` inside work whose identity is fixed.

    An execution fixes its identity when it starts, and a task it started
    keeps it, even one that outlives it. Changing who acts there silently
    would let one request's work act as someone else, so the only way is an
    explicit switch, which opens work of its own and is logged:
    ``executor.run(UseCase, identity=...)``, ``invoker.for_identity(...)``
    or ``ExecutionScope(...)``.

    Args:
        message: Description of the refusal, with the supported switches.
    """

    is_retryable = False
    default_message: ClassVar[str] = (
        "The identity is fixed for the work in progress and every task it started: only an "
        "explicit switch changes it, executor.run(UseCase, identity=...), "
        "invoker.for_identity(...) or ExecutionScope(...), which is logged."
    )

    def __init__(self, message: str = default_message) -> None:
        super().__init__(message)
