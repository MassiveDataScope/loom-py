"""Authentication and authorization errors."""

from __future__ import annotations

from collections.abc import Iterable
from typing import ClassVar

from loom.core.errors.base import DomainError
from loom.core.errors.codes import ErrorCode


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
