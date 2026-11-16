"""SQL subsystem errors built on the existing domain error hierarchy.

Inheriting from the existing domain types keeps the HTTP mapping unchanged:
:class:`~loom.core.errors.NotFound` maps to 404,
:class:`~loom.core.errors.Forbidden` to 403 and
:class:`~loom.core.errors.RuleViolation` to 422 through the current
``HttpErrorMapper`` without touching the public error API.
"""

from __future__ import annotations

from loom.core.errors import Forbidden, NotFound, RuleViolation


class UnknownConnectionError(NotFound):
    """Raised when a SQL connection name is not configured.

    Args:
        connection: Name of the missing connection.
    """

    def __init__(self, connection: str) -> None:
        super().__init__("SqlConnection", id=connection)


class RoleNotAllowedError(Forbidden):
    """Raised when the caller role is not in the connection allowlist.

    Without a configured allowlist every caller-provided role is rejected
    (fail-closed policy).

    Args:
        role: Role requested by the caller.
        connection: Name of the connection that rejected the role.
    """

    def __init__(self, role: str, *, connection: str) -> None:
        super().__init__(f"Role {role!r} is not allowed for SQL connection {connection!r}")


class RoleRequiredError(Forbidden):
    """Raised when no effective role can be resolved for a query.

    A query never runs with the full default roles of the connection user:
    without a caller role and without ``default_role`` execution is refused.

    Args:
        connection: Name of the connection lacking an effective role.
    """

    def __init__(self, connection: str) -> None:
        super().__init__(
            f"SQL connection {connection!r} requires a role: the request carries "
            "no role and the connection defines no 'default_role'"
        )


class RolesNotBoundError(Forbidden):
    """Raised when the verified identity yields no allowed role.

    Covers every fail-closed outcome of identity binding — no verified claims,
    a missing, empty or wrongly typed roles claim, or an empty intersection
    with the connection allowlist. The message is deliberately uniform so the
    response never reveals which part of the token failed; the precise reason
    is logged server-side.

    Args:
        connection: Name of the connection that refused the request.
    """

    def __init__(self, connection: str) -> None:
        super().__init__(
            f"SQL connection {connection!r} derives its roles from the verified "
            "identity, which carries no allowed role for this connection"
        )


class SqlExecutionError(RuleViolation):
    """Raised when the backend rejects a SQL statement.

    Carries a sanitized message (backend error code plus first line, without
    host, DSN or stack trace).

    Args:
        message: Sanitized backend rejection message.
    """

    def __init__(self, message: str) -> None:
        super().__init__("sql", message)
