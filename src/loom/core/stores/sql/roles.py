"""Identity-bound role resolution of the SQL pillar.

The effective roles of a query are derived from the VERIFIED identity the
authentication middleware published, never from the request body.  The body may
only narrow the resulting set.  Resolution is fail-closed: an anonymous caller
or one holding no allowlisted role is denied, and ``default_role`` is never a
fallback once a connection binds roles to identity.

This module knows nothing about tokens or claims: whichever mechanism
authenticated the caller, it sees the same
:class:`~loom.core.identity.identity.Identity`.

Public API of the SQL pillar: any layer that executes caller-scoped queries
(the REST endpoint, the agent compiler's SQL capability at run time) resolves
the effective roles through this single function.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from enum import StrEnum
from typing import NoReturn

from loom.core.identity import Identity
from loom.core.log_names import LogField
from loom.core.stores.event import StoresEvent, StoresKey
from loom.core.stores.sql.abc.errors import RoleNotAllowedError, RolesNotBoundError

_logger = logging.getLogger(__name__)


class _Denial(StrEnum):
    """Why a request was denied; recorded server-side only."""

    NO_IDENTITY = "no_identity"
    NO_ROLE = "no_role"
    NO_ALLOWLISTED_ROLE = "no_allowlisted_role"


def resolve_query_roles(
    identity: Identity,
    *,
    connection: str,
    roles_bound: bool,
    allowed_roles: frozenset[str],
    requested_roles: Sequence[str] | None,
) -> tuple[str, ...]:
    """Resolve the roles one query may use.

    Args:
        identity: Verified caller published by the authentication middleware.
        connection: Name of the SQL connection being queried.
        roles_bound: Whether the configured authentication mechanism binds
            roles to the identity.  ``False`` means the connection declares no
            binding and is single-role by config.
        allowed_roles: Connection allowlist, the ceiling of the intersection.
        requested_roles: Roles asked for in the body; they may only narrow.

    Returns:
        The effective roles for this single request.

    Raises:
        RolesNotBoundError: When no allowed role can be derived from the
            verified identity.
        RoleNotAllowedError: When the body asks for a role the identity does
            not hold.
    """
    if not roles_bound:
        return tuple(requested_roles or ())

    authorized = _authorized_roles(identity, connection=connection, allowed_roles=allowed_roles)
    return _narrow(authorized, requested_roles, connection)


def _authorized_roles(
    identity: Identity,
    *,
    connection: str,
    allowed_roles: frozenset[str],
) -> tuple[str, ...]:
    """Intersect the roles the identity holds with the allowlist, fail-closed."""
    if not identity.is_authenticated:
        _deny(connection, identity, _Denial.NO_IDENTITY)
    if not identity.roles:
        _deny(connection, identity, _Denial.NO_ROLE)
    authorized = tuple(role for role in identity.roles if role in allowed_roles)
    if not authorized:
        _deny(connection, identity, _Denial.NO_ALLOWLISTED_ROLE)
    return authorized


def _narrow(
    authorized: tuple[str, ...],
    requested: Sequence[str] | None,
    connection: str,
) -> tuple[str, ...]:
    """Apply the body narrowing: a subset of *authorized*, never a widening."""
    if not requested:
        return authorized
    for role in requested:
        if role not in authorized:
            raise RoleNotAllowedError(role, connection=connection)
    return tuple(dict.fromkeys(requested))


def _deny(connection: str, identity: Identity, reason: _Denial) -> NoReturn:
    """Log the audit trail of a denial and refuse the request.

    The response message stays generic on purpose (no oracle about which part
    of the credentials failed); the precise reason is only recorded server-side.
    """
    _logger.warning(
        StoresEvent.ROLE_AUTHORIZATION_DENIED,
        extra={
            StoresKey.CONNECTION: connection,
            StoresKey.SUBJECT: identity.subject,
            StoresKey.MECHANISM: identity.mechanism,
            LogField.REASON: reason,
        },
    )
    raise RolesNotBoundError(connection)
