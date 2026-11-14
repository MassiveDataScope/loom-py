"""Shared SQL role-binding rule for every ``sql`` capability caller.

The model-facing ``sql`` tool
(:mod:`loom.ai.engines.pydantic_ai._capabilities`) and the marker-facing
``sql`` grant view (:mod:`loom.ai.runtime._grants`) resolve the caller's
roles through the exact same rule: roles are bound to the verified identity
— never to a caller-supplied argument — and an empty result is refused
rather than silently falling back to the connection's shared ``default_role``. The rule
lives here once, the way :mod:`loom.ai._filters` holds the include/exclude rule both
paths also share, so the two callers cannot drift apart.
"""

from __future__ import annotations

from loom.ai.compiler import CompiledSqlCapability
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.identity import Identity
from loom.core.sql.abc import RoleNotAllowedError, RolesNotBoundError
from loom.core.sql.roles import resolve_query_roles


def bound_query_roles(capability: CompiledSqlCapability, identity: Identity) -> tuple[str, ...]:
    """Resolve one caller's roles on one granted connection, bound and non-empty.

    ``roles_bound`` is hard-coded ``True`` and the result is re-checked for
    emptiness: an empty tuple reaching ``SqlQueryService.execute`` would fall
    through to the connection's shared ``default_role``, which this
    function refuses instead.

    Args:
        capability: The compiled ``sql`` capability the caller was granted.
        identity: Verified caller whose roles are resolved.

    Returns:
        The caller's roles allowlisted on this connection, never empty.

    Raises:
        AgentRunError: With ``UNAUTHORIZED`` when the caller carries no role
            bound to this connection, or none of its roles is allowlisted.
    """
    try:
        roles = resolve_query_roles(
            identity,
            connection=capability.connection,
            roles_bound=True,
            allowed_roles=frozenset(capability.config.allowed_roles),
            requested_roles=None,
        )
    except (RolesNotBoundError, RoleNotAllowedError) as exc:
        raise AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED,
            f"the caller may not query the {capability.connection!r} connection",
        ) from exc
    if not roles:
        raise AgentRunError(
            AgentRunErrorCode.UNAUTHORIZED,
            f"no role of the caller is allowlisted on the {capability.connection!r} connection",
        )
    return roles


__all__ = ["bound_query_roles"]
