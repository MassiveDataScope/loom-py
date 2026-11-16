"""Caller-bound SQL execution: the roles come from the verified identity.

:class:`~loom.core.stores.sql.service.SqlQueryService` takes the roles as an argument,
so the code that writes the call chooses them. That is the right shape for
system work with no caller, and the wrong one for work done *on behalf of* a
caller: within the connection allowlist, a use case could then query with roles
its caller does not hold.

This module gives that second case its own collaborator. It resolves the
effective roles through :func:`~loom.core.stores.sql.roles.resolve_query_roles`, the
same function the agent capability and the REST endpoint use, and exposes no
way to pass roles in.

The identity it is handed must be declared with the
:func:`~loom.core.use_case.markers.Caller` marker, which the executor fills
from the identity the transport verified::

    async def execute(self, identity: Identity = Caller()) -> SqlQueryResult:

An ``identity`` parameter *without* that marker is a plain primitive parameter:
the compiler binds it from the ``params`` the calling code supplies, so it is
whatever that code says it is — forgeable, and, on the agent path, an argument
the model itself fills in. Binding the roles to a forged identity binds them to
nothing.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractContextManager, nullcontext
from typing import Any

from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.stores.sql.abc.contracts import SqlQueryResult
from loom.core.stores.sql.abc.errors import RolesNotBoundError
from loom.core.stores.sql.config import SqlConfig
from loom.core.stores.sql.roles import resolve_query_roles
from loom.core.stores.sql.service import SqlQueryService
from loom.core.tracing import get_trace_id


class CallerBoundSql:
    """Executes SQL with the roles of the verified caller, and only those.

    Every query is bound to an :class:`~loom.core.identity.identity.Identity`:
    the effective roles are the intersection of the roles that identity holds
    with the connection allowlist. There is deliberately no ``roles``
    parameter — accepting one would reintroduce the very gap this collaborator
    closes, since the caller's entitlements would stop being what decides.

    **The identity must come from** :func:`~loom.core.use_case.markers.Caller`.
    That marker is the only thing that makes the executor inject the identity
    the transport verified. An ``identity`` parameter declared without it is an
    ordinary primitive parameter, bound from the ``params`` the calling code
    supplies: the caller would then choose its own identity, and with it the
    roles this class derives. On the agent path it is worse still, because
    those ``params`` are tool arguments the model writes. A query bound to a
    forged identity is unbound.

    A connection with an empty ``allowed_roles`` cannot be queried this way: its
    only role is the shared ``default_role``, which is not derived from anyone,
    so :class:`~loom.core.stores.sql.abc.errors.RolesNotBoundError` is raised. Use
    :class:`~loom.core.stores.sql.service.SqlQueryService` for that unbound work.

    Each accepted query opens one span labelled with the effective roles, the
    caller subject and the authentication mechanism. Those labels match what
    the REST endpoint records, but the span is its own: a narrower scope and a
    different name, so the two are auditable on the same terms rather than
    being one record. Without an observability runtime there is no span and no
    other difference.

    Args:
        service: Underlying query service applying the connection policy.
        config: Parsed ``sql:`` section holding the per-connection allowlists.
        observability: Runtime the audit span is opened on. ``None`` disables
            the span; the auto-bootstrapped app injects the registered runtime.

    Example::

        from loom.core.identity import Identity
        from loom.core.stores.sql import CallerBoundSql, SqlQueryResult
        from loom.core.use_case.markers import Caller
        from loom.core.use_case.use_case import UseCase


        class ListSales(UseCase[object, SqlQueryResult]):
            def __init__(self, sql: CallerBoundSql) -> None:
                self._sql = sql

            async def execute(self, identity: Identity = Caller()) -> SqlQueryResult:
                return await self._sql.execute(
                    "SELECT * FROM sales", connection="analytics", identity=identity
                )
    """

    def __init__(
        self,
        service: SqlQueryService,
        config: SqlConfig,
        observability: ObservabilityRuntime | None = None,
    ) -> None:
        self._service = service
        self._observability = observability
        self._allowed_roles: dict[str, frozenset[str]] = {
            name: frozenset(connection.allowed_roles)
            for name, connection in config.connections.items()
        }

    async def execute(
        self,
        sql: str,
        *,
        connection: str,
        identity: Identity,
        parameters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> SqlQueryResult:
        """Execute *sql* on *connection* with the roles *identity* holds.

        Args:
            sql: SQL statement with native parameter placeholders.
            connection: Name of the configured connection to use.
            identity: Verified caller injected by the
                :func:`~loom.core.use_case.markers.Caller` marker; the sole
                source of the effective roles.
            parameters: Values bound server-side by the backend.
            limit: Requested row limit; clamped to the connection ``max_limit``
                and defaulted to ``default_limit`` when absent.
            offset: Number of rows to skip.

        Returns:
            The standard tabular result envelope from the executor.

        Raises:
            UnknownConnectionError: When *connection* is not configured.
            RolesNotBoundError: When the identity is anonymous, holds no role,
                or holds none that the connection allowlists. A connection
                configured without a registered executor is reported this way
                too when the caller holds no allowlisted role: the roles are
                checked here, before the service reports the connection as
                unknown. Both are refusals; only the wording differs.
            ConfigError: When the application has no ``sql:`` section.
        """
        allowed_roles = self._allowed_roles.get(connection)
        if allowed_roles is None:
            return await self._service.execute(
                sql, connection=connection, parameters=parameters, limit=limit, offset=offset
            )
        roles = self._bound_roles(identity, connection, allowed_roles)
        with self._audit_span(connection, identity, roles):
            return await self._service.execute(
                sql,
                connection=connection,
                roles=roles,
                parameters=parameters,
                limit=limit,
                offset=offset,
            )

    def _bound_roles(
        self,
        identity: Identity,
        connection: str,
        allowed_roles: frozenset[str],
    ) -> tuple[str, ...]:
        """Derive the caller's roles; the shared ``default_role`` is unreachable.

        ``roles_bound`` is hard-coded ``True``, because binding to the identity
        is what this collaborator is.

        The emptiness check that follows is defence in depth, not a reachable
        branch: with ``roles_bound=True`` and no requested roles, the
        resolution either returns a non-empty intersection or raises. It stays
        because ``()`` is the one value that would be *accepted* downstream —
        ``SqlQueryService.execute`` reads it as "no roles given" and falls back
        to the connection's shared ``default_role``, turning a
        refusal into a silent privilege swap.
        """
        roles = resolve_query_roles(
            identity,
            connection=connection,
            roles_bound=True,
            allowed_roles=allowed_roles,
            requested_roles=None,
        )
        if not roles:  # pragma: no cover
            raise RolesNotBoundError(connection)
        return roles

    def _audit_span(
        self,
        connection: str,
        identity: Identity,
        roles: tuple[str, ...],
    ) -> AbstractContextManager[None]:
        """Open the audit span of one accepted query, or a no-op.

        The roles are always non-empty here, so the label names the privileges
        the query really runs with — never the connection's shared role, which
        this path cannot reach.
        """
        if self._observability is None:
            return nullcontext()
        return self._observability.span(
            Scope.READ,
            f"sql:{connection}",
            trace_id=get_trace_id(),
            connection=connection,
            roles=",".join(roles),
            subject=identity.subject,
            mechanism=identity.mechanism,
        )
