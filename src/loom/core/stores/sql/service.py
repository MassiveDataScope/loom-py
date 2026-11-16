"""Application-scope SQL query service applying the fail-closed policy.

The service resolves the connection, applies the role/readonly/pagination
policy and delegates execution to the backend executor. The policy is applied
last: connection-level settings can never override it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from loom.core.config.errors import ConfigError
from loom.core.stores.sql.abc.contracts import SqlExecutionOptions, SqlExecutor, SqlQueryResult
from loom.core.stores.sql.abc.errors import (
    RoleNotAllowedError,
    RoleRequiredError,
    UnknownConnectionError,
)
from loom.core.stores.sql.config import SqlConfig, SqlConnectionConfig


class SqlQueryService:
    """Executes SQL through named connections under the fail-closed role policy.

    Role resolution, readonly enforcement and limit clamping happen here,
    before the executor is ever touched. A rejected role never reaches the
    backend.

    This is the **unbound** path: the roles are an argument, so the code
    writing the call chooses them, bounded only by the connection allowlist.
    That is correct for system work with no caller — a scheduled job, a
    migration, a health probe. Work done on behalf of a caller uses
    :class:`~loom.core.stores.sql.caller_bound.CallerBoundSql`, which derives the
    roles from the verified identity and accepts no ``roles`` argument.

    Args:
        executors: Backend executor per connection name.
        config: Parsed ``sql:`` section with the named connections.

    Example — system work with no caller, running as the connection's own
    ``default_role``::

        service = SqlQueryService(executors=executors, config=sql_config)
        result = await service.execute(
            "SELECT count() FROM sales", connection="analytics"
        )
    """

    def __init__(self, executors: Mapping[str, SqlExecutor], config: SqlConfig) -> None:
        self._executors = executors
        self._config = config
        self._allowed_roles: dict[str, frozenset[str]] = {
            name: frozenset(connection.allowed_roles)
            for name, connection in config.connections.items()
        }

    async def execute(
        self,
        sql: str,
        *,
        connection: str,
        roles: Sequence[str] | None = None,
        parameters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> SqlQueryResult:
        """Execute *sql* on *connection* applying the connection policy.

        Args:
            sql: SQL statement with native parameter placeholders.
            roles: Caller roles, each validated against the connection
                allowlist; the query runs with the union of their privileges.
                Empty or ``None`` falls back to the connection ``default_role``.
                They are not checked against any caller: when the query is run
                on behalf of one, use
                :class:`~loom.core.stores.sql.caller_bound.CallerBoundSql`.
            connection: Name of the configured connection to use.
            parameters: Values bound server-side by the backend.
            limit: Requested row limit; clamped to the connection ``max_limit``
                and defaulted to ``default_limit`` when absent.
            offset: Number of rows to skip.

        Returns:
            The standard tabular result envelope from the executor.

        Raises:
            UnknownConnectionError: When *connection* is not configured.
            RoleNotAllowedError: When any requested role is outside the
                allowlist — the whole request is refused, never filtered.
            RoleRequiredError: When no effective role can be resolved.
        """
        conn = self._connection_config(connection)
        options = SqlExecutionOptions(
            roles=_resolve_roles(roles, self._allowed_roles[connection], conn, connection),
            readonly=conn.readonly,
            limit=_effective_limit(limit, conn),
            offset=offset,
            max_execution_time=conn.max_execution_time,
        )
        executor = self._executors[connection]
        return await executor.execute(sql, parameters=parameters, options=options)

    def _connection_config(self, connection: str) -> SqlConnectionConfig:
        config = self._config.connections.get(connection)
        if config is None or connection not in self._executors:
            raise UnknownConnectionError(connection)
        return config


class NullSqlQueryService(SqlQueryService):
    """Null implementation registered when no ``sql:`` section is configured.

    Keeps :class:`SqlQueryService` always resolvable from the container so a
    use case never hits an opaque resolution error: the first ``execute`` call
    raises an actionable :class:`~loom.core.config.errors.ConfigError` instead.
    """

    def __init__(self) -> None:
        super().__init__(executors={}, config=SqlConfig(connections={}))

    async def execute(
        self,
        sql: str,
        *,
        connection: str,
        roles: Sequence[str] | None = None,
        parameters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> SqlQueryResult:
        """Always fail: SQL execution requires the ``sql`` config section.

        Raises:
            ConfigError: Always, with instructions to configure the section.
        """
        raise ConfigError(
            "SqlQueryService is not configured: the application config has no 'sql' "
            "section. Add 'sql.connections.<name>' with at least 'backend' and 'url' "
            "to enable SQL execution."
        )


def _resolve_roles(
    roles: Sequence[str] | None,
    allowed_roles: frozenset[str],
    config: SqlConnectionConfig,
    connection: str,
) -> tuple[str, ...]:
    """Resolve the effective roles fail-closed: allowlist, then default_role.

    The allowlist is the last barrier of the whole chain: it is checked per
    role (O(1) each) and a single rejected role refuses the request instead of
    silently narrowing it.
    """
    if roles:
        for role in roles:
            if role not in allowed_roles:
                raise RoleNotAllowedError(role, connection=connection)
        return tuple(roles)
    if config.default_role is not None:
        return (config.default_role,)
    raise RoleRequiredError(connection)


def _effective_limit(limit: int | None, config: SqlConnectionConfig) -> int:
    """Return ``min(limit or default_limit, max_limit)``."""
    requested = limit if limit is not None else config.default_limit
    return min(requested, config.max_limit)
