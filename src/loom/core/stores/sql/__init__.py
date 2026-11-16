"""Backend-agnostic SQL query subsystem.

Provides an injectable :class:`SqlQueryService` that executes SQL through
named connections with a fail-closed per-request role policy, readonly
enforcement and pagination. Concrete backends implement the
:class:`SqlExecutor` port; the first supported backend is ClickHouse
(``loom.stores.clickhouse``, optional extra ``loom-kernel[clickhouse]``).

Work done on behalf of a caller uses :class:`CallerBoundSql` instead, which
derives the roles from the verified identity rather than from an argument.
"""

from loom.core.stores.sql.abc.contracts import (
    SqlColumn,
    SqlExecutionOptions,
    SqlExecutor,
    SqlQueryResult,
)
from loom.core.stores.sql.abc.errors import (
    RoleNotAllowedError,
    RoleRequiredError,
    RolesNotBoundError,
    SqlExecutionError,
    UnknownConnectionError,
)
from loom.core.stores.sql.caller_bound import CallerBoundSql
from loom.core.stores.sql.config import SqlConfig, SqlConnectionConfig, SqlEndpointConfig
from loom.core.stores.sql.roles import resolve_query_roles
from loom.core.stores.sql.service import NullSqlQueryService, SqlQueryService

__all__ = [
    "CallerBoundSql",
    "NullSqlQueryService",
    "RoleNotAllowedError",
    "RoleRequiredError",
    "RolesNotBoundError",
    "SqlColumn",
    "SqlConfig",
    "SqlConnectionConfig",
    "SqlEndpointConfig",
    "SqlExecutionError",
    "SqlExecutionOptions",
    "SqlExecutor",
    "SqlQueryResult",
    "SqlQueryService",
    "UnknownConnectionError",
    "resolve_query_roles",
]
