"""ClickHouse implementation of the :class:`~loom.core.stores.sql.abc.SqlExecutor` port."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from loom.core.errors import SystemError
from loom.core.stores.sql.abc import (
    SqlColumn,
    SqlExecutionError,
    SqlExecutionOptions,
    SqlQueryResult,
)
from loom.core.stores.sql.config import SqlConnectionConfig
from loom.stores.clickhouse._client import (
    AsyncClickHouseClient,
    ClickHouseQueryResult,
    DatabaseError,
    OperationalError,
    sanitize_backend_error,
)


class ClickHouseSqlExecutor:
    """Executes SQL on one ClickHouse connection with per-query policy settings.

    Translates :class:`SqlExecutionOptions` into native per-query ``settings``
    merged last over the connection settings, so the policy (role, readonly,
    limits) can never be overridden. Pagination requests ``limit + 1`` rows to
    compute ``has_more`` and relies on the ``max_result_rows`` backstop with
    ``result_overflow_mode='throw'`` (never truncate silently). The client is
    never mutated between queries: isolation relies exclusively on per-query
    settings.

    Args:
        client: Async ClickHouse client bound to this connection.
        config: Connection configuration providing limits and extra settings.
    """

    def __init__(self, *, client: AsyncClickHouseClient, config: SqlConnectionConfig) -> None:
        self._client = client
        self._config = config

    async def execute(
        self,
        sql: str,
        *,
        parameters: Mapping[str, Any] | None = None,
        options: SqlExecutionOptions,
    ) -> SqlQueryResult:
        """Execute *sql* with server-side bound *parameters* under *options*.

        Args:
            sql: SQL statement with native ``{name:Type}`` placeholders.
            parameters: Values bound server-side by the driver; never
                interpolated locally.
            options: Policy-resolved execution options for this single query.

        Returns:
            The standard tabular envelope, trimmed to the effective limit.

        Raises:
            SqlExecutionError: When ClickHouse rejects the statement (sanitized
                error code line, without host, DSN or stack trace).
            SystemError: When the backend is unreachable; the message never
                carries the URL.
        """
        limit = self._effective_limit(options.limit)
        settings = self._query_settings(options, limit)
        started = time.perf_counter()
        try:
            driver_result = await self._client.query(sql, parameters=parameters, settings=settings)
        except OperationalError as exc:
            raise SystemError("SQL backend is unreachable or failed at transport level") from exc
        except DatabaseError as exc:
            raise SqlExecutionError(sanitize_backend_error(str(exc))) from exc
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        return _build_result(
            driver_result, limit=limit, offset=options.offset, elapsed_ms=elapsed_ms
        )

    def _effective_limit(self, requested: int | None) -> int:
        limit = requested if requested is not None else self._config.default_limit
        return min(limit, self._config.max_limit)

    def _query_settings(self, options: SqlExecutionOptions, limit: int) -> dict[str, Any]:
        """Merge the policy settings last so connection settings never win."""
        settings: dict[str, Any] = dict(self._config.settings)
        settings.pop("role", None)
        settings.update(
            limit=limit + 1,
            offset=options.offset,
            max_execution_time=options.max_execution_time,
            max_result_rows=self._config.max_limit + 1,
            result_overflow_mode="throw",
        )
        if options.roles:
            settings["role"] = _role_setting(options.roles)
        if options.readonly:
            settings["readonly"] = 1
        return settings


def _role_setting(roles: tuple[str, ...]) -> str | tuple[str, ...]:
    """Render the roles as the driver expects them for one query.

    A single role keeps the plain scalar form (one ``role=`` parameter, the
    only shape older drivers ever supported); several roles travel as a
    sequence, which the driver boundary serializes into repeated ``role=``
    parameters — the only form ClickHouse accepts for multiple roles.
    """
    return roles[0] if len(roles) == 1 else roles


def _build_result(
    driver_result: ClickHouseQueryResult,
    *,
    limit: int,
    offset: int,
    elapsed_ms: float,
) -> SqlQueryResult:
    """Build the envelope: trim the ``limit + 1`` probe row into ``has_more``."""
    raw_rows = driver_result.result_rows
    has_more = len(raw_rows) > limit
    rows = tuple(tuple(row) for row in (raw_rows[:limit] if has_more else raw_rows))
    columns = tuple(
        SqlColumn(name=name, type=column_type.name)
        for name, column_type in zip(
            driver_result.column_names, driver_result.column_types, strict=True
        )
    )
    return SqlQueryResult(
        columns=columns,
        rows=rows,
        row_count=len(rows),
        limit=limit,
        offset=offset,
        has_more=has_more,
        elapsed_ms=elapsed_ms,
    )
