"""Backend-agnostic SQL execution contracts.

These types form the port between :class:`~loom.core.stores.sql.service.SqlQueryService`
and concrete SQL backends. They carry no infrastructure imports: each executor
translates :class:`SqlExecutionOptions` to its native mechanism and returns the
standard :class:`SqlQueryResult` envelope.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from loom.core.model import LoomFrozenStruct


class SqlExecutionOptions(LoomFrozenStruct, frozen=True, kw_only=True):
    """Backend-agnostic execution options resolved by the service policy.

    Each executor translates these values to its native per-query mechanism
    (for ClickHouse: per-query ``settings``). The policy applies them last, so
    connection-level settings can never override them.

    Attributes:
        roles: Effective roles for this single query, already validated against
            the connection allowlist. Several roles apply the union of their
            privileges; empty means no role at all (internal probes only).
        readonly: Whether the query must run in read-only mode.
        limit: Maximum number of rows to return. The executor requests
            ``limit + 1`` rows to compute ``has_more``.
        offset: Number of rows to skip before returning results.
        max_execution_time: Per-query execution timeout in seconds.
    """

    roles: tuple[str, ...] = ()
    readonly: bool = True
    limit: int | None = None
    offset: int = 0
    max_execution_time: int = 30


class SqlColumn(LoomFrozenStruct, frozen=True, kw_only=True):
    """Column descriptor of a SQL result set.

    Attributes:
        name: Column name as reported by the backend.
        type: Native backend type (e.g. ``"DateTime64(3)"``).
    """

    name: str
    type: str


class SqlQueryResult(LoomFrozenStruct, frozen=True, kw_only=True):
    """Standard tabular envelope returned for any SQL statement.

    Attributes:
        columns: Ordered column descriptors of the result set.
        rows: Result rows, already trimmed to ``limit``.
        row_count: Number of rows in ``rows``.
        limit: Effective limit applied to this page.
        offset: Effective offset applied to this page.
        has_more: Whether more rows exist beyond this page.
        elapsed_ms: Server-side execution telemetry in milliseconds.
    """

    columns: tuple[SqlColumn, ...]
    rows: tuple[tuple[Any, ...], ...]
    row_count: int
    limit: int
    offset: int
    has_more: bool
    elapsed_ms: float


class SqlExecutor(Protocol):
    """Port implemented by concrete SQL backends.

    Exists for dependency inversion (the service never imports driver code)
    and for testing with fakes. Documented as a contract in evolution until a
    second backend implementation lands.
    """

    async def execute(
        self,
        sql: str,
        *,
        parameters: Mapping[str, Any] | None = None,
        options: SqlExecutionOptions,
    ) -> SqlQueryResult:
        """Execute *sql* with server-side bound *parameters* under *options*.

        Args:
            sql: SQL statement with native parameter placeholders.
            parameters: Values bound server-side; never interpolated locally.
            options: Policy-resolved execution options for this single query.

        Returns:
            The standard tabular result envelope.

        Raises:
            SqlExecutionError: When the backend rejects the statement.
        """
        ...
