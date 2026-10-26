"""Typed fakes and builders shared by the SQL subsystem unit tests.

The contracts consumed here (``loom.core.sql.abc``, ``loom.core.sql.config``)
are the ones defined in ``specs/sql_api_clickhouse_spec.md`` §3. This module
implements no production code: only test doubles that capture whatever the
real code hands them.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from clickhouse_connect.driver.exceptions import DatabaseError

from loom.core.model.redaction import Dsn, RedactedMap, Secret
from loom.core.sql.abc import SqlColumn, SqlExecutionOptions, SqlQueryResult
from loom.core.sql.config import SqlConfig, SqlConnectionConfig

DEFAULT_URL = "http://clickhouse.test:8123"
DEFAULT_ROLES: tuple[str, ...] = ("role_viz_reader", "role_viz_sales")


def make_connection_config(**overrides: Any) -> SqlConnectionConfig:
    """Build a valid ``SqlConnectionConfig`` with targeted overrides.

    When ``max_limit`` is lowered without an explicit ``default_limit``, the
    default limit is clamped so the config honours the spec §3 invariant
    ``default_limit <= max_limit`` validated at parse time.
    """
    params: dict[str, Any] = {
        "backend": "clickhouse",
        "url": DEFAULT_URL,
        "allowed_roles": DEFAULT_ROLES,
        "default_role": "role_viz_reader",
    }
    params.update(overrides)
    if "max_limit" in overrides and "default_limit" not in overrides:
        params["default_limit"] = min(1000, overrides["max_limit"])
    url = params.get("url")
    if isinstance(url, str):
        params["url"] = Dsn(url)
    password = params.get("password")
    if isinstance(password, str):
        params["password"] = Secret(password)
    settings = params.get("settings")
    if isinstance(settings, dict):
        params["settings"] = RedactedMap(settings)
    return SqlConnectionConfig(**params)


def make_sql_config(**connections: SqlConnectionConfig) -> SqlConfig:
    """Build a ``SqlConfig`` from named connections."""
    return SqlConfig(connections=dict(connections))


def make_query_result(**overrides: Any) -> SqlQueryResult:
    """Build a reference ``SqlQueryResult`` for the fakes."""
    params: dict[str, Any] = {
        "columns": (
            SqlColumn(name="id", type="UInt64"),
            SqlColumn(name="name", type="String"),
        ),
        "rows": ((1, "alpha"), (2, "beta")),
        "row_count": 2,
        "limit": 1000,
        "offset": 0,
        "has_more": False,
        "elapsed_ms": 1.5,
    }
    params.update(overrides)
    return SqlQueryResult(**params)


@dataclass(frozen=True)
class ExecutorCall:
    """Call captured by :class:`FakeSqlExecutor`."""

    sql: str
    parameters: Mapping[str, Any] | None
    options: SqlExecutionOptions


class FakeSqlExecutor:
    """Test ``SqlExecutor``: captures the options of every call."""

    def __init__(
        self,
        *,
        result: SqlQueryResult | None = None,
        error: Exception | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        self.calls: list[ExecutorCall] = []
        self._result = result if result is not None else make_query_result()
        self._error = error
        self._delay_seconds = delay_seconds

    @property
    def result(self) -> SqlQueryResult:
        """Fixed result returned by every successful call."""
        return self._result

    async def execute(
        self,
        sql: str,
        *,
        parameters: Mapping[str, Any] | None = None,
        options: SqlExecutionOptions,
    ) -> SqlQueryResult:
        """Record the call and return the fixed result (or raise the error)."""
        if self._delay_seconds:
            await asyncio.sleep(self._delay_seconds)
        self.calls.append(ExecutorCall(sql=sql, parameters=parameters, options=options))
        if self._error is not None:
            raise self._error
        return self._result


class FakeColumnType:
    """Driver column type: exposes ``.name`` like the real driver does."""

    def __init__(self, name: str) -> None:
        self.name = name

    def __str__(self) -> str:
        return self.name


class FakeDriverQueryResult:
    """``client.query`` result exposing the surface the executor consumes."""

    def __init__(
        self,
        *,
        rows: Sequence[tuple[Any, ...]],
        names: Sequence[str],
        types: Sequence[str],
    ) -> None:
        self.result_rows: list[tuple[Any, ...]] = list(rows)
        self.column_names: tuple[str, ...] = tuple(names)
        self.column_types: tuple[FakeColumnType, ...] = tuple(FakeColumnType(t) for t in types)


@dataclass(frozen=True)
class ClientQueryCall:
    """Call captured by :class:`FakeClickHouseClient`."""

    query: str | None
    parameters: Mapping[str, Any] | None
    settings: Mapping[str, Any] | None
    kwargs: Mapping[str, Any] = field(default_factory=dict)


class FakeClickHouseClient:
    """Async client fake: emulates per-query role (511), limit/offset, close.

    - A role in ``settings`` that is not in ``known_roles`` raises the same
      ``DatabaseError`` code 511 the real server produces (behaviour verified
      in spec §2), unless ``ignore_role=True``, which simulates a server or
      driver that does NOT apply the role (the startup probe must detect that
      and abort).
    - ``settings['limit']``/``settings['offset']`` are applied to the rows
      just like the native server settings.
    """

    def __init__(
        self,
        *,
        rows: Sequence[tuple[Any, ...]] | None = None,
        names: Sequence[str] = ("id", "name"),
        types: Sequence[str] = ("UInt64", "String"),
        known_roles: frozenset[str] = frozenset(DEFAULT_ROLES),
        ignore_role: bool = False,
        transport_settings: frozenset[str] = frozenset(
            {"role", "readonly", "limit", "offset", "max_execution_time"}
        ),
        error: Exception | None = None,
    ) -> None:
        self._rows: list[tuple[Any, ...]] = (
            list(rows) if rows is not None else [(1, "alpha"), (2, "beta")]
        )
        self._names = tuple(names)
        self._types = tuple(types)
        self._known_roles = known_roles
        self._ignore_role = ignore_role
        self._transport_settings = transport_settings
        self._error = error
        self.queries: list[ClientQueryCall] = []
        self.closed = False

    @property
    def valid_transport_settings(self) -> frozenset[str]:
        """Per-query settings accepted by the driver, as the real one reports."""
        return self._transport_settings

    async def query(
        self,
        query: str | None = None,
        *,
        parameters: Mapping[str, Any] | None = None,
        settings: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> FakeDriverQueryResult:
        """Record the call and emulate server-side role/limit/offset."""
        self.queries.append(
            ClientQueryCall(
                query=query,
                parameters=dict(parameters) if parameters is not None else None,
                settings=dict(settings) if settings is not None else None,
                kwargs=dict(kwargs),
            )
        )
        if self._error is not None:
            raise self._error
        self._enforce_role(settings)
        return FakeDriverQueryResult(
            rows=self._apply_window(settings),
            names=self._names,
            types=self._types,
        )

    async def close(self) -> None:
        """Mark the client as closed (verifiable by the registry tests)."""
        self.closed = True

    def _enforce_role(self, settings: Mapping[str, Any] | None) -> None:
        """Reject unknown roles like the server does, one per repeated parameter."""
        setting = settings.get("role") if settings else None
        if setting is None or self._ignore_role:
            return
        roles = (setting,) if isinstance(setting, str) else tuple(setting)
        for role in roles:
            if role not in self._known_roles:
                raise DatabaseError(
                    f"HTTPDriver for {DEFAULT_URL} received ClickHouse error code 511\n"
                    f"Code: 511. DB::Exception: There is no role `{role}` "
                    "in user directories. (UNKNOWN_ROLE)"
                )

    def _apply_window(self, settings: Mapping[str, Any] | None) -> list[tuple[Any, ...]]:
        rows = list(self._rows)
        if settings is None:
            return rows
        if "offset" in settings:
            rows = rows[int(settings["offset"]) :]
        if "limit" in settings:
            rows = rows[: int(settings["limit"])]
        return rows


class RecordingClientFactory:
    """Fake client factory that captures the kwargs handed to the driver.

    The registry must invoke it with the same keyword arguments it would pass
    to ``clickhouse_connect.get_async_client`` — that lets the tests pin the
    hard rules of the spec (``autogenerate_session_id=False``,
    ``query_retries=0``, explicit timeouts) without any network.
    """

    def __init__(self, *clients: FakeClickHouseClient) -> None:
        self.calls: list[dict[str, Any]] = []
        self.created: list[FakeClickHouseClient] = []
        self._queue: list[FakeClickHouseClient] = list(clients)

    async def __call__(self, **kwargs: Any) -> FakeClickHouseClient:
        self.calls.append(dict(kwargs))
        client = self._queue.pop(0) if self._queue else FakeClickHouseClient()
        self.created.append(client)
        return client
