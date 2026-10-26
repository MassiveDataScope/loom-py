"""ClickHouse executor tests with a fake client — no network (spec §2/§3/§7.9)."""

from __future__ import annotations

import importlib
import sys
from typing import Any

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from loom.core.errors import RuleViolation
from loom.core.errors import SystemError as LoomSystemError
from loom.core.sql.abc import SqlColumn, SqlExecutionError, SqlExecutionOptions
from loom.core.sql.clickhouse import ClickHouseConnectionRegistry, ClickHouseSqlExecutor
from tests.helpers.extras import CLICKHOUSE_ISLAND
from tests.unit.core.sql._fakes import (
    FakeClickHouseClient,
    RecordingClientFactory,
    make_connection_config,
    make_sql_config,
)


def _executor(client: FakeClickHouseClient, **overrides: Any) -> ClickHouseSqlExecutor:
    return ClickHouseSqlExecutor(client=client, config=make_connection_config(**overrides))


def _options(**overrides: Any) -> SqlExecutionOptions:
    params: dict[str, Any] = {
        "roles": ("role_viz_reader",),
        "readonly": True,
        "limit": 3,
        "offset": 0,
    }
    params.update(overrides)
    return SqlExecutionOptions(**params)


def _hide_modules(monkeypatch: pytest.MonkeyPatch, prefix: str) -> None:
    """Simulate the absence of an optional dependency by nulling its modules."""
    for name in list(sys.modules):
        if name == prefix or name.startswith(f"{prefix}."):
            monkeypatch.setitem(sys.modules, name, None)
    monkeypatch.setitem(sys.modules, prefix, None)


def test_missing_clickhouse_connect_raises_import_error_with_hint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the extra, loading the SQL backend fails naming 'loom-kernel[clickhouse]'."""
    _hide_modules(monkeypatch, "clickhouse_connect")
    for name in list(sys.modules):
        if name.startswith(("loom.core.sql.clickhouse", CLICKHOUSE_ISLAND)):
            monkeypatch.delitem(sys.modules, name)
    with pytest.raises(ImportError, match=r"loom-kernel\[clickhouse\]"):
        importlib.import_module("loom.core.sql.clickhouse")


async def test_parameters_travel_server_side_without_interpolation() -> None:
    """The SQL reaches the driver untouched and parameters travel apart (native binding)."""
    client = FakeClickHouseClient()
    sql = "SELECT * FROM t WHERE uid = {uid:UInt64}"
    await _executor(client).execute(sql, parameters={"uid": 7}, options=_options())
    assert (client.queries[0].query, client.queries[0].parameters) == (sql, {"uid": 7})


async def test_registry_sets_query_retries_zero_when_readonly_off() -> None:
    """A non-readonly connection is created with ``query_retries=0`` (no re-INSERT)."""
    factory = RecordingClientFactory()
    config = make_sql_config(analytics=make_connection_config(readonly=False))
    async with ClickHouseConnectionRegistry(config=config, client_factory=factory):
        pass
    assert factory.calls[0]["query_retries"] == 0


async def test_readonly_on_sends_readonly_1_per_query() -> None:
    """With ``readonly=True`` every query carries the native ``readonly=1`` setting."""
    client = FakeClickHouseClient()
    await _executor(client).execute("SELECT 1", options=_options())
    settings = client.queries[0].settings or {}
    assert settings["readonly"] == 1


async def test_readonly_off_does_not_force_readonly_1() -> None:
    """With ``readonly=False`` the query is not forced into readonly=1."""
    client = FakeClickHouseClient()
    await _executor(client, readonly=False).execute("SELECT 1", options=_options(readonly=False))
    settings = client.queries[0].settings or {}
    assert settings.get("readonly") in (None, 0)


async def test_translates_limit_and_offset_to_native_settings_with_extra_row() -> None:
    """Pagination uses native settings: requests ``limit+1`` plus the given offset."""
    client = FakeClickHouseClient(rows=[(i, f"n{i}") for i in range(10)])
    await _executor(client).execute("SELECT 1", options=_options(limit=3, offset=2))
    settings = client.queries[0].settings or {}
    assert (settings["limit"], settings["offset"]) == (4, 2)


async def test_trims_the_extra_row_and_flags_has_more() -> None:
    """When row ``limit+1`` arrives it is trimmed and the envelope flags ``has_more``."""
    client = FakeClickHouseClient(rows=[(i, f"n{i}") for i in range(10)])
    result = await _executor(client).execute("SELECT 1", options=_options(limit=3))
    assert (len(result.rows), result.row_count, result.has_more, result.limit) == (
        3,
        3,
        True,
        3,
    )


async def test_no_extra_row_means_no_has_more() -> None:
    """When the backend returns fewer than ``limit+1`` rows there are no more pages."""
    client = FakeClickHouseClient(rows=[(1, "a"), (2, "b")])
    result = await _executor(client).execute("SELECT 1", options=_options(limit=3))
    assert (result.row_count, result.has_more) == (2, False)


async def test_backstop_max_result_rows_with_overflow_throw() -> None:
    """The backstop requests ``max_result_rows=max_limit+1`` and never truncates silently."""
    client = FakeClickHouseClient()
    await _executor(client, max_limit=50).execute("SELECT 1", options=_options(limit=3))
    settings = client.queries[0].settings or {}
    assert (settings["max_result_rows"], settings["result_overflow_mode"]) == (51, "throw")


async def test_connection_settings_do_not_override_the_policy() -> None:
    """The policy is applied last: connection settings cannot override it."""
    client = FakeClickHouseClient()
    executor = _executor(
        client,
        max_limit=50,
        settings={
            "role": "role_admin_evil",
            "readonly": 0,
            "limit": 999999,
            "offset": 99,
            "max_result_rows": 5,
        },
    )
    await executor.execute("SELECT 1", options=_options(limit=3, offset=2))
    settings = client.queries[0].settings or {}
    assert (
        settings["role"],
        settings["readonly"],
        settings["limit"],
        settings["offset"],
        settings["max_result_rows"],
    ) == ("role_viz_reader", 1, 4, 2, 51)


async def test_one_role_travels_as_a_scalar_setting() -> None:
    """A single role keeps the plain scalar form the driver has always sent."""
    client = FakeClickHouseClient()
    await _executor(client).execute("SELECT 1", options=_options())
    settings = client.queries[0].settings or {}
    assert settings["role"] == "role_viz_reader"


async def test_several_roles_travel_as_a_sequence_setting() -> None:
    """Several roles reach the driver as a sequence: one HTTP parameter each."""
    client = FakeClickHouseClient()
    await _executor(client).execute(
        "SELECT 1", options=_options(roles=("role_viz_reader", "role_viz_sales"))
    )
    settings = client.queries[0].settings or {}
    assert settings["role"] == ("role_viz_reader", "role_viz_sales")


async def test_without_roles_no_role_setting_is_sent() -> None:
    """No effective role means no ``role`` setting at all (fail-closed)."""
    client = FakeClickHouseClient()
    await _executor(client, settings={"role": "role_admin_evil"}).execute(
        "SELECT 1", options=_options(roles=())
    )
    settings = client.queries[0].settings or {}
    assert "role" not in settings


async def test_benign_connection_settings_do_reach_the_driver() -> None:
    """Extra CH settings not reserved by the policy do travel per query."""
    client = FakeClickHouseClient()
    await _executor(client, settings={"max_block_size": 5000}).execute(
        "SELECT 1", options=_options()
    )
    settings = client.queries[0].settings or {}
    assert settings["max_block_size"] == 5000


async def test_max_execution_time_travels_as_per_query_setting() -> None:
    """The ``max_execution_time`` from the options governs every query."""
    client = FakeClickHouseClient()
    await _executor(client).execute("SELECT 1", options=_options(max_execution_time=7))
    settings = client.queries[0].settings or {}
    assert settings["max_execution_time"] == 7


async def test_builds_columns_from_driver_names_and_native_types() -> None:
    """``columns`` comes from column_names/column_types with the backend native type."""
    client = FakeClickHouseClient(names=("id", "ts"), types=("UInt64", "DateTime64(3)"))
    result = await _executor(client).execute("SELECT 1", options=_options())
    assert result.columns == (
        SqlColumn(name="id", type="UInt64"),
        SqlColumn(name="ts", type="DateTime64(3)"),
    )


async def test_backend_error_is_sanitized_into_sql_execution_error() -> None:
    """A DatabaseError becomes SqlExecutionError with the CH code and no stacktrace."""
    client = FakeClickHouseClient(
        error=DatabaseError(
            "Code: 62. DB::Exception: Syntax error: failed at position 8\n"
            "Stack trace:\n0. Poco::Net::HTTPServerConnection::run()"
        )
    )
    executor = _executor(client)
    options = _options()
    with pytest.raises(SqlExecutionError) as exc_info:
        await executor.execute("SELEC 1", options=options)
    assert ("62" in str(exc_info.value), "Stack trace" not in str(exc_info.value)) == (
        True,
        True,
    )


async def test_unreachable_backend_does_not_leak_the_url_in_the_error() -> None:
    """A network failure becomes a generic system error without host or DSN."""
    client = FakeClickHouseClient(
        error=OperationalError(
            "Error executing HTTP request http://secret-host:8123 (connection refused)"
        )
    )
    executor = _executor(client)
    options = _options()
    with pytest.raises(LoomSystemError) as exc_info:
        await executor.execute("SELECT 1", options=options)
    assert "secret-host" not in str(exc_info.value)


def test_sql_execution_error_inherits_from_rule_violation() -> None:
    """SqlExecutionError reuses RuleViolation to map to 422 without touching the API."""
    assert issubclass(SqlExecutionError, RuleViolation)
