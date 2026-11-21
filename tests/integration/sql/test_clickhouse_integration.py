"""ClickHouse integration tests for the SQL API (spec §2/§3/§7 "Integración").

Runs against the ``clickhouse`` service from ``docker-compose.local.yaml``, whose
init SQL (``tests/integration/sql/init/01_users_roles.sql``) creates the canonical
grants matrix from spec §2 (user ``loom_api`` with ``DEFAULT ROLE NONE``; roles
``loom_reader_a``/``loom_reader_b`` granted, ``loom_ungranted`` not granted).

Gating: only optional third-party pieces skip — a missing ``clickhouse-connect``
extra (``pytest.importorskip``) or no reachable server (default
``http://localhost:18123``, override with ``LOOM_CLICKHOUSE_IT_URL``). The
``loom.core.stores.sql`` modules are first-party: a breakage there must FAIL, never skip.

Ground truth pinned from the real server (clickhouse/clickhouse-server:25.3,
version 25.3.14.14, captured via raw HTTP):

- unknown role            → code 511 UNKNOWN_ROLE (HTTP 404)
- existing non-granted role → code 512 SET_NON_GRANTED_ROLE (HTTP 403)
- no role + DEFAULT ROLE NONE → code 497 ACCESS_DENIED on table reads
"""

from __future__ import annotations

import asyncio
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

pytest.importorskip("clickhouse_connect", reason="clickhouse-connect extra not installed")

# Marked `integration`: this module needs a provisioned ClickHouse. The fast CI
# lane deselects it and the integration lane runs it with the service actually
# up — before this marker existed the module ran in CI and skipped every time,
# which reads as approved without ever having executed.
pytestmark = pytest.mark.integration

# Imports after the importorskip guard: loom.stores.clickhouse imports the
# clickhouse-connect driver at module import time.
from loom.core.stores.sql.abc import SqlExecutionError, SqlExecutionOptions  # noqa: E402
from loom.stores.clickhouse import ClickHouseConnectionRegistry  # noqa: E402

URL_ENV_VAR = "LOOM_CLICKHOUSE_IT_URL"
DEFAULT_URL = "http://loom_api:loom_api_pw@localhost:18123"
CONNECTION = "analytics"

ROLE_A = "loom_reader_a"  # granted to loom_api; reads loom_it only
ROLE_B = "loom_reader_b"  # granted to loom_api; the only role reading loom_it_b
ROLE_UNGRANTED = "loom_ungranted"  # exists, NOT granted to loom_api
ROLE_UNKNOWN = "loom_role_that_does_not_exist"
SENTINEL_ROLE = "loom_probe_sentinel_role_does_not_exist"

# Observed server error codes (see module docstring) — the fail-closed contract.
UNKNOWN_ROLE_CODE = "511"
NON_GRANTED_ROLE_CODE = "512"


def _server_url() -> str:
    return os.environ.get(URL_ENV_VAR, DEFAULT_URL)


def _ping_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return f"{parts.scheme}://{parts.hostname}:{parts.port or 8123}/ping"


def _server_available(url: str) -> bool:
    try:
        with urllib.request.urlopen(_ping_url(url), timeout=2) as response:
            return bool(response.status == 200)
    except (urllib.error.URLError, OSError):
        return False


@pytest.fixture(scope="module")
def clickhouse_url() -> str:
    """Skip the module when the docker ClickHouse server is not reachable."""
    url = _server_url()
    if not _server_available(url):
        pytest.skip(
            f"ClickHouse not reachable at {url} — start it with "
            f"'docker compose -f docker-compose.local.yaml up -d clickhouse' "
            f"or point {URL_ENV_VAR} at a server provisioned with "
            f"tests/integration/sql/init/01_users_roles.sql"
        )
    return url


def _connection_config(url: str, allowed_roles: list[str]) -> dict[str, Any]:
    """Connection mapping shaped exactly like spec §3 ``sql.connections.<name>``."""
    return {
        "backend": "clickhouse",
        "url": url,
        "allowed_roles": allowed_roles,
        "default_role": ROLE_A,
        "readonly": True,
        "default_limit": 100,
        "max_limit": 1000,
        "max_execution_time": 30,
    }


def _registry(url: str, *, allowed_roles: list[str] | None = None) -> ClickHouseConnectionRegistry:
    """Build the registry (async CM, spec §3) from the raw ``sql:`` section mapping."""
    return ClickHouseConnectionRegistry.from_config(
        {"connections": {CONNECTION: _connection_config(url, allowed_roles or [ROLE_A])}}
    )


def _options(**overrides: Any) -> SqlExecutionOptions:
    return SqlExecutionOptions(**overrides)


def _as_utc(value: datetime) -> datetime:
    """Normalize driver datetimes: naive values are UTC by clickhouse-connect contract."""
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value


class TestPerQueryRole:
    async def test_role_and_readonly_applied_per_query(self, clickhouse_url: str) -> None:
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)
            result = await executor.execute(
                "SELECT currentRoles(), toUInt8(getSetting('readonly'))",
                options=_options(roles=(ROLE_A,)),
            )
        current_roles, readonly = result.rows[0]
        assert list(current_roles) == [ROLE_A]
        assert int(readonly) == 1

    async def test_role_grants_table_access(self, clickhouse_url: str) -> None:
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)
            result = await executor.execute(
                "SELECT count() FROM loom_it.events", options=_options(roles=(ROLE_A,))
            )
        assert result.rows[0][0] == 5

    async def test_unknown_role_fails_closed_with_code_511(self, clickhouse_url: str) -> None:
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)
            options = _options(roles=(ROLE_UNKNOWN,))
            with pytest.raises(SqlExecutionError) as excinfo:
                await executor.execute("SELECT 1", options=options)
        assert UNKNOWN_ROLE_CODE in str(excinfo.value)

    async def test_non_granted_role_rejected_with_code_512(self, clickhouse_url: str) -> None:
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)
            options = _options(roles=(ROLE_UNGRANTED,))
            with pytest.raises(SqlExecutionError) as excinfo:
                await executor.execute("SELECT 1", options=options)
        assert NON_GRANTED_ROLE_CODE in str(excinfo.value)


class TestMultipleRolesPerQuery:
    """Several roles in ONE query: repeated ``role`` HTTP parameters (spec §2/§3).

    ClickHouse activates one role per repeated parameter; a comma-joined value
    is read as a single role name and rejected with code 511. These tests are
    the end-to-end proof of the driver transport workaround in
    ``loom.stores.clickhouse._client``.
    """

    async def test_both_roles_are_active_in_the_same_query(self, clickhouse_url: str) -> None:
        async with _registry(clickhouse_url, allowed_roles=[ROLE_A, ROLE_B]) as registry:
            result = await registry.executor(CONNECTION).execute(
                "SELECT currentRoles()", options=_options(roles=(ROLE_A, ROLE_B))
            )
        assert sorted(result.rows[0][0]) == sorted([ROLE_A, ROLE_B])

    async def test_query_gets_the_union_of_both_role_privileges(self, clickhouse_url: str) -> None:
        """Only the union reads both databases: ROLE_A owns loom_it, ROLE_B loom_it_b."""
        sql = (
            "SELECT (SELECT count() FROM loom_it.events) + (SELECT count() FROM loom_it_b.events_b)"
        )
        async with _registry(clickhouse_url, allowed_roles=[ROLE_A, ROLE_B]) as registry:
            result = await registry.executor(CONNECTION).execute(
                sql, options=_options(roles=(ROLE_A, ROLE_B))
            )
        assert result.rows[0][0] == 7

    async def test_single_role_cannot_read_the_other_roles_database(
        self, clickhouse_url: str
    ) -> None:
        """The union is real: ROLE_A alone is denied on the ROLE_B database."""
        async with _registry(clickhouse_url, allowed_roles=[ROLE_A, ROLE_B]) as registry:
            executor = registry.executor(CONNECTION)
            options = _options(roles=(ROLE_A,))
            with pytest.raises(SqlExecutionError):
                await executor.execute("SELECT count() FROM loom_it_b.events_b", options=options)

    async def test_a_non_granted_role_still_fails_closed_with_code_512(
        self, clickhouse_url: str
    ) -> None:
        """One bad role poisons the whole query: no partial privilege activation."""
        async with _registry(clickhouse_url, allowed_roles=[ROLE_A, ROLE_B]) as registry:
            executor = registry.executor(CONNECTION)
            options = _options(roles=(ROLE_A, ROLE_UNGRANTED))
            with pytest.raises(SqlExecutionError) as excinfo:
                await executor.execute("SELECT 1", options=options)
        assert NON_GRANTED_ROLE_CODE in str(excinfo.value)


class TestConcurrentIsolation:
    async def test_concurrent_queries_each_keep_their_own_role(self, clickhouse_url: str) -> None:
        roles = tuple(ROLE_A if i % 2 == 0 else ROLE_B for i in range(10))
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)

            async def run(role: str) -> tuple[str, Any]:
                result = await executor.execute(
                    "SELECT currentRoles()", options=_options(roles=(role,))
                )
                return role, result.rows[0][0]

            outcomes = await asyncio.gather(*(run(role) for role in roles))
        for requested_role, current_roles in outcomes:
            assert list(current_roles) == [requested_role]


class TestPagination:
    async def test_limit_offset_return_correct_slices(self, clickhouse_url: str) -> None:
        sql = "SELECT id FROM loom_it.events ORDER BY id"
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)

            async def page(offset: int) -> Any:
                return await executor.execute(
                    sql, options=_options(roles=(ROLE_A,), limit=2, offset=offset)
                )

            first, middle, last = await page(0), await page(2), await page(4)

        assert [row[0] for row in first.rows] == [1, 2]
        assert (first.limit, first.offset, first.row_count) == (2, 0, 2)
        assert first.has_more is True

        assert [row[0] for row in middle.rows] == [3, 4]
        assert middle.has_more is True

        assert [row[0] for row in last.rows] == [5]
        assert (last.limit, last.offset, last.row_count) == (2, 4, 1)
        assert last.has_more is False


class TestStartupProbe:
    async def test_registry_probe_passes_against_real_server(self, clickhouse_url: str) -> None:
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)
            result = await executor.execute("SELECT 1", options=_options(roles=(ROLE_A,)))
        assert result.rows[0][0] == 1

    async def test_sentinel_role_rejected_even_for_privilege_free_query(
        self, clickhouse_url: str
    ) -> None:
        """Ground truth the fail-closed probe relies on (spec §2).

        ``SELECT 1`` needs no grants, yet the server must still reject the query
        when the per-query role does not exist (code 511) — a server that
        silently ignores the role setting must abort startup.
        """
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)
            options = _options(roles=(SENTINEL_ROLE,))
            with pytest.raises(SqlExecutionError) as excinfo:
                await executor.execute("SELECT 1", options=options)
        assert UNKNOWN_ROLE_CODE in str(excinfo.value)


class TestHeterogeneousTypes:
    async def test_type_matrix_round_trip(self, clickhouse_url: str) -> None:
        """Serialization ground truth for the endpoint wave (spec §3 columns/rows)."""
        async with _registry(clickhouse_url) as registry:
            executor = registry.executor(CONNECTION)
            result = await executor.execute(
                "SELECT id, created_at, amount, note, flags FROM loom_it.events ORDER BY id",
                options=_options(roles=(ROLE_A,)),
            )

        assert tuple(column.name for column in result.columns) == (
            "id",
            "created_at",
            "amount",
            "note",
            "flags",
        )
        normalized_types = tuple(column.type.replace(" ", "") for column in result.columns)
        assert normalized_types == (
            "UInt32",
            "DateTime64(3,'UTC')",
            "Decimal(18,4)",
            "Nullable(String)",
            "Array(UInt8)",
        )

        assert result.row_count == 5
        first = result.rows[0]
        assert first[0] == 1
        assert _as_utc(first[1]) == datetime(2026, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)
        assert first[2] == Decimal("10.5000")
        assert first[3] == "first"
        assert list(first[4]) == [1, 2, 3]

        second = result.rows[1]
        assert second[2] == Decimal("-2.2500")
        assert second[3] is None
        assert list(second[4]) == []

        last = result.rows[4]
        assert _as_utc(last[1]) == datetime(2026, 7, 4, 16, 20, 0, 500000, tzinfo=UTC)
        assert last[2] == Decimal("123.4567")
        assert list(last[4]) == [42]
