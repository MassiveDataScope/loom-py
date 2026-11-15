"""readonly, limits and pagination policy tests for ``SqlQueryService`` (spec §3/§7.4).

The executor-native mechanics (limit+1, has_more, max_result_rows) are pinned
in ``test_clickhouse_executor.py``; this module pins the service-side policy.
"""

from __future__ import annotations

from tests.unit.core.sql._fakes import FakeSqlExecutor, make_connection_config
from tests.unit.core.sql.conftest import ServiceFactory


class TestPaginationLimits:
    async def test_readonly_is_active_when_connection_uses_the_default(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """``readonly`` defaults to True and travels in the options of every query."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute("SELECT 1", connection="analytics")
        assert fake_executor.calls[0].options.readonly is True

    async def test_readonly_is_off_when_connection_declares_it(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """``readonly: false`` on the connection disables the flag in the options."""
        service = make_service(fake_executor, make_connection_config(readonly=False))
        await service.execute("INSERT INTO t VALUES (1)", connection="analytics")
        assert fake_executor.calls[0].options.readonly is False

    async def test_applies_default_limit_when_request_brings_no_limit(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """Without an explicit ``limit`` the service applies ``default_limit`` (1000)."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute("SELECT 1", connection="analytics")
        assert fake_executor.calls[0].options.limit == 1000

    async def test_clamps_the_limit_when_it_exceeds_max_limit(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """The caller ``limit`` never exceeds the connection ``max_limit``."""
        service = make_service(
            fake_executor, make_connection_config(default_limit=10, max_limit=50)
        )
        await service.execute("SELECT 1", connection="analytics", limit=5000)
        assert fake_executor.calls[0].options.limit == 50

    async def test_respects_the_limit_when_within_range(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """A ``limit`` below ``max_limit`` travels unaltered."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute("SELECT 1", connection="analytics", limit=7)
        assert fake_executor.calls[0].options.limit == 7

    async def test_propagates_the_request_offset(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """The caller ``offset`` reaches the executor options untouched."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute("SELECT 1", connection="analytics", offset=30)
        assert fake_executor.calls[0].options.offset == 30

    async def test_propagates_max_execution_time_from_the_connection(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """The connection ``max_execution_time`` governs every executed query."""
        service = make_service(fake_executor, make_connection_config(max_execution_time=7))
        await service.execute("SELECT 1", connection="analytics")
        assert fake_executor.calls[0].options.max_execution_time == 7

    async def test_returns_the_executor_result_without_copying_it(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """The executor envelope is returned as-is (no double serialization/copies)."""
        service = make_service(fake_executor, make_connection_config())
        result = await service.execute("SELECT 1", connection="analytics")
        assert result is fake_executor.result
