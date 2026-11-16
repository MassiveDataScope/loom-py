"""Per-call role isolation tests — no shared mutable state (spec §7.3)."""

from __future__ import annotations

import asyncio

import pytest

from loom.core.stores.sql.abc import SqlExecutionOptions
from loom.stores.clickhouse import ClickHouseConnectionRegistry
from tests.unit.core.sql._fakes import (
    FakeSqlExecutor,
    RecordingClientFactory,
    make_connection_config,
    make_sql_config,
)
from tests.unit.core.sql.conftest import ServiceFactory


class TestSqlCallIsolation:
    async def test_each_concurrent_call_keeps_its_own_role(
        self,
        make_service: ServiceFactory,
    ) -> None:
        """N concurrent executions with distinct roles each capture their own role."""
        roles = tuple(f"role_{i}" for i in range(8))
        executor = FakeSqlExecutor(delay_seconds=0.005)
        service = make_service(
            executor, make_connection_config(allowed_roles=roles, default_role=None)
        )
        await asyncio.gather(
            *(
                service.execute(f"SELECT {i}", connection="analytics", roles=[f"role_{i}"])
                for i in range(8)
            )
        )
        assert {call.sql: call.options.roles for call in executor.calls} == {
            f"SELECT {i}": (f"role_{i}",) for i in range(8)
        }

    async def test_call_without_role_does_not_inherit_previous_role(
        self, fake_executor: FakeSqlExecutor, make_service: ServiceFactory
    ) -> None:
        """The role of one request does not stick to the service for the next one."""
        service = make_service(fake_executor, make_connection_config())
        await service.execute("SELECT 1", connection="analytics", roles=["role_viz_sales"])
        await service.execute("SELECT 2", connection="analytics")
        assert fake_executor.calls[1].options.roles == ("role_viz_reader",)

    async def test_registry_creates_clients_without_automatic_session_id(self) -> None:
        """The registry passes an explicit ``autogenerate_session_id=False`` to the driver."""
        factory = RecordingClientFactory()
        config = make_sql_config(analytics=make_connection_config())
        async with ClickHouseConnectionRegistry(config=config, client_factory=factory):
            pass
        assert factory.calls[0]["autogenerate_session_id"] is False

    def test_execution_options_are_immutable(self) -> None:
        """``SqlExecutionOptions`` is frozen: the roles cannot be mutated after creation."""
        options = SqlExecutionOptions(roles=("role_viz_reader",))
        with pytest.raises(AttributeError):
            options.roles = ("role_admin_evil",)  # type: ignore[misc]
