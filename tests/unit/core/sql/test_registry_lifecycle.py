"""ClickHouse connection registry lifecycle tests — async CM (spec §2/§7.5)."""

from __future__ import annotations

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError

from loom.core.config.errors import ConfigError
from loom.core.sql.clickhouse import ClickHouseConnectionRegistry
from loom.core.sql.config import SqlConnectionConfig
from tests.unit.core.sql._fakes import (
    FakeClickHouseClient,
    RecordingClientFactory,
    make_connection_config,
    make_sql_config,
)


class _FailingCloseClient(FakeClickHouseClient):
    """Client whose ``close`` always fails — exercises the isolated shutdown path."""

    async def close(self) -> None:
        raise RuntimeError("close failed")


def _registry(
    factory: RecordingClientFactory, **connections: SqlConnectionConfig
) -> ClickHouseConnectionRegistry:
    return ClickHouseConnectionRegistry(
        config=make_sql_config(**connections),
        client_factory=factory,
    )


async def test_creates_one_client_per_connection_on_enter() -> None:
    """Entering the context creates exactly one client per configured connection."""
    factory = RecordingClientFactory()
    registry = _registry(
        factory,
        analytics=make_connection_config(),
        reporting=make_connection_config(),
    )
    async with registry:
        assert len(factory.created) == 2


async def test_exposes_one_executor_per_connection_inside_the_context() -> None:
    """Inside the context every configured connection has its executor."""
    factory = RecordingClientFactory()
    registry = _registry(factory, analytics=make_connection_config())
    async with registry:
        assert registry.executor("analytics") is not None


async def test_closes_all_clients_on_context_exit() -> None:
    """Leaving the context closes every created client (no leaks)."""
    factory = RecordingClientFactory()
    registry = _registry(
        factory,
        analytics=make_connection_config(),
        reporting=make_connection_config(),
    )
    async with registry:
        pass
    assert [client.closed for client in factory.created] == [True, True]


async def test_one_failing_close_does_not_prevent_closing_the_rest() -> None:
    """Each client closes in isolation: a failing close never leaks the others."""
    factory = RecordingClientFactory(_FailingCloseClient(), FakeClickHouseClient())
    registry = _registry(
        factory,
        analytics=make_connection_config(),
        reporting=make_connection_config(),
    )
    async with registry:
        pass
    assert factory.created[1].closed is True


async def test_probe_failure_with_an_incidental_511_still_aborts_startup() -> None:
    """Only the ``Code: 511`` marker satisfies the probe — an incidental 511 does not."""
    error = DatabaseError("Code: 241. DB::Exception: Memory limit exceeded: would use 511 MiB")
    factory = RecordingClientFactory(FakeClickHouseClient(error=error))
    registry = _registry(factory, analytics=make_connection_config())
    with pytest.raises(ConfigError, match="probe failed unexpectedly"):
        async with registry:
            pass


async def test_startup_probe_sends_a_sentinel_role_in_settings() -> None:
    """The startup probe runs a query carrying a nonexistent sentinel role."""
    client = FakeClickHouseClient()
    factory = RecordingClientFactory(client)
    async with _registry(factory, analytics=make_connection_config()):
        pass
    assert any(call.settings is not None and "role" in call.settings for call in client.queries)


async def test_aborts_startup_when_the_backend_ignores_the_role() -> None:
    """If the sentinel role does NOT produce error 511, the probe aborts (fail-closed)."""
    factory = RecordingClientFactory(FakeClickHouseClient(ignore_role=True))
    registry = _registry(factory, analytics=make_connection_config())
    with pytest.raises(ConfigError):
        async with registry:
            pass


async def test_aborts_startup_when_role_is_not_a_transport_setting() -> None:
    """Without ``role`` in the client ``valid_transport_settings`` startup fails."""
    factory = RecordingClientFactory(
        FakeClickHouseClient(transport_settings=frozenset({"readonly", "limit"}))
    )
    registry = _registry(factory, analytics=make_connection_config())
    with pytest.raises(ConfigError):
        async with registry:
            pass


async def test_closes_created_clients_when_the_probe_fails() -> None:
    """A probe failure leaves no clients open behind (guaranteed cleanup)."""
    factory = RecordingClientFactory(
        FakeClickHouseClient(ignore_role=True),
        FakeClickHouseClient(ignore_role=True),
    )
    registry = _registry(
        factory,
        analytics=make_connection_config(),
        reporting=make_connection_config(),
    )
    with pytest.raises(ConfigError):
        async with registry:
            pass
    assert all(client.closed for client in factory.created)


async def test_passes_explicit_connection_timeouts_to_the_driver() -> None:
    """``connect_timeout`` and ``read_timeout`` travel explicitly to the driver."""
    factory = RecordingClientFactory()
    registry = _registry(
        factory,
        analytics=make_connection_config(connect_timeout=5, read_timeout=42),
    )
    async with registry:
        pass
    assert (
        factory.calls[0]["connect_timeout"],
        factory.calls[0]["send_receive_timeout"],
    ) == (5, 42)


async def test_passes_explicit_credentials_to_the_driver() -> None:
    """``username``/``password`` travel as driver arguments, never inside the DSN."""
    factory = RecordingClientFactory()
    registry = _registry(
        factory,
        analytics=make_connection_config(username="mcp_reader", password="p4ss#hash"),
    )
    async with registry:
        pass
    assert (
        factory.calls[0]["username"],
        factory.calls[0]["password"],
    ) == ("mcp_reader", "p4ss#hash")


async def test_omits_credential_arguments_when_not_configured() -> None:
    """Absent credentials are not forwarded, so the DSN's own ones still apply."""
    factory = RecordingClientFactory()
    registry = _registry(factory, analytics=make_connection_config())
    async with registry:
        pass
    assert ("username" in factory.calls[0], "password" in factory.calls[0]) == (False, False)


async def test_using_the_registry_outside_the_context_fails_explicitly() -> None:
    """Requesting an executor without entering the context is an explicit error."""
    registry = _registry(RecordingClientFactory(), analytics=make_connection_config())
    with pytest.raises(RuntimeError):
        registry.executor("analytics")


async def test_check_probes_on_temporary_clients_and_closes_them() -> None:
    """``check`` verifies and probes without leaving any client or executor behind."""
    factory = RecordingClientFactory()
    registry = _registry(factory, analytics=make_connection_config())

    await registry.check()

    assert [client.closed for client in factory.created] == [True]
    with pytest.raises(RuntimeError):
        registry.executor("analytics")


async def test_check_failure_closes_every_temporary_client() -> None:
    """A probe failure in ``check`` still closes the clients it created."""
    factory = RecordingClientFactory(FakeClickHouseClient(), FakeClickHouseClient(ignore_role=True))
    registry = _registry(
        factory,
        analytics=make_connection_config(),
        reporting=make_connection_config(),
    )
    with pytest.raises(ConfigError):
        await registry.check()
    assert all(client.closed for client in factory.created)


async def test_entering_after_a_passed_check_keeps_the_driver_checks() -> None:
    """After ``check``, entering skips the probe but still refuses a driver without roles."""
    factory = RecordingClientFactory(
        FakeClickHouseClient(),
        FakeClickHouseClient(transport_settings=frozenset({"readonly", "limit"})),
    )
    registry = _registry(factory, analytics=make_connection_config())
    await registry.check()

    with pytest.raises(ConfigError, match="per-query transport setting"):
        async with registry:
            pass
    assert factory.created[1].queries == []
