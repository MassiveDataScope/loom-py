"""Container wiring of the SQL collaborators registered by ``create_app``."""

from __future__ import annotations

import pytest

from loom.composition._sql import SqlWiring, register_sql_collaborators
from loom.core.di.container import LoomContainer
from loom.core.identity import Identity
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql import CallerBoundSql, NullSqlQueryService, SqlQueryService
from loom.core.sql.abc import RolesNotBoundError
from tests.unit.core.sql._fakes import FakeSqlExecutor, make_connection_config, make_sql_config


class _RecordingObserver:
    """Lifecycle observer capturing the audit events of the registered instance."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


def _wiring() -> SqlWiring:
    config = make_sql_config(analytics=make_connection_config())
    service = SqlQueryService(executors={"analytics": FakeSqlExecutor()}, config=config)
    return SqlWiring(config=config, registry=None, service=service)


def test_registers_both_the_unbound_and_the_caller_bound_collaborators() -> None:
    """A use case may inject either path, so both must be resolvable."""
    container = LoomContainer()
    register_sql_collaborators(container, _wiring(), ObservabilityRuntime.noop())
    assert isinstance(container.resolve(SqlQueryService), SqlQueryService)
    assert isinstance(container.resolve(CallerBoundSql), CallerBoundSql)


def test_registers_the_caller_bound_collaborator_without_a_sql_section() -> None:
    """Resolution never depends on the config being present."""
    container = LoomContainer()
    register_sql_collaborators(
        container,
        SqlWiring(config=None, registry=None, service=NullSqlQueryService()),
        ObservabilityRuntime.noop(),
    )
    assert isinstance(container.resolve(CallerBoundSql), CallerBoundSql)


async def test_the_registered_collaborator_binds_the_roles_to_the_identity() -> None:
    """The instance in the container is the bound one, not a passthrough."""
    container = LoomContainer()
    register_sql_collaborators(container, _wiring(), ObservabilityRuntime.noop())
    sql = container.resolve(CallerBoundSql)
    identity = Identity(subject="user-1", roles=("role_intruder",), mechanism="jwt")
    with pytest.raises(RolesNotBoundError):
        await sql.execute("SELECT 1", connection="analytics", identity=identity)


async def test_the_registered_collaborator_audits_with_the_app_runtime() -> None:
    """The span must reach the app's observers, not a runtime built on the side."""
    observer = _RecordingObserver()
    container = LoomContainer()
    register_sql_collaborators(container, _wiring(), ObservabilityRuntime([observer]))
    sql = container.resolve(CallerBoundSql)
    await sql.execute(
        "SELECT 1",
        connection="analytics",
        identity=Identity(subject="user-1", roles=("role_viz_sales",), mechanism="jwt"),
    )
    started = next(e for e in observer.events if e.kind == EventKind.START)
    assert started.scope == Scope.READ
    assert started.meta["roles"] == "role_viz_sales"
