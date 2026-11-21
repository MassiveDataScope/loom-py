"""Container wiring of the SQL collaborators registered by ``create_app``."""

from __future__ import annotations

import pytest

from loom.composition._sql import SqlWiring, register_sql_collaborators
from loom.core.di.container import LoomContainer
from loom.core.identity import Identity
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.observability import Observability
from loom.core.stores.sql import CallerBoundSql, NullSqlQueryService, SqlQueryService
from loom.core.stores.sql.abc import RolesNotBoundError
from tests.helpers.spans import build_recorder
from tests.unit.core.sql._fakes import FakeSqlExecutor, make_connection_config, make_sql_config


class RecordingAuditObserver:
    """Lifecycle observer capturing the audit events of the registered instance."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


def _wiring() -> SqlWiring:
    config = make_sql_config(analytics=make_connection_config())
    service = SqlQueryService(executors={"analytics": FakeSqlExecutor()}, config=config)
    return SqlWiring(config=config, registry=None, service=service)


class TestSqlCollaboratorRegistration:
    def test_registers_both_the_unbound_and_the_caller_bound_collaborators(
        self,
    ) -> None:
        """A use case may inject either path, so both must be resolvable."""
        container = LoomContainer()
        register_sql_collaborators(container, _wiring(), Observability.noop())
        assert isinstance(container.resolve(SqlQueryService), SqlQueryService)
        assert isinstance(container.resolve(CallerBoundSql), CallerBoundSql)

    def test_registers_the_caller_bound_collaborator_without_a_sql_section(
        self,
    ) -> None:
        """Resolution never depends on the config being present."""
        container = LoomContainer()
        register_sql_collaborators(
            container,
            SqlWiring(config=None, registry=None, service=NullSqlQueryService()),
            Observability.noop(),
        )
        assert isinstance(container.resolve(CallerBoundSql), CallerBoundSql)

    async def test_the_registered_collaborator_binds_the_roles_to_the_identity(
        self,
    ) -> None:
        """The instance in the container is the bound one, not a passthrough."""
        container = LoomContainer()
        register_sql_collaborators(container, _wiring(), Observability.noop())
        sql = container.resolve(CallerBoundSql)
        identity = Identity(subject="user-1", roles=("role_intruder",), mechanism="jwt")
        with pytest.raises(RolesNotBoundError):
            await sql.execute("SELECT 1", connection="analytics", identity=identity)

    async def test_the_registered_collaborator_audits_with_the_app_runtime(
        self,
    ) -> None:
        """The span must reach the app's observers, not a runtime built on the side."""
        recorder = build_recorder()
        container = LoomContainer()
        register_sql_collaborators(container, _wiring(), recorder.runtime)
        sql = container.resolve(CallerBoundSql)
        await sql.execute(
            "SELECT 1",
            connection="analytics",
            identity=Identity(subject="user-1", roles=("role_viz_sales",), mechanism="jwt"),
        )
        started = next(e for e in recorder.collector.events if e.kind == EventKind.START)
        assert started.scope == Scope.READ
        assert (recorder.one("read:sql:analytics").attributes or {})["roles"] == "role_viz_sales"
