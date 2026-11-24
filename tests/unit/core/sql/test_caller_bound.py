"""Identity-bound SQL execution tests for ``CallerBoundSql``.

The collaborator exists so a use case cannot query with roles its caller does
not hold: the roles come from the verified identity and from nowhere else.
"""

from __future__ import annotations

import inspect

import pytest

from loom.core.config.errors import ConfigError
from loom.core.identity import ANONYMOUS, Identity
from loom.core.identity.context import IdentityKey
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.observability import Observability
from loom.core.stores.event import StoresEvent
from loom.core.stores.sql.abc import RolesNotBoundError, UnknownConnectionError
from loom.core.stores.sql.caller_bound import CallerBoundSql
from loom.core.stores.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.stores.sql.service import NullSqlQueryService, SqlQueryService
from tests.helpers.loggers import CapturedLog
from tests.unit.core.sql._fakes import FakeSqlExecutor, make_connection_config, make_sql_config


class AcmeSqlRecordingObserver:
    """Lifecycle observer capturing every event the audit span emits."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


def _caller_bound(
    executor: FakeSqlExecutor,
    connection: SqlConnectionConfig,
    *,
    observability: Observability | None = None,
) -> CallerBoundSql:
    """Build a ``CallerBoundSql`` over a fake 'analytics' connection."""
    config = make_sql_config(analytics=connection)
    service = SqlQueryService(executors={"analytics": executor}, config=config)
    return CallerBoundSql(service, config, observability)


def _identity(*roles: str) -> Identity:
    return Identity(subject="user-1", roles=roles, mechanism="jwt")


class TestCallerBoundRoles:
    async def test_runs_with_the_roles_the_identity_holds(
        self, fake_executor: FakeSqlExecutor
    ) -> None:
        """The effective roles are the ones carried by the verified identity."""
        sql = _caller_bound(fake_executor, make_connection_config())
        await sql.execute("SELECT 1", connection="analytics", identity=_identity("role_viz_sales"))
        assert fake_executor.calls[0].options.roles == ("role_viz_sales",)

    async def test_keeps_only_the_identity_roles_that_are_allowlisted(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """A role the identity holds but the connection does not allowlist is dropped."""
        sql = _caller_bound(fake_executor, make_connection_config())
        await sql.execute(
            "SELECT 1",
            connection="analytics",
            identity=_identity("role_viz_sales", "role_intruder"),
        )
        assert fake_executor.calls[0].options.roles == ("role_viz_sales",)

    async def test_rejects_an_identity_holding_no_allowlisted_role_before_the_backend(
        self, fake_executor: FakeSqlExecutor, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The caller's entitlements decide before execution; the denial names the caller."""
        sql = _caller_bound(fake_executor, make_connection_config())
        identity = _identity("role_intruder")
        with pytest.raises(RolesNotBoundError):
            await sql.execute("SELECT 1", connection="analytics", identity=identity)
        assert fake_executor.calls == []
        denials = CapturedLog(caplog).named(StoresEvent.ROLE_AUTHORIZATION_DENIED)
        assert [fields[IdentityKey.SUBJECT] for fields in denials] == [identity.subject]

    async def test_rejects_an_identity_carrying_no_role(
        self, fake_executor: FakeSqlExecutor
    ) -> None:
        """An authenticated caller without roles cannot borrow any."""
        sql = _caller_bound(fake_executor, make_connection_config())
        identity = _identity()
        with pytest.raises(RolesNotBoundError):
            await sql.execute("SELECT 1", connection="analytics", identity=identity)
        assert fake_executor.calls == []

    async def test_rejects_an_anonymous_caller(self, fake_executor: FakeSqlExecutor) -> None:
        """No verified identity means no query, whatever the connection allows."""
        sql = _caller_bound(fake_executor, make_connection_config())
        with pytest.raises(RolesNotBoundError):
            await sql.execute("SELECT 1", connection="analytics", identity=ANONYMOUS)
        assert fake_executor.calls == []

    async def test_never_falls_back_to_the_shared_default_role(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """Holding the shared role does not help when the connection does not allowlist it."""
        connection = make_connection_config(
            allowed_roles=("role_viz_sales",), default_role="role_viz_reader"
        )
        sql = _caller_bound(fake_executor, connection)
        identity = _identity("role_viz_reader")
        with pytest.raises(RolesNotBoundError):
            await sql.execute("SELECT 1", connection="analytics", identity=identity)
        assert fake_executor.calls == []

    async def test_refuses_a_connection_whose_allowlist_is_empty(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """A single-role connection has nothing to bind to an identity, so it is refused."""
        connection = make_connection_config(allowed_roles=(), default_role="role_viz_reader")
        sql = _caller_bound(fake_executor, connection)
        identity = _identity("role_viz_reader")
        with pytest.raises(RolesNotBoundError):
            await sql.execute("SELECT 1", connection="analytics", identity=identity)
        assert fake_executor.calls == []

    def test_execute_accepts_no_roles_argument(self) -> None:
        """No parameter may let the calling code choose the roles — that is the whole point."""
        parameters = inspect.signature(CallerBoundSql.execute).parameters
        assert "roles" not in parameters
        assert parameters["identity"].default is inspect.Parameter.empty

    async def test_forwards_parameters_limit_and_offset(
        self, fake_executor: FakeSqlExecutor
    ) -> None:
        """Everything but the roles behaves exactly as in the unbound service."""
        sql = _caller_bound(fake_executor, make_connection_config(max_limit=100))
        await sql.execute(
            "SELECT 1",
            connection="analytics",
            identity=_identity("role_viz_sales"),
            parameters={"id": 7},
            limit=500,
            offset=20,
        )
        call = fake_executor.calls[0]
        assert call.parameters == {"id": 7}
        assert call.options.limit == 100
        assert call.options.offset == 20

    async def test_unknown_connection_is_reported_as_such(
        self, fake_executor: FakeSqlExecutor
    ) -> None:
        """An unconfigured connection name fails with the domain error of the pillar."""
        sql = _caller_bound(fake_executor, make_connection_config())
        identity = _identity("role_viz_sales")
        with pytest.raises(UnknownConnectionError):
            await sql.execute("SELECT 1", connection="unknown", identity=identity)
        assert fake_executor.calls == []

    async def test_without_a_sql_section_the_actionable_config_error_surfaces(self) -> None:
        """Wrapping the null service keeps its actionable error instead of hiding it."""
        sql = CallerBoundSql(NullSqlQueryService(), SqlConfig(connections={}))
        identity = _identity("role_viz_sales")
        with pytest.raises(ConfigError, match="sql"):
            await sql.execute("SELECT 1", connection="analytics", identity=identity)

    async def test_a_configured_connection_without_an_executor_refuses_on_the_roles(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """The roles are checked first, so the refusal names them, not the connection."""
        config = make_sql_config(analytics=make_connection_config())
        service = SqlQueryService(executors={}, config=config)
        sql = CallerBoundSql(service, config)
        identity = _identity("role_intruder")
        with pytest.raises(RolesNotBoundError):
            await sql.execute("SELECT 1", connection="analytics", identity=identity)
        assert fake_executor.calls == []

    async def test_an_accepted_query_emits_a_span_labelled_with_roles_never_the_subject(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """The bound path records the labels the REST endpoint guarantees, on its own span."""
        observer = AcmeSqlRecordingObserver()
        sql = _caller_bound(
            fake_executor, make_connection_config(), observability=Observability([observer])
        )
        await sql.execute("SELECT 1", connection="analytics", identity=_identity("role_viz_sales"))
        started = next(e for e in observer.events if e.kind == EventKind.START)
        assert started.scope == Scope.READ
        assert started.name == "sql:analytics"
        assert started.meta["roles"] == "role_viz_sales"
        assert "subject" not in started.meta
        assert started.meta["mechanism"] == "jwt"

    async def test_the_span_names_only_the_roles_the_query_runs_with(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """A dropped role must not appear in the audit trail as if it had applied."""
        observer = AcmeSqlRecordingObserver()
        sql = _caller_bound(
            fake_executor, make_connection_config(), observability=Observability([observer])
        )
        await sql.execute(
            "SELECT 1",
            connection="analytics",
            identity=_identity("role_viz_sales", "role_intruder"),
        )
        started = next(e for e in observer.events if e.kind == EventKind.START)
        assert started.meta["roles"] == "role_viz_sales"

    async def test_a_refused_query_emits_no_span(self, fake_executor: FakeSqlExecutor) -> None:
        """No query ran, so there is nothing to attribute to any set of roles."""
        observer = AcmeSqlRecordingObserver()
        sql = _caller_bound(
            fake_executor, make_connection_config(), observability=Observability([observer])
        )
        identity = _identity("role_intruder")
        with pytest.raises(RolesNotBoundError):
            await sql.execute("SELECT 1", connection="analytics", identity=identity)
        assert observer.events == []

    async def test_without_observability_the_query_runs_and_records_nothing(
        self,
        fake_executor: FakeSqlExecutor,
    ) -> None:
        """The span is an addition, never a requirement of the bound path.

        The twin of this case with a runtime is
        ``test_an_accepted_query_emits_a_span_labelled_with_roles_never_the_subject``;
        what this one pins is that dropping the runtime costs the caller nothing
        but the record, so the observer built here must stay empty.
        """
        observer = AcmeSqlRecordingObserver()
        sql = _caller_bound(fake_executor, make_connection_config())
        await sql.execute("SELECT 1", connection="analytics", identity=_identity("role_viz_sales"))
        assert fake_executor.calls[0].options.roles == ("role_viz_sales",)
        assert observer.events == []
