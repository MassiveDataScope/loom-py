"""Doubles shared by the call-time handle tests."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import msgspec

from loom.ai.abc import AgentEvent, Conversation, FinalEvent, HealthStatus
from loom.ai.compiler import CompiledSqlCapability
from loom.core.execution import execution_identity
from loom.core.identity import Identity
from loom.core.stores.sql.abc import SqlColumn, SqlQueryResult
from loom.core.stores.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.stores.sql.service import SqlQueryService
from tests.integration.ai.conftest import DEFAULT_USAGE, make_sql_capability

AGENT = "acme-triage"
SERVER = "acme-crm"
CONNECTION = "acme-reporting"
TOOL = "find_orders"

ALICE = Identity(subject="acme-alice", roles=("analyst",), mechanism="test")
BOB = Identity(subject="acme-bob", roles=("auditor",), mechanism="test")
NO_SUBJECT = Identity(subject="", roles=("analyst",), mechanism="test")


class AcmeEngine:
    """One successful turn per run, recording who the runtime ran it as and who was acting."""

    def __init__(self) -> None:
        self.identities: list[Identity] = []
        self.acting: list[Identity] = []

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> object:
        del prompt, conversation, state
        self.identities.append(identity)
        self.acting.append(execution_identity())

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            async def _events() -> AsyncIterator[AgentEvent]:
                yield FinalEvent(output={"ok": True}, usage=DEFAULT_USAGE)

            yield _events()

        return _stream()

    def native(
        self,
        *,
        identity: Identity,
        state: object | None = None,
        guard: Callable[[], AbstractAsyncContextManager[None]],
    ) -> object:
        del state, guard
        self.identities.append(identity)
        self.acting.append(execution_identity())
        return object()

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


class AcmeSqlService(SqlQueryService):
    """Records the roles of every query; answers as a grant table keyed by subject would.

    Holds a row for the empty subject, so a caller with no subject that
    reached it would read that row.
    """

    def __init__(self) -> None:
        super().__init__(executors={}, config=SqlConfig(connections={}))
        self.calls: list[tuple[str, ...]] = []
        self.rows_by_subject: Mapping[str, tuple[int, ...]] = {
            "": (0,),
            ALICE.subject: (1,),
            BOB.subject: (2,),
        }

    async def execute(
        self,
        sql: str,
        *,
        connection: str,
        roles: Sequence[str] | None = None,
        parameters: Mapping[str, Any] | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> SqlQueryResult:
        del sql, connection, parameters, limit, offset
        self.calls.append(tuple(roles or ()))
        return SqlQueryResult(
            columns=(SqlColumn(name="id", type="Int64"),),
            rows=(self.rows_by_subject[execution_identity().subject],),
            row_count=1,
            limit=1000,
            offset=0,
            has_more=False,
            elapsed_ms=1.0,
        )


def analyst_sql_capability() -> CompiledSqlCapability:
    """The ``sql`` grant of the agent under test, allowing the ``analyst`` and ``auditor`` roles."""
    capability = make_sql_capability(CONNECTION)
    return msgspec.structs.replace(
        capability,
        config=SqlConnectionConfig(
            backend="clickhouse",
            url=capability.config.url,
            allowed_roles=("analyst", "auditor"),
            readonly=True,
        ),
    )


Call = Callable[[], Awaitable[object]]
"""One handle call, bound to its handle or view already and made when awaited."""


def run_agent(triage: Any, crm: Any) -> Call:
    """Bind a run of the agent through its handle."""
    del crm
    return lambda: triage.run("assess")


def open_native(triage: Any, crm: Any) -> Call:
    """Bind a request for the agent's engine-native objects through its handle."""
    del crm

    async def native() -> object:
        return triage.native()

    return native


def call_mcp(triage: Any, crm: Any) -> Call:
    """Bind an untyped call of the granted tool through the ``Mcp()`` handle."""
    del triage
    return lambda: crm.call_untyped(TOOL, {})


def call_mcp_typed(triage: Any, crm: Any) -> Call:
    """Bind a typed call of the granted tool through the ``Mcp()`` handle."""
    del triage
    return lambda: crm.call(TOOL, {}, expect=dict)


def call_granted_mcp(triage: Any, crm: Any) -> Call:
    """Build the agent's own ``mcp`` grant view now and bind an untyped call through it."""
    del crm
    view = triage.mcp(SERVER)
    return lambda: view.call_untyped(TOOL, {})


def query_sql(triage: Any, crm: Any) -> Call:
    """Build the agent's ``sql`` grant view now and bind a query through it."""
    del crm
    view = triage.sql(CONNECTION)
    return lambda: view.query("select id from orders")


HANDLE_CALLS = {
    "agent": run_agent,
    "native": open_native,
    "mcp": call_mcp,
    "mcp-typed": call_mcp_typed,
    "granted-mcp": call_granted_mcp,
    "sql": query_sql,
}
"""Every handle call under test, by name: bound when called, made when the result is awaited."""
