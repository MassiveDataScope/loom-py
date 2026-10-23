"""End-to-end coverage of the grant handles and the three run modes (spec 014, PR3).

Drives the whole path a consumer would, exactly as ``test_agent_marker_integration.py``
does for PR2: a real ``UseCaseCompiler`` compiles a use case declaring ``Agent()``, a
real ``RuntimeExecutor`` runs it, and a real ``AgentRuntime`` serves the named agent —
only the model and the MCP/SQL network are faked, at the edge.

Covers points 3, 4 and 5 of the standing integration requirement in
``specs/014-model-as-actor/tasks.md``: the three run modes over one artefact; a
granted tool call filtered by the artefact's own grant, with an excluded tool
refused before any network call; and a granted query run with the caller's own
roles under the grant's bounds.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from typing import Any

import msgspec
import pytest

from loom.ai.abc import AgentHandle
from loom.ai.compiler._plan import CompiledCapability
from loom.ai.config import AiConfig, McpServerConfig
from loom.ai.declarative import PolicySpec
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import agent_marker_resolver
from loom.core.di import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.observability.event import Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.abc.contracts import SqlColumn, SqlQueryResult
from loom.core.sql.config import SqlConfig, SqlConnectionConfig
from loom.core.sql.service import NullSqlQueryService, SqlQueryService
from loom.core.use_case import Agent, UseCase
from tests.integration.ai.conftest import (
    MARKER_AGENT_NAME,
    CountingEngineProvider,
    RecordingMcpSession,
    RecordingObserver,
    ShapedRecordingEngine,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_plan,
    make_policies,
    make_sql_capability,
    make_sql_config,
    marker_use_case_executor,
    mcp_client_factory,
)

_ALICE = Identity(subject="alice", roles=("analyst",), mechanism="test")
_MCP_SERVER = "tools"
_SQL_CONNECTION = "reporting"


class ThreeModesUseCase(UseCase[object, dict[str, object]]):
    """One artefact, run through its three modes (T304)."""

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> dict[str, object]:
        declared = await triage.run("assess this")
        overridden = await triage.run("assess this", expect=dict)
        prose = await triage.run_text("assess this")
        return {
            "declared": declared.output,
            "overridden": overridden.output,
            "prose": prose.output,
        }


class GrantedToolUseCase(UseCase[object, dict[str, object]]):
    """Calls a tool the artefact's own grant admits (T301)."""

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> dict[str, object]:
        result = await triage.mcp(_MCP_SERVER).call_untyped("read_orders", {"customer": "acme"})
        return dict(result)


class RejectedToolUseCase(UseCase[object, None]):
    """Calls a tool the artefact's own grant excludes (T301)."""

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> None:
        await triage.mcp(_MCP_SERVER).call_untyped("write_orders", {"customer": "acme"})


class GrantedQueryUseCase(UseCase[object, Sequence[Mapping[str, Any]]]):
    """Queries the artefact's own bounded ``sql`` grant (T303)."""

    async def execute(
        self,
        triage: AgentHandle[dict[str, Any]] = Agent(MARKER_AGENT_NAME),
    ) -> Sequence[Mapping[str, Any]]:
        return await triage.sql(_SQL_CONNECTION).query("select * from orders")


class RecordingSqlQueryService(SqlQueryService):
    """A minimal ``SqlQueryService`` double recording the roles it was called with."""

    def __init__(self, result: SqlQueryResult) -> None:
        super().__init__(executors={}, config=SqlConfig(connections={}))
        self._result = result
        self.calls: list[tuple[str, ...]] = []

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
        return self._result


def _sql_result() -> SqlQueryResult:
    return SqlQueryResult(
        columns=(SqlColumn(name="id", type="Int64"),),
        rows=((1,), (2,)),
        row_count=2,
        limit=1000,
        offset=0,
        has_more=False,
        elapsed_ms=1.0,
    )


def _granted_mcp_capability() -> CompiledCapability:
    return make_mcp_capability(_MCP_SERVER, exclude=("write_*",))


def _granted_sql_capability() -> CompiledCapability:
    capability = make_sql_capability(_SQL_CONNECTION)
    return msgspec.structs.replace(
        capability,
        config=SqlConnectionConfig(
            backend="clickhouse",
            url=capability.config.url,
            allowed_roles=("analyst",),
            readonly=True,
        ),
    )


def _ai_config(*, granted_mcp: bool) -> AiConfig:
    mcp_servers = (
        {_MCP_SERVER: McpServerConfig(url="https://tools.internal/mcp")} if granted_mcp else {}
    )
    return make_ai_config(mcp_servers=mcp_servers)


def _wired(
    *,
    engine: ShapedRecordingEngine,
    compiler_and_executor: tuple[UseCaseCompiler, RuntimeExecutor],
    deps: StubDepsFactory,
    container: LoomContainer,
    capabilities: Sequence[CompiledCapability] = (),
    mcp_clients: dict[str, StubMcpClient] | None = None,
    granted_sql: bool = False,
    policies: PolicySpec | None = None,
) -> tuple[RuntimeExecutor, AgentRuntime]:
    """Wire a real executor to a real ``AgentRuntime``, exactly as ``create_app`` does."""
    _, executor = compiler_and_executor
    provider = CountingEngineProvider(engines={MARKER_AGENT_NAME: engine})
    runtime = AgentRuntime(
        plans=[make_plan(MARKER_AGENT_NAME, capabilities=capabilities, policies=policies)],
        config=_ai_config(granted_mcp=bool(mcp_clients)),
        engine_provider=provider,  # type: ignore[arg-type]
        deps=deps,
        container=container,
        mcp_client_factory=mcp_client_factory(mcp_clients or {}),  # type: ignore[arg-type]
        sql_config=make_sql_config(_SQL_CONNECTION) if granted_sql else None,
    )
    sql_query_service = (
        container.resolve(SqlQueryService)
        if container.is_registered(SqlQueryService)
        else NullSqlQueryService()
    )
    executor.bind_agent_resolver(
        agent_marker_resolver(runtime, sql_query_service=sql_query_service, observability=None)
    )
    return executor, runtime


class TestTheThreeModesOverOneArtefact:
    """Standing requirement 3: the declared shape, the overridden one, and open text."""

    async def test_all_three_runs_reach_the_real_engine(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(ThreeModesUseCase),
            deps=deps,
            container=container,
        )

        async with runtime:
            result = await executor.execute(ThreeModesUseCase(), identity=_ALICE)

        assert result["declared"] == {"answer": "42"}
        assert result["overridden"] == {"answer": "42"}
        assert result["prose"] == {"answer": "42"}
        assert engine.shaped_calls == [dict, str]
        assert len(engine.identities) == 3


class TestACallFilteredByTheGrant:
    """Standing requirement 4: the grant's filter, and the refusal before the network."""

    async def test_an_admitted_tool_reaches_the_server(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        session = RecordingMcpSession(
            tools=("read_orders", "write_orders"),
            results={"read_orders": {"orders": [1, 2, 3]}},
        )
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(GrantedToolUseCase),
            deps=deps,
            container=container,
            capabilities=(_granted_mcp_capability(),),
            mcp_clients={_MCP_SERVER: StubMcpClient(label=_MCP_SERVER, session=session, log=[])},
        )

        async with runtime:
            result = await executor.execute(GrantedToolUseCase(), identity=_ALICE)

        assert result == {"orders": [1, 2, 3]}
        assert session.calls == [("read_orders", {"customer": "acme"})]

    async def test_an_excluded_tool_is_refused_without_calling_the_server(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        session = RecordingMcpSession(tools=("read_orders", "write_orders"))
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(RejectedToolUseCase),
            deps=deps,
            container=container,
            capabilities=(_granted_mcp_capability(),),
            mcp_clients={_MCP_SERVER: StubMcpClient(label=_MCP_SERVER, session=session, log=[])},
        )

        use_case = RejectedToolUseCase()
        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await executor.execute(use_case, identity=_ALICE)

        assert excinfo.value.code is AgentRunErrorCode.TOOL_UNKNOWN
        assert session.calls == []


class TestAQueryWithTheCallersRoles:
    """Standing requirement 5: the query runs with the verified caller's roles."""

    async def test_the_query_runs_with_the_callers_role(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        service = RecordingSqlQueryService(_sql_result())
        container.register_instance(SqlQueryService, service)
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(GrantedQueryUseCase),
            deps=deps,
            container=container,
            capabilities=(_granted_sql_capability(),),
            granted_sql=True,
        )

        async with runtime:
            rows = await executor.execute(GrantedQueryUseCase(), identity=_ALICE)

        assert list(rows) == [{"id": 1}, {"id": 2}]
        assert service.calls == [("analyst",)]


class TestAgentPathIncludeIsWiredToo:
    """PR3 moved include/exclude/agent/timeout to the AgentHandle.mcp() call site.

    Nothing else in this module ever passes a narrowing ``include`` (every
    other capability here uses ``exclude`` instead), so these are the only
    tests standing between the agent path's own filter and a mutation that
    drops it and admits the whole catalogue.
    """

    async def test_the_agent_handles_own_include_still_narrows(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        session = RecordingMcpSession(tools=("read_orders", "write_orders"))
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(GrantedToolUseCase),
            deps=deps,
            container=container,
            capabilities=(make_mcp_capability(_MCP_SERVER, include=("read_*",)),),
            mcp_clients={_MCP_SERVER: StubMcpClient(label=_MCP_SERVER, session=session, log=[])},
        )

        async with runtime:
            resolver = agent_marker_resolver(
                runtime, sql_query_service=NullSqlQueryService(), observability=None
            )
            handle = resolver(MARKER_AGENT_NAME, _ALICE)
            assert handle.mcp(_MCP_SERVER).tools() == ("read_orders",)

    async def test_the_agent_tool_span_still_says_agent_not_mcp_server(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        session = RecordingMcpSession(
            tools=("read_orders",), results={"read_orders": {"orders": []}}
        )
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(GrantedToolUseCase),
            deps=deps,
            container=container,
            capabilities=(_granted_mcp_capability(),),
            mcp_clients={_MCP_SERVER: StubMcpClient(label=_MCP_SERVER, session=session, log=[])},
        )
        observer = RecordingObserver()
        observability = ObservabilityRuntime(observers=[observer])

        async with runtime:
            resolver = agent_marker_resolver(
                runtime, sql_query_service=NullSqlQueryService(), observability=observability
            )
            handle = resolver(MARKER_AGENT_NAME, _ALICE)
            await handle.mcp(_MCP_SERVER).call_untyped("read_orders", {})

        tool_spans = [e.meta or {} for e in observer.events if e.scope is Scope.TOOL]
        assert tool_spans
        for meta in tool_spans:
            assert meta.get("agent") == MARKER_AGENT_NAME, meta
            assert "mcp_server" not in meta

    async def test_the_agent_deadline_is_the_plans_own_not_the_servers(
        self, deps: StubDepsFactory, container: LoomContainer
    ) -> None:
        """The plan's ``tool_timeout_ms`` (10ms) bounds the call, not the server's own (20s).

        A swap of the two timeouts at the ``mcp()`` call site would let this
        call sail through un-bounded by anything short enough for a test to
        wait for, so this pins which one actually governs.
        """

        class SlowSession(RecordingMcpSession):
            async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> Any:  # noqa: ANN401
                await asyncio.sleep(0.05)
                return await super().call_tool(name, arguments)

        session = SlowSession(tools=("read_orders",), results={"read_orders": {"orders": []}})
        engine = ShapedRecordingEngine()
        executor, runtime = _wired(
            engine=engine,
            compiler_and_executor=marker_use_case_executor(GrantedToolUseCase),
            deps=deps,
            container=container,
            capabilities=(_granted_mcp_capability(),),
            mcp_clients={_MCP_SERVER: StubMcpClient(label=_MCP_SERVER, session=session, log=[])},
            policies=make_policies(tool_timeout_ms=10),
        )

        async with runtime:
            resolver = agent_marker_resolver(
                runtime, sql_query_service=NullSqlQueryService(), observability=None
            )
            handle = resolver(MARKER_AGENT_NAME, _ALICE)
            mcp_handle = handle.mcp(_MCP_SERVER)
            with pytest.raises(AgentRunError) as excinfo:
                await mcp_handle.call_untyped("read_orders", {})

        assert excinfo.value.code is AgentRunErrorCode.TOOL_TIMEOUT
