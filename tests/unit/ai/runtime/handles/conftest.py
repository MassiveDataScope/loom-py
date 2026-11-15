"""A runtime granting one agent an ``mcp`` server and a ``sql`` connection, and both resolvers."""

from __future__ import annotations

from typing import Any

import pytest

from loom.ai.abc import AgentHandle, McpHandle
from loom.ai.runtime import AgentRuntime
from loom.ai.runtime._handle import AgentMarkerResolver, McpMarkerResolver
from loom.ai.runtime._lifecycle import UseCaseMcpGrant
from loom.core.di import LoomContainer
from loom.core.use_case import Agent, Mcp
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    RecordingMcpSession,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    make_sql_config,
    mcp_client_factory,
)

from .doubles import (
    AGENT,
    CONNECTION,
    SERVER,
    TOOL,
    AcmeEngine,
    AcmeSqlService,
    analyst_sql_capability,
)


@pytest.fixture
def session() -> RecordingMcpSession:
    return RecordingMcpSession(
        tools=(TOOL, "delete_orders"), schemas=(TOOL,), results={TOOL: {"orders": 1}}
    )


@pytest.fixture
def engine() -> AcmeEngine:
    return AcmeEngine()


@pytest.fixture
def sql_service() -> AcmeSqlService:
    return AcmeSqlService()


@pytest.fixture
def make_runtime(session: RecordingMcpSession, engine: AcmeEngine) -> Any:
    """Build the runtime, not entered; ``reachable=False`` tolerates the server as down."""

    def make(*, reachable: bool = True, use_case_servers: tuple[str, ...] = (SERVER,)) -> Any:
        client = StubMcpClient(
            label="crm",
            session=session,
            log=[],
            connect_error=None if reachable else "refused",
        )
        return AgentRuntime(
            plans=[
                make_plan(
                    AGENT,
                    capabilities=(
                        make_mcp_capability(SERVER, include=("find_*",)),
                        analyst_sql_capability(),
                    ),
                )
            ],
            config=make_ai_config(
                mcp_servers=make_mcp_servers(SERVER),
                remote_clients="required" if reachable else "optional",
            ),
            engine_provider=CountingEngineProvider(engines={AGENT: engine}),  # type: ignore[dict-item]
            deps=StubDepsFactory(),
            container=LoomContainer(),
            mcp_client_factory=mcp_client_factory({SERVER: client}),  # type: ignore[arg-type]
            sql_config=make_sql_config(CONNECTION),
            use_case_mcp=tuple(
                UseCaseMcpGrant(
                    capability=make_mcp_capability(server, include=("find_*",)),
                    usecase="acme.find_orders",
                    parameter="crm",
                )
                for server in use_case_servers
            ),
        )

    return make


@pytest.fixture
def runtime(make_runtime: Any) -> AgentRuntime:
    return make_runtime()  # type: ignore[no-any-return]


@pytest.fixture
def agent_resolver(runtime: AgentRuntime, sql_service: AcmeSqlService) -> AgentMarkerResolver:
    return AgentMarkerResolver(runtime, sql_query_service=sql_service, observability=None)


@pytest.fixture
def mcp_resolver(runtime: AgentRuntime) -> McpMarkerResolver:
    return McpMarkerResolver(runtime, observability=None)


@pytest.fixture
def triage(agent_resolver: AgentMarkerResolver) -> Any:
    """The handle ``Agent(AGENT)`` resolves to."""
    return agent_resolver.resolve(Agent(AGENT), AgentHandle[dict[str, Any]])


@pytest.fixture
def crm(mcp_resolver: McpMarkerResolver) -> Any:
    """The handle ``Mcp(SERVER, include=["find_*"])`` resolves to."""
    return mcp_resolver.resolve(Mcp(SERVER, include=["find_*"]), McpHandle)


@pytest.fixture
def backend_calls(
    engine: AcmeEngine, session: RecordingMcpSession, sql_service: AcmeSqlService
) -> Any:
    """Count every call that reached a backend: a run, a tool call or a query."""

    def count() -> int:
        return len(engine.identities) + len(session.calls) + len(sql_service.calls)

    return count
