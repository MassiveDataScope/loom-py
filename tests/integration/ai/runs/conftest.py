"""A runtime serving one agent, its HTTP endpoints, and a raw ASGI stream that walks away."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from fastapi import FastAPI

from loom.ai.execution import AgentRuntime
from loom.ai.execution._lifecycle import UseCaseMcpGrant
from loom.ai.fastapi.endpoints import bind_agent_endpoints
from loom.core.di import LoomContainer
from loom.core.observability.observability import ObservabilityRuntime
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    RecordingMcpSession,
    StubDepsFactory,
    StubMcpClient,
    make_ai_config,
    make_endpoint,
    make_mcp_capability,
    make_mcp_servers,
    make_plan,
    mcp_client_factory,
)

from .doubles import AGENT, PREFIX, SERVER, TOOL, AcmeAuthenticator, AcmeIdentityMiddleware


@pytest.fixture
def session() -> RecordingMcpSession:
    return RecordingMcpSession(tools=(TOOL,), results={TOOL: {"orders": 1}})


@pytest.fixture
def make_runtime(session: RecordingMcpSession) -> Any:
    """Build a runtime, not entered, serving *engine* as the one agent."""

    def make(engine: object) -> AgentRuntime:
        client = StubMcpClient(label="crm", session=session, log=[])
        return AgentRuntime(
            plans=[make_plan(AGENT)],
            config=make_ai_config(
                endpoints={AGENT: make_endpoint()}, mcp_servers=make_mcp_servers(SERVER)
            ),
            engine_plugin=CountingEngineProvider(engines={AGENT: engine}),  # type: ignore[dict-item]
            deps=StubDepsFactory(),
            container=LoomContainer(),
            mcp_client_factory=mcp_client_factory({SERVER: client}),  # type: ignore[arg-type]
            use_case_mcp=(
                UseCaseMcpGrant(
                    capability=make_mcp_capability(SERVER, include=(TOOL,)),
                    usecase="acme.find_orders",
                    parameter="crm",
                ),
            ),
        )

    return make


@pytest.fixture
def serving() -> Any:
    """Serve an entered runtime's agent endpoints, acting as Alice for each request."""

    @asynccontextmanager
    async def serve(runtime: AgentRuntime) -> AsyncIterator[FastAPI]:
        app = FastAPI()
        bind_agent_endpoints(
            app,
            runtime=runtime,
            config=make_ai_config(endpoints={AGENT: make_endpoint()}),
            authenticator=AcmeAuthenticator(),  # type: ignore[arg-type]
            observability_runtime=ObservabilityRuntime([]),
            prefix=PREFIX,
        )
        app.add_middleware(AcmeIdentityMiddleware)
        yield app

    return serve
