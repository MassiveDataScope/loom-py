"""An ``mcp`` or ``a2a`` grant compiles *and* starts (the wiring, end to end).

Two independent gaps closed here, both of which let an artifact compile and
then fail:

* ``a2a`` was missing from the kinds the pydantic-ai provider announces, so the
  compiler refused every grant of it and the whole outbound A2A path was
  unreachable.
* The composition root built the runtime without either client factory, so a
  grant that *did* compile aborted start-up with "no MCP client factory is
  configured".

Every artifact here goes through the real :class:`~loom.ai.compiler.AgentCompiler`
with the kinds the real provider announces — never a hand-built
``CompiledA2ACapability`` and never a provider written to accept one.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import TracebackType
from typing import Any

import pytest
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel

from loom.ai.compiler import AgentCompiler, AgentPlan
from loom.ai.config import A2AAgentConfig, AiConfig, McpServerConfig
from loom.ai.declarative import (
    A2ACapability,
    AgentSpecV1,
    JsonSchemaOutput,
    McpCapability,
)
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider, _capabilities
from loom.ai.inference import InferenceTarget
from loom.ai.runtime import AgentRuntime
from loom.core.di import LoomContainer
from loom.core.model.redaction import Dsn
from loom.core.use_case.registry import UseCaseRegistry
from tests.integration.ai.conftest import RecordingMcpSession, StubDepsFactory

_AGENT = "delegator"
_REMOTE = "translations"
_REMOTE_URL = "https://agents.example.com/translations"
_SERVER = "tools"
_SERVER_URL = "https://tools.internal/mcp"

_ANSWER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string"}},
}


class StubA2AClient:
    """Async context manager standing in for one connected remote agent."""

    def __init__(self) -> None:
        self.opened = False

    async def __aenter__(self) -> object:
        """Report the session as connected."""
        self.opened = True
        return object()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Close the stub session."""
        self.opened = False


@asynccontextmanager
async def _stub_a2a_client(capability: object) -> AsyncIterator[object]:
    del capability
    async with StubA2AClient() as session:
        yield session


@asynccontextmanager
async def _stub_mcp_client(capability: object) -> AsyncIterator[RecordingMcpSession]:
    del capability
    yield RecordingMcpSession(tools=("alpha", "beta"))


def _silent_model() -> Model:
    """A model that answers immediately: no run is driven by these tests."""

    def respond(messages: object, info: AgentInfo) -> ModelResponse:
        del messages, info
        return ModelResponse(parts=[TextPart(content="{}")])

    return FunctionModel(respond)


def _config() -> AiConfig:
    """Deployment configuration registering the remote agent and the server."""
    return AiConfig(
        engine="pydantic-ai",
        specs=(),
        models={"default": InferenceTarget(provider="fake", model="fake-model")},
        mcp_servers={_SERVER: McpServerConfig(url=Dsn(_SERVER_URL))},
        a2a_agents={_REMOTE: A2AAgentConfig(url=Dsn(_REMOTE_URL))},
        startup_timeout_ms=2000,
        health_cache_ttl_ms=5000,
    )


def _spec(*capabilities: object) -> AgentSpecV1:
    """Author one artifact granting *capabilities*, as a deployment would."""
    return AgentSpecV1(
        spec_version=1,
        name=_AGENT,
        description="Delegates translation work and reads the shared tool server.",
        instructions="Answer using only the prompt and the granted capabilities.",
        output=JsonSchemaOutput(schema=_ANSWER_SCHEMA),
        capabilities=tuple(capabilities),  # type: ignore[arg-type]
    )


def _compile(*capabilities: object) -> AgentPlan:
    """Compile one artifact with the kinds the real provider announces."""
    compiler = AgentCompiler(
        config=_config(),
        registry=UseCaseRegistry.build([]),
        supported_kinds=PydanticAIEngineProvider().supported_capability_kinds(),
    )
    return compiler.compile(_spec(*capabilities), source_path=f"ai/agents/{_AGENT}/agent.yaml")


def _runtime(plan: AgentPlan) -> AgentRuntime:
    """Build the runtime the composition root builds, over local stubs only."""
    return AgentRuntime(
        plans=[plan],
        config=_config(),
        engine_provider=PydanticAIEngineProvider(model_resolver=lambda target: _silent_model()),
        deps=StubDepsFactory(),  # type: ignore[arg-type]
        container=LoomContainer(),
        mcp_client_factory=_stub_mcp_client,  # type: ignore[arg-type]
        a2a_client_factory=_stub_a2a_client,  # type: ignore[arg-type]
    )


class TestGrantA2A:
    """The provider announces ``a2a``, so the compiler admits it and it runs."""

    def test_the_provider_announces_a2a_when_asked(self) -> None:
        """Announcing the kind is what makes the compiler admit the grant."""
        assert "a2a" in PydanticAIEngineProvider().supported_capability_kinds()

    def test_the_compiler_admits_the_grant_when_the_agent_is_registered(self) -> None:
        """Through the real compiler: one compiled capability, not zero."""
        plan = _compile(A2ACapability(agent=_REMOTE))

        assert [capability.kind for capability in plan.capabilities] == ["a2a"]

    async def test_the_runtime_starts_when_the_artifact_grants_a2a(self) -> None:
        """Start-up opens the remote client and builds the engine for the plan."""
        plan = _compile(A2ACapability(agent=_REMOTE))

        async with _runtime(plan) as runtime:
            assert runtime.has_agent(_AGENT)

    async def test_the_agent_declares_the_a2a_capability_once_started(self) -> None:
        """The started agent reports the kind, so the mount announces it too."""
        plan = _compile(A2ACapability(agent=_REMOTE))

        async with _runtime(plan) as runtime:
            assert runtime.capability_kinds(_AGENT) == ("a2a",)


class TestGrantMcp:
    """An ``mcp`` grant reaches a start-up client instead of aborting."""

    @pytest.fixture(autouse=True)
    def stub_server_toolset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Replace only the engine-side toolset: the MCP client is an extra.

        The start-up path under test — compiler, runtime, client factory — is
        the real one; the engine's own ``MCPToolset`` needs the optional MCP
        client library, which no test environment installs.
        """
        from pydantic_ai.toolsets import FunctionToolset

        monkeypatch.setattr(
            _capabilities, "_mcp_server", lambda capability, context: FunctionToolset([])
        )

    async def test_the_runtime_starts_when_the_artifact_grants_mcp(self) -> None:
        """The grant no longer fails start-up for want of a client factory."""
        plan = _compile(McpCapability(server=_SERVER))

        async with _runtime(plan) as runtime:
            assert runtime.has_agent(_AGENT)

    async def test_validates_the_filter_against_the_server_when_the_grant_declares_it(
        self,
    ) -> None:
        """A declared filter is checked against the tools the server lists."""
        plan = _compile(McpCapability(server=_SERVER, include=("alpha",)))

        async with _runtime(plan) as runtime:
            assert runtime.has_agent(_AGENT)


class TestCombinedGrants:
    """Both grants in one artifact still start: the two factories coexist."""

    @pytest.fixture(autouse=True)
    def stub_server_toolset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Same engine-side stand-in as :class:`TestGrantMcp`."""
        from pydantic_ai.toolsets import FunctionToolset

        monkeypatch.setattr(
            _capabilities, "_mcp_server", lambda capability, context: FunctionToolset([])
        )

    async def test_the_runtime_starts_when_the_artifact_grants_both(self) -> None:
        """One plan, two live dependencies, one entered runtime."""
        plan = _compile(McpCapability(server=_SERVER), A2ACapability(agent=_REMOTE))

        async with _runtime(plan) as runtime:
            kinds = set(runtime.capability_kinds(_AGENT))

        assert kinds == {"mcp", "a2a"}
