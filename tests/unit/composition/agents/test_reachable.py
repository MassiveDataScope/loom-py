"""Which agents an entry point compiles, and which MCP servers it opens at Init.

An entry point serving no agent endpoints (a Lambda function) reaches agents
only through the ``Agent()`` markers of its compiled use cases, so it builds
only the agents those markers name. Unreachable agents are neither compiled
nor entered, and their MCP servers are never opened. An entry point serving
endpoints (FastAPI) builds every declared agent.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.ai import registry
from loom.ai.abc import AgentHandle, McpToolCallResult, McpToolInfo
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.composition import Application, CompositionSource, Recipe, compose_application
from loom.core.config import ConfigContext
from loom.core.discovery.base import DiscoveryResult
from loom.core.persistence.profile import SERVER_PROFILE
from loom.core.use_case import Agent
from loom.core.use_case.use_case import UseCase
from tests.helpers.ai import CountingEngineProvider
from tests.helpers.composition import composition_source

_AGENTS_GLOB = "agents/*.yaml"
_SERVER_A = "server-a"
_SERVER_B = "server-b"
_MODELS = {"default": {"provider": "fake", "model": "fake-model"}}
_AI: dict[str, Any] = {
    "engine": "fake",
    "models": _MODELS,
    "mcp_servers": {
        _SERVER_A: {"transport": "http", "url": "https://a.example.com/mcp"},
        _SERVER_B: {"transport": "http", "url": "https://b.example.com/mcp"},
    },
    "a2a_agents": {"b": {"url": "https://remote.example.com/a2a"}},
}


class ReachesAUseCase(UseCase[object, object]):
    """Names agent ``a`` through its marker; nothing names agent ``b``."""

    async def execute(self, agent: AgentHandle = Agent("a")) -> object:  # type: ignore[type-arg]
        return agent


class ReachesMissingUseCase(UseCase[object, object]):
    """Names an agent no artifact declares."""

    async def execute(self, agent: AgentHandle = Agent("missing")) -> object:  # type: ignore[type-arg]
        return agent


class _OpeningProvider(CountingEngineProvider):
    """An in-process engine provider recording the MCP servers the runtime opens."""

    def __init__(self) -> None:
        super().__init__()
        self.opened: list[str] = []

    def mcp_client_factory(self, capability: Any) -> Any:  # type: ignore[override]
        return self._session(capability.server)

    @asynccontextmanager
    async def _session(self, server: str) -> AsyncIterator[_EmptySession]:
        self.opened.append(server)
        yield _EmptySession()


class _EmptySession:
    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return ()

    async def call_tool(self, name: str, arguments: Any) -> McpToolCallResult:
        raise AssertionError(f"no tool is called at Init, got {name}")


class _Entry:
    """An entry point hosting agents, with or without their HTTP endpoints."""

    def __init__(self, use_case: type[UseCase[Any, Any]], *, agent_endpoints: bool) -> None:
        self._use_case = use_case
        self._agent_endpoints = agent_endpoints

    def recipe(self, source: CompositionSource) -> Recipe:
        del source
        return Recipe(
            profile=SERVER_PROFILE,
            discovered=DiscoveryResult(
                models=(),
                use_cases=(self._use_case,),
                interfaces=(),
                jobs=(),
                agent_specs=(_AGENTS_GLOB,),
            ),
            agents=True,
            agent_endpoints=self._agent_endpoints,
        )

    def bind(self, application: Application) -> Application:
        return application


def _agent(name: str, capability: dict[str, Any]) -> dict[str, Any]:
    return {
        "spec_version": 1,
        "name": name,
        "description": f"Agent {name}.",
        "instructions": "Answer.",
        "output": {
            "kind": "json_schema",
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["answer"],
                "properties": {"answer": {"type": "string", "description": "The answer."}},
            },
        },
        "capabilities": [capability],
    }


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch) -> _OpeningProvider:
    opening = _OpeningProvider()
    monkeypatch.setattr(registry, "resolve_engine_provider", lambda _: opening)
    return opening


def _compose(
    tmp_path: Path,
    a_capability: dict[str, Any],
    *,
    use_case: type[UseCase[Any, Any]] = ReachesAUseCase,
    agent_endpoints: bool,
) -> Application:
    """Agent ``a`` with *a_capability* and agent ``b`` using ``server-b``, composed."""
    agents = tmp_path / "agents"
    agents.mkdir()
    for name, capability in (("a", a_capability), ("b", {"kind": "mcp", "server": _SERVER_B})):
        (agents / f"{name}.yaml").write_text(
            yaml.safe_dump(_agent(name, capability)), encoding="utf-8"
        )
    document = {"app": {"name": "reachable"}, "ai": _AI}
    source = composition_source(
        ConfigContext.from_dict(document), code_path=tmp_path, config_root=tmp_path
    )
    return compose_application(source, _Entry(use_case, agent_endpoints=agent_endpoints))


def _init(application: Application) -> None:
    async def opened() -> None:
        async with application.running():
            pass

    asyncio.run(opened())


def _plan_names(application: Application) -> list[str]:
    return sorted(plan.name for plan in application.ai.plans)


_MCP_A = {"kind": "mcp", "server": _SERVER_A}


def test_without_endpoints_only_marker_named_agents_are_compiled_and_opened(
    tmp_path: Path, provider: _OpeningProvider
) -> None:
    application = _compose(tmp_path, _MCP_A, agent_endpoints=False)

    _init(application)

    assert _plan_names(application) == ["a"]
    assert provider.opened == [_SERVER_A]


def test_with_endpoints_every_declared_agent_is_compiled_and_opened(
    tmp_path: Path, provider: _OpeningProvider
) -> None:
    application = _compose(tmp_path, _MCP_A, agent_endpoints=True)

    _init(application)

    assert _plan_names(application) == ["a", "b"]
    assert sorted(provider.opened) == [_SERVER_A, _SERVER_B]


def test_an_a2a_capability_adds_no_local_agent(tmp_path: Path, provider: _OpeningProvider) -> None:
    application = _compose(tmp_path, {"kind": "a2a", "agent": "b"}, agent_endpoints=False)

    assert _plan_names(application) == ["a"]
    assert provider.opened == []


def test_a_marker_naming_a_missing_agent_still_fails_start_up(
    tmp_path: Path, provider: _OpeningProvider
) -> None:
    with pytest.raises(AgentCompilationError) as raised:
        _compose(tmp_path, _MCP_A, use_case=ReachesMissingUseCase, agent_endpoints=False)

    assert {issue.code for issue in raised.value.issues} == {AgentErrorCode.AGENT_MARKER_UNKNOWN}
    assert "missing" in str(raised.value)
