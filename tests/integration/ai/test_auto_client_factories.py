"""The composition root hands the runtime its client factories (FR-025, FR-040).

``create_app`` used to build :class:`~loom.ai.runtime.AgentRuntime` with neither
``mcp_client_factory`` nor ``a2a_client_factory``. An artifact granting either
kind therefore compiled and then aborted start-up with "no MCP client factory is
configured" — a failure no deployment could act on, because nothing in the
configuration file was wrong.

These tests drive the real ``create_app`` over a temporary project and enter its
lifespan, which is where the runtime opens its live clients. Both grants point
at addresses nothing serves, so start-up still fails; what must have changed is
*why*. The absence of the missing-factory wording is the assertion: it can only
be produced by a runtime that was handed no factory at all.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from loom.ai.errors import AgentCompilationError
from loom.core.plugins import entrypoints as entrypoints_module
from loom.rest.fastapi.auto import create_app
from tests.integration.ai._entrypoints import fake_entry_points
from tests.integration.ai.test_auto_gate import (
    _APP_MODULE,
    _APP_SOURCE,
    _ENGINE_NAME,
    _GROUP,
    _FakeEntryPoint,
)

_MCP_AGENT = "tool-user"
_A2A_AGENT = "delegator"
_SERVER = "tools"
_REMOTE = "translations"

# Discard port on the loopback: nothing serves an MCP endpoint or an agent card
# there, ever, so start-up fails without a single packet leaving the machine.
_UNREACHABLE = "https://127.0.0.1:9"

_MISSING_MCP_FACTORY = "no MCP client factory is configured"
_MISSING_A2A_FACTORY = "no A2A client factory is configured"

_ANSWER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string", "description": "Short answer."}},
}


def _spec(name: str, capability: dict[str, Any]) -> dict[str, Any]:
    """Author one artifact granting exactly one capability."""
    return {
        "spec_version": 1,
        "name": name,
        "description": "Answers plain product questions and returns a short summary.",
        "instructions": "Answer the user question using only the conversation.",
        "output": {"kind": "json_schema", "schema": _ANSWER_SCHEMA},
        "capabilities": [capability],
    }


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the in-process engine ``ai.engine`` resolves to.

    The engine is faked so that no provider SDK, credential or model call is
    involved: what is under test is the composition root, not the adapter.
    """
    monkeypatch.setattr(
        entrypoints_module, "entry_points", fake_entry_points(_GROUP, (_FakeEntryPoint(),))
    )


def _write_project(tmp_path: Path, *, specs: dict[str, dict[str, Any]]) -> str:
    """Write the fixture app, one artifact per spec and the YAML config."""
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    for name, spec in specs.items():
        agent_dir = tmp_path / "ai" / "agents" / name
        agent_dir.mkdir(parents=True)
        (agent_dir / "agent.yaml").write_text(yaml.safe_dump(spec), encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "ai-factories-demo",
            "code_path": str(tmp_path),
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [_APP_MODULE], "warn_recommended": False},
            },
        },
        "database": {"url": "sqlite+aiosqlite:///"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
        "ai": {
            "engine": _ENGINE_NAME,
            "specs": ["ai/agents/*/agent.yaml"],
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
            "mcp_servers": {_SERVER: {"url": f"{_UNREACHABLE}/mcp"}},
            "a2a_agents": {_REMOTE: {"url": f"{_UNREACHABLE}/{_REMOTE}"}},
            "startup_timeout_ms": 2000,
        },
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _start_failure(config_path: str) -> AgentCompilationError:
    """Enter the app's lifespan and return the start-up failure it raises."""
    app = create_app(config_path)
    with pytest.raises(AgentCompilationError) as failure, TestClient(app):
        pass  # pragma: no cover - start-up never completes
    return failure.value


def _messages(failure: AgentCompilationError) -> str:
    """Join every issue message of an aggregated start-up failure."""
    return "\n".join(issue.message for issue in failure.issues)


class TestMcpFactory:
    """An ``mcp`` grant reaches a client factory, not a missing-factory refusal."""

    @pytest.mark.usefixtures("fake_engine")
    def test_does_not_complain_about_the_factory_when_the_artifact_grants_mcp(
        self, tmp_path: Path
    ) -> None:
        """The wording only a factory-less runtime produces must be gone."""
        config_path = _write_project(
            tmp_path, specs={_MCP_AGENT: _spec(_MCP_AGENT, {"kind": "mcp", "server": _SERVER})}
        )

        assert _MISSING_MCP_FACTORY not in _messages(_start_failure(config_path))

    @pytest.mark.usefixtures("fake_engine")
    def test_names_the_server_when_the_artifact_grants_mcp(self, tmp_path: Path) -> None:
        """The operator is told which registered server could not be reached."""
        config_path = _write_project(
            tmp_path, specs={_MCP_AGENT: _spec(_MCP_AGENT, {"kind": "mcp", "server": _SERVER})}
        )

        assert _SERVER in _messages(_start_failure(config_path))


class TestA2AFactory:
    """An ``a2a`` grant compiles at all, and then reaches its client factory."""

    @pytest.mark.usefixtures("fake_engine")
    def test_does_not_complain_about_the_factory_when_the_artifact_grants_a2a(
        self, tmp_path: Path
    ) -> None:
        """Same wording, same absence: the outbound factory is wired too."""
        config_path = _write_project(
            tmp_path, specs={_A2A_AGENT: _spec(_A2A_AGENT, {"kind": "a2a", "agent": _REMOTE})}
        )

        assert _MISSING_A2A_FACTORY not in _messages(_start_failure(config_path))

    @pytest.mark.usefixtures("fake_engine")
    def test_names_the_remote_agent_when_the_artifact_grants_a2a(self, tmp_path: Path) -> None:
        """Named as the deployment registered it, never by URL (FR-038)."""
        config_path = _write_project(
            tmp_path, specs={_A2A_AGENT: _spec(_A2A_AGENT, {"kind": "a2a", "agent": _REMOTE})}
        )

        assert _REMOTE in _messages(_start_failure(config_path))

    @pytest.mark.usefixtures("fake_engine")
    def test_does_not_expose_the_url_when_the_remote_agent_is_unreachable(
        self, tmp_path: Path
    ) -> None:
        """The address is deployment topology; the issue names the agent only."""
        config_path = _write_project(
            tmp_path, specs={_A2A_AGENT: _spec(_A2A_AGENT, {"kind": "a2a", "agent": _REMOTE})}
        )
        failure = _start_failure(config_path)

        assert all(issue.component == _REMOTE for issue in failure.issues)
