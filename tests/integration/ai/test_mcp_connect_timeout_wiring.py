"""``ai.startup_timeout_ms`` reaches the real pydantic-ai engine's MCP toolsets (FR-051).

Drives the real composition root — ``create_app`` -> ``resolve_ai`` ->
``loom.ai.registry.configure_engine_mcp_connect_timeout`` -> the real,
installed ``pydantic-ai`` engine's own ``configure_mcp_connect_timeout`` ->
:class:`~loom.ai.engines.pydantic_ai._mcp.SharedMcpToolsets` — with no fake
engine and no fake entry point: this is the exact path a deployment runs.

Only :meth:`SharedMcpToolsets.set_connect_timeout` is replaced, with a double
that records every value it is called with and then behaves exactly as the
real one, so the moment the seconds value crosses this last, private seam is
observed without needing a real MCP server to reach; the grant still points
at an unreachable address and start-up still fails, exactly as
``test_auto_client_factories.py`` — this suite only adds the recording.

Guards mutants a mocked-provider test cannot: ``ai.startup_timeout_ms /
1000`` silently turned into ``* 1.0`` in ``auto.py``, or either
``configure_engine_mcp_connect_timeout`` (``registry.py``) or the provider's
own ``configure_mcp_connect_timeout`` (``provider.py``) reduced to a no-op —
every one of those leaves the rest of the suite green without this file.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.errors import AgentCompilationError
from loom.rest.fastapi.auto import create_app

_APP_MODULE = "loom_mcp_timeout_wiring_fixture_app"
_AGENT = "tool-user"
_SERVER = "orders"
_STARTUP_TIMEOUT_MS = 7000

# Discard port on the loopback: nothing serves an MCP endpoint there, ever, so
# start-up fails without a single packet leaving the machine.
_UNREACHABLE = "https://127.0.0.1:9"

_APP_SOURCE = '''\
"""Minimal discoverable app used by the MCP connect-timeout wiring test."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class McpTimeoutWiringRecord(BaseModel):
    __tablename__ = "mcp_timeout_wiring_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class McpTimeoutWiringPingUseCase(UseCase[McpTimeoutWiringRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class McpTimeoutWiringPingInterface(RestInterface[str]):
    prefix = "/mcp-timeout-wiring-ping"
    routes = (RestRoute(use_case=McpTimeoutWiringPingUseCase, method="GET", path="/"),)
'''

_ANSWER_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string", "description": "Short answer."}},
}

_AGENT_SPEC: dict[str, Any] = {
    "spec_version": 1,
    "name": _AGENT,
    "description": "Answers plain product questions and returns a short summary.",
    "instructions": "Answer the user question using only the conversation.",
    "output": {"kind": "json_schema", "schema": _ANSWER_SCHEMA},
    "capabilities": [{"kind": "mcp", "server": _SERVER}],
}


def _write_project(tmp_path: Path) -> str:
    """Write the fixture app, one MCP-granting artifact and the YAML config."""
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    agent_dir = tmp_path / "ai" / "agents" / _AGENT
    agent_dir.mkdir(parents=True)
    (agent_dir / "agent.yaml").write_text(yaml.safe_dump(_AGENT_SPEC), encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "mcp-timeout-wiring-demo",
            "code_path": str(tmp_path),
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [_APP_MODULE], "warn_recommended": False},
            },
        },
        "database": {"url": "sqlite+aiosqlite:///"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
        "ai": {
            "engine": "pydantic-ai",
            "specs": ["ai/agents/*/agent.yaml"],
            "models": {"default": {"provider": "openai", "model": "gpt-4o"}},
            "mcp_servers": {_SERVER: {"url": f"{_UNREACHABLE}/mcp"}},
            "startup_timeout_ms": _STARTUP_TIMEOUT_MS,
        },
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def test_startup_timeout_ms_reaches_the_toolset_as_seconds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``startup_timeout_ms: 7000`` reaches the toolset as ``7.0``, never ``7000``."""
    seen: list[float] = []
    real_set = SharedMcpToolsets.set_connect_timeout

    def _recording_set(self: SharedMcpToolsets, seconds: float) -> None:
        seen.append(seconds)
        real_set(self, seconds)

    monkeypatch.setattr(SharedMcpToolsets, "set_connect_timeout", _recording_set)
    config_path = _write_project(tmp_path)
    app = create_app(config_path)

    # The grant's server is unreachable, so start-up still fails here exactly
    # as it does with no timeout override at all — what is under test is the
    # value recorded before that failure, not the failure itself.
    with pytest.raises(AgentCompilationError), TestClient(app):
        pass  # pragma: no cover - start-up never completes

    assert seen == [_STARTUP_TIMEOUT_MS / 1000]
