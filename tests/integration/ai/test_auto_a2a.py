"""The composition root mounts the inbound A2A surface (T183, FR-041).

``bind_a2a_endpoints`` had no caller in ``src/``: the whole inbound transport
was implemented, tested in isolation, and unreachable in any application
``create_app`` builds. These tests drive the real ``create_app`` over a
temporary project and speak A2A to the app it returns, so the card, one
``message/send`` and one refused method are proven end to end rather than
against a hand-mounted ``FastAPI``.

The engine entry point is faked in-process: no provider SDK, no network, no
credential. The agent opts into anonymous callers so the assertions stay on the
wiring rather than on token minting, which
``tests/integration/ai/test_a2a.py`` already covers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient

from loom.ai.a2a.card import card_path
from loom.rest.fastapi.auto import create_app
from tests.integration.ai._entrypoints import fake_entry_points
from tests.integration.ai.conftest import DEFAULT_OUTPUT
from tests.integration.ai.test_auto_gate import (
    _APP_MODULE,
    _APP_SOURCE,
    _ENGINE_NAME,
    _GROUP,
    _FakeEntryPoint,
)

_AGENT = "published-agent"
_BASE_URL = "https://agents.example.com"

_AGENT_SPEC: dict[str, Any] = {
    "spec_version": 1,
    "name": _AGENT,
    "description": "Answers plain product questions and returns a short summary.",
    "instructions": "Answer the user question using only the conversation.",
    "output": {
        "kind": "json_schema",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["answer"],
            "properties": {"answer": {"type": "string", "description": "Short answer."}},
        },
    },
}


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the in-process engine ``ai.engine`` resolves to."""
    monkeypatch.setattr(
        "loom.core.plugins.entrypoints.entry_points",
        fake_entry_points(_GROUP, (_FakeEntryPoint(),)),
    )


def _write_project(tmp_path: Path, *, ai_extra: dict[str, Any]) -> str:
    """Write the fixture app, one agent spec and the YAML config; return its path.

    Args:
        tmp_path: Directory the project is written into.
        ai_extra: Extra keys merged into the ``ai:`` section — ``a2a`` and
            ``endpoints`` — so one writer serves both the mounted and the
            unmounted case.

    Returns:
        Path of the written configuration file.
    """
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    agent_dir = tmp_path / "ai" / "agents" / _AGENT
    agent_dir.mkdir(parents=True)
    (agent_dir / "agent.yaml").write_text(yaml.safe_dump(_AGENT_SPEC), encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "a2a-wiring-demo",
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
            **ai_extra,
        },
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


_PUBLISHED: dict[str, Any] = {
    "a2a": {"base_url": _BASE_URL, "expose": [_AGENT]},
    # Anonymity is a double opt-in: the HTTP stanza must itself be in force
    # before its 'allow_anonymous' reaches the A2A surface.
    "endpoints": {_AGENT: {"enabled": True, "auth": "identity", "allow_anonymous": True}},
}


def _rpc(client: TestClient, method: str, params: dict[str, Any] | None = None) -> Any:
    """Post one JSON-RPC call to the published agent and return the decoded body."""
    payload: dict[str, Any] = {"jsonrpc": "2.0", "id": 1, "method": method}
    if params is not None:
        payload["params"] = params
    return client.post(f"/a2a/{_AGENT}", json=payload).json()


class TestA2ASurfaceMounted:
    """``create_app`` serves the card and the JSON-RPC endpoint of every exposure."""

    @pytest.mark.usefixtures("fake_engine")
    def test_serves_the_published_agents_card(self, tmp_path: Path) -> None:
        """The card is reachable, anonymous, and advertises the configured base URL."""
        config_path = _write_project(tmp_path, ai_extra=_PUBLISHED)

        with TestClient(create_app(config_path)) as client:
            response = client.get(card_path(_AGENT))

        assert response.status_code == 200
        assert response.json()["url"] == f"{_BASE_URL}/a2a/{_AGENT}"

    @pytest.mark.usefixtures("fake_engine")
    def test_responds_a_completed_task_to_message_send(self, tmp_path: Path) -> None:
        """One ``message/send`` runs the agent and answers its terminal task."""
        config_path = _write_project(tmp_path, ai_extra=_PUBLISHED)

        with TestClient(create_app(config_path)) as client:
            body = _rpc(
                client, "message/send", {"message": {"parts": [{"kind": "text", "text": "hola"}]}}
            )

        task = body["result"]
        assert task["status"]["state"] == "completed"
        assert task["artifacts"][0]["parts"][0]["data"] == dict(DEFAULT_OUTPUT)

    @pytest.mark.usefixtures("fake_engine")
    def test_responds_a_json_rpc_error_to_an_unsupported_method(self, tmp_path: Path) -> None:
        """An advertised-absent method answers ``-32004``, never an HTTP 500."""
        config_path = _write_project(tmp_path, ai_extra=_PUBLISHED)

        with TestClient(create_app(config_path)) as client:
            body = _rpc(client, "tasks/get", {"id": "whatever"})

        assert body["error"]["code"] == -32004


class TestNoA2ASection:
    """No ``ai.a2a`` section publishes nothing at all (FR-041)."""

    @pytest.mark.usefixtures("fake_engine")
    def test_serves_no_card_when_there_is_no_a2a_section(self, tmp_path: Path) -> None:
        """The agent still has its HTTP surface; the A2A one is simply absent."""
        config_path = _write_project(
            tmp_path,
            ai_extra={
                "endpoints": {
                    _AGENT: {"enabled": True, "auth": "identity", "allow_anonymous": True}
                }
            },
        )

        with TestClient(create_app(config_path)) as client:
            assert client.get(card_path(_AGENT)).status_code == 404
            assert client.post(f"/a2a/{_AGENT}", json={}).status_code == 404
