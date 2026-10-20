"""Start-up gates the agent surface inherits from the SQL one (§4).

Drives :func:`loom.rest.fastapi.auto.create_app` over a temporary project whose
``ai:`` section exposes one agent, mirroring
``tests/unit/rest/test_sql_endpoint_auth_gate.py``. The engine entry point is
faked in-process: no provider SDK, no network, no credential.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.core.config.errors import ConfigError
from loom.core.plugins import entrypoints as entrypoints_module
from loom.rest.fastapi.auto import create_app
from tests.integration.ai._entrypoints import fake_entry_points
from tests.integration.ai.conftest import CountingEngineProvider

_APP_MODULE = "loom_aigate_fixture_app"
_ENGINE_NAME = "aigate-fake"
_AGENT = "minimal-agent"
_GROUP = "loom.ai.engines"

_APP_SOURCE = '''\
"""Minimal discoverable app used by the agent auth-gate tests."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class AiGateRecord(BaseModel):
    __tablename__ = "aigate_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class AiGatePingUseCase(UseCase[AiGateRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class AiGatePingInterface(RestInterface[str]):
    prefix = "/aigate-ping"
    routes = (RestRoute(use_case=AiGatePingUseCase, method="GET", path="/"),)
'''

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

_JWT_WITHOUT_AUDIENCE: dict[str, Any] = {
    "secret_path": "",
    "algorithms": ["HS256"],
}


class _FakeDist:
    """Minimal stand-in for ``importlib.metadata.Distribution``."""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    """Entry point resolving the fake engine provider of these tests."""

    def __init__(self) -> None:
        self.name = _ENGINE_NAME
        self.group = _GROUP
        self.dist = _FakeDist("loom-aigate-tests")

    def load(self) -> object:
        """Return the provider class the registry instantiates."""
        return CountingEngineProvider


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the in-process engine ``ai.engine`` resolves to."""
    monkeypatch.setattr(
        entrypoints_module, "entry_points", fake_entry_points(_GROUP, (_FakeEntryPoint(),))
    )


def _write_project(
    tmp_path: Path,
    *,
    endpoint: dict[str, Any],
    jwt_section: dict[str, Any],
    a2a: dict[str, Any] | None = None,
) -> str:
    """Write the fixture app, one agent spec and the YAML config; return its path.

    The spec lives at ``ai/agents/<name>/agent.yaml``: one directory per agent,
    which is the layout ``ai.specs`` globs and where a skill library sits next
    to the artifact that grants it.
    """
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    secret_path = tmp_path / "hs.key"
    secret_path.write_text("integration-test-secret", encoding="utf-8")
    agent_dir = tmp_path / "ai" / "agents" / _AGENT
    agent_dir.mkdir(parents=True)
    (agent_dir / "agent.yaml").write_text(yaml.safe_dump(_AGENT_SPEC), encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "aigate-demo",
            "code_path": str(tmp_path),
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [_APP_MODULE], "warn_recommended": False},
            },
            "rest": {"auth": {"jwt": {**jwt_section, "secret_path": str(secret_path)}}},
        },
        "database": {"url": "sqlite+aiosqlite:///"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
        "ai": {
            "engine": _ENGINE_NAME,
            "specs": ["ai/agents/*/agent.yaml"],
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
            "endpoints": {_AGENT: endpoint},
            **({"a2a": a2a} if a2a is not None else {}),
        },
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


class TestTheAgentsJwtAudience:
    """An agent surface must not boot where the SQL surface already refuses (§4)."""

    @pytest.mark.usefixtures("fake_engine")
    def test_fails_to_start_when_the_jwt_does_not_validate_audience(self, tmp_path: Path) -> None:
        """Without ``aud`` a token minted for a sibling service drives every capability."""
        config_path = _write_project(
            tmp_path,
            endpoint={"enabled": True, "auth": "identity"},
            jwt_section=_JWT_WITHOUT_AUDIENCE,
        )

        with pytest.raises(ConfigError, match="audience"):
            create_app(config_path)

    @pytest.mark.usefixtures("fake_engine")
    def test_names_the_agent_when_the_jwt_does_not_validate_audience(self, tmp_path: Path) -> None:
        """The operator is told which agent is exposed, not which connection."""
        config_path = _write_project(
            tmp_path,
            endpoint={"enabled": True, "auth": "identity"},
            jwt_section=_JWT_WITHOUT_AUDIENCE,
        )

        with pytest.raises(ConfigError) as failure:
            create_app(config_path)

        assert _AGENT in str(failure.value)

    @pytest.mark.usefixtures("fake_engine")
    def test_starts_when_the_jwt_validates_audience(self, tmp_path: Path) -> None:
        """A validated ``aud`` is all the gate asks for."""
        config_path = _write_project(
            tmp_path,
            endpoint={"enabled": True, "auth": "identity"},
            jwt_section={**_JWT_WITHOUT_AUDIENCE, "audience": "loom-api"},
        )

        assert create_app(config_path) is not None


_A2A_SECTION: dict[str, Any] = {
    "base_url": "https://agents.example.com",
    "expose": [_AGENT],
}

# The HTTP surface is switched off, so only 'ai.a2a.expose' publishes this
# agent: the gate must fire on the A2A exposure alone or it fires on nothing.
_HTTP_OFF: dict[str, Any] = {"enabled": False, "auth": "identity"}


class TestTheJwtAudienceOfTheAgentPublishedOverA2a:
    """The public A2A surface is gated at least as strictly as the HTTP one (§4).

    An agent in ``ai.a2a.expose`` is announced to the internet with a card
    naming its bearer scheme. Without a validated ``aud`` any token signed by
    the same key — including one minted for a sibling service — drives every
    capability that agent holds, as that caller.
    """

    @pytest.mark.usefixtures("fake_engine")
    def test_fails_to_start_when_the_jwt_does_not_validate_audience(self, tmp_path: Path) -> None:
        """Publishing over A2A alone is enough to require the ``aud`` gate."""
        config_path = _write_project(
            tmp_path,
            endpoint=_HTTP_OFF,
            jwt_section=_JWT_WITHOUT_AUDIENCE,
            a2a=_A2A_SECTION,
        )

        with pytest.raises(ConfigError, match="audience"):
            create_app(config_path)

    @pytest.mark.usefixtures("fake_engine")
    def test_names_the_agent_when_the_jwt_does_not_validate_audience(self, tmp_path: Path) -> None:
        """The operator is told which exposed agent blocks start-up."""
        config_path = _write_project(
            tmp_path,
            endpoint=_HTTP_OFF,
            jwt_section=_JWT_WITHOUT_AUDIENCE,
            a2a=_A2A_SECTION,
        )

        with pytest.raises(ConfigError) as failure:
            create_app(config_path)

        assert _AGENT in str(failure.value)

    @pytest.mark.usefixtures("fake_engine")
    def test_starts_when_the_jwt_validates_audience(self, tmp_path: Path) -> None:
        """A validated ``aud`` is all the gate asks for, here too."""
        config_path = _write_project(
            tmp_path,
            endpoint=_HTTP_OFF,
            jwt_section={**_JWT_WITHOUT_AUDIENCE, "audience": "loom-api"},
            a2a=_A2A_SECTION,
        )

        assert create_app(config_path) is not None

    @pytest.mark.usefixtures("fake_engine")
    def test_starts_without_audience_when_the_published_agent_is_anonymous(
        self, tmp_path: Path
    ) -> None:
        """An agent that verifies no caller has no ``aud`` to validate."""
        config_path = _write_project(
            tmp_path,
            endpoint={"enabled": True, "auth": "identity", "allow_anonymous": True},
            jwt_section=_JWT_WITHOUT_AUDIENCE,
            a2a=_A2A_SECTION,
        )

        assert create_app(config_path) is not None
