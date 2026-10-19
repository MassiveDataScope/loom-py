"""Single source of agent artifacts: manifest ``AGENTS`` or ``ai.specs`` (§4).

Drives :func:`loom.rest.fastapi.auto.create_app` over a temporary project
discovered by manifest, mirroring ``tests/integration/ai/test_auto_gate.py``.
The two declaration sites are mutually exclusive — an implicit precedence
would silently ignore half of what an operator declared — so the four
combinations are pinned here: only ``AGENTS``, both, neither, and ``AGENTS``
without an ``ai:`` section at all.

The engine entry point is faked in-process and the app lifespan is never
entered: no provider SDK, no network, no credential.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.core.config.errors import ConfigError
from loom.core.plugins import entrypoints as entrypoints_module
from loom.rest.fastapi.auto import create_app, describe_fastapi_app
from tests.integration.ai._entrypoints import fake_entry_points
from tests.integration.ai.conftest import CountingEngineProvider

_APP_MODULE = "loom_aispecs_fixture_app"
_MANIFEST_MODULE = "loom_aispecs_fixture_manifest"
_ENGINE_NAME = "aispecs-fake"
_GROUP = "loom.ai.engines"

_AGENT = "specs-source-agent"
_APP_NAME = "aispecs-demo"
_SPEC_GLOB = "ai/agents/*/agent.yaml"

_APP_SOURCE = '''\
"""Minimal discoverable app used by the artifact-source tests."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class AiSpecsRecord(BaseModel):
    __tablename__ = "aispecs_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class AiSpecsPingUseCase(UseCase[AiSpecsRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class AiSpecsPingInterface(RestInterface[str]):
    prefix = "/aispecs-ping"
    routes = (RestRoute(use_case=AiSpecsPingUseCase, method="GET", path="/"),)
'''

_MANIFEST_SOURCE = """\
\"\"\"Manifest declaring the fixture interface and, optionally, its agents.\"\"\"

from __future__ import annotations

from {app_module} import AiSpecsPingInterface, AiSpecsRecord

MODELS = [AiSpecsRecord]
INTERFACES = [AiSpecsPingInterface]
AGENTS = {agents!r}
"""

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


class _FakeDist:
    """Minimal stand-in for ``importlib.metadata.Distribution``."""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    """Entry point resolving the fake engine provider of these tests."""

    def __init__(self) -> None:
        self.name = _ENGINE_NAME
        self.group = _GROUP
        self.dist = _FakeDist("loom-aispecs-tests")

    def load(self) -> object:
        """Return the provider class the registry instantiates."""
        return CountingEngineProvider


@pytest.fixture(autouse=True)
def fresh_manifest() -> Iterator[None]:
    """Re-import the manifest per test: each writes a different ``AGENTS``."""
    sys.modules.pop(_MANIFEST_MODULE, None)
    yield
    sys.modules.pop(_MANIFEST_MODULE, None)


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the in-process engine ``ai.engine`` resolves to."""
    monkeypatch.setattr(
        entrypoints_module, "entry_points", fake_entry_points(_GROUP, (_FakeEntryPoint(),))
    )


def _write_project(
    tmp_path: Path,
    *,
    manifest_agents: list[str],
    config_specs: list[str] | None,
    with_ai_section: bool = True,
) -> str:
    """Write the fixture app, its manifest, one agent artifact and the config.

    Args:
        tmp_path: Directory the project is written to.
        manifest_agents: Value of the manifest ``AGENTS`` attribute.
        config_specs: Value of ``ai.specs``, or ``None`` to omit the key.
        with_ai_section: Whether the configuration declares an ``ai:`` section.

    Returns:
        Path of the written configuration file.
    """
    (tmp_path / f"{_APP_MODULE}.py").write_text(_APP_SOURCE, encoding="utf-8")
    (tmp_path / f"{_MANIFEST_MODULE}.py").write_text(
        _MANIFEST_SOURCE.format(app_module=_APP_MODULE, agents=manifest_agents),
        encoding="utf-8",
    )
    agent_dir = tmp_path / "ai" / "agents" / _AGENT
    agent_dir.mkdir(parents=True)
    (agent_dir / "agent.yaml").write_text(yaml.safe_dump(_AGENT_SPEC), encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": _APP_NAME,
            "code_path": str(tmp_path),
            "discovery": {"mode": "manifest", "manifest": {"module": _MANIFEST_MODULE}},
        },
        "database": {"url": "sqlite+aiosqlite:///"},
        "persistence": {"cursors": {"signing": "ephemeral"}},
    }
    if with_ai_section:
        ai_section: dict[str, Any] = {
            "engine": _ENGINE_NAME,
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
        }
        if config_specs is not None:
            ai_section["specs"] = config_specs
        config["ai"] = ai_section
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _codes(failure: AgentCompilationError) -> tuple[AgentErrorCode, ...]:
    """Codes of every issue the compilation reported."""
    return tuple(issue.code for issue in failure.issues)


class TestManifestAsTheOnlySource:
    """A manifest may be the only place the agents of an application are declared."""

    @pytest.mark.usefixtures("fake_engine")
    def test_compiles_the_agents_when_only_the_manifest_declares_them(self, tmp_path: Path) -> None:
        """``AGENTS`` alone compiles the artifacts it names."""
        config_path = _write_project(tmp_path, manifest_agents=[_SPEC_GLOB], config_specs=[])

        description = describe_fastapi_app(create_app(config_path))

        assert [agent["name"] for agent in description["agents"]] == [_AGENT]


class TestExclusiveSources:
    """Declaring the artifacts twice is an error, never a silent precedence."""

    @pytest.mark.usefixtures("fake_engine")
    def test_fails_when_the_manifest_and_the_config_both_declare_agents(
        self, tmp_path: Path
    ) -> None:
        """Two sources means half the declaration would be ignored."""
        config_path = _write_project(
            tmp_path, manifest_agents=[_SPEC_GLOB], config_specs=[_SPEC_GLOB]
        )

        with pytest.raises(AgentCompilationError) as failure:
            create_app(config_path)

        assert AgentErrorCode.AGENT_SPECS_CONFLICT in _codes(failure.value)

    @pytest.mark.usefixtures("fake_engine")
    def test_fails_when_no_source_declares_agents(self, tmp_path: Path) -> None:
        """An ``ai:`` section that names no artifact configures nothing."""
        config_path = _write_project(tmp_path, manifest_agents=[], config_specs=None)

        with pytest.raises(AgentCompilationError) as failure:
            create_app(config_path)

        assert AgentErrorCode.AGENT_SPECS_MISSING in _codes(failure.value)


class TestManifestWithNoAiSection:
    """Agents declared with no deployment to compile them against are refused."""

    def test_fails_when_the_manifest_declares_agents_with_no_ai_section(
        self, tmp_path: Path
    ) -> None:
        """The operator is told which attribute and which section disagree."""
        config_path = _write_project(
            tmp_path,
            manifest_agents=[_SPEC_GLOB],
            config_specs=None,
            with_ai_section=False,
        )

        with pytest.raises(ConfigError) as failure:
            create_app(config_path)

        message = str(failure.value)
        assert "AGENTS" in message
        assert "ai:" in message
