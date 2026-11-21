"""Zero compiled agent artifacts, one declaring use case (spec 015, T204).

``AiConfig.artifact_specs`` tolerates an empty artifact set
only when at least one compiled use case declares an ``Mcp()`` marker — a
deployment whose only AI usage is ``Mcp()`` no longer has to ship a filler
agent artifact it never runs. Every other empty-artifact deployment still
aborts with ``AGENT_SPECS_MISSING``, exactly as before this train.

Drives :func:`loom.rest.fastapi.auto.create_app` over a temporary project
discovered by manifest, mirroring ``tests/integration/ai/test_agent_specs_source.py``.
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
from loom.core.engine.executor import UseCaseExecutor
from loom.core.plugins import entrypoints as entrypoints_module
from loom.rest.fastapi.auto import create_app
from tests.integration.ai._entrypoints import fake_entry_points
from tests.integration.ai.conftest import CountingEngineProvider

_APP_MODULE = "loom_zeroagentmcp_fixture_app"
_MANIFEST_MODULE = "loom_zeroagentmcp_fixture_manifest"
_ENGINE_NAME = "zeroagentmcp-fake"
_GROUP = "loom.ai.engines"
_SERVER = "knowledge"
_APP_NAME = "zeroagentmcp-demo"

_APP_SOURCE = '''\
"""Minimal discoverable app: one use case that never declares Mcp()."""

from __future__ import annotations

from loom.core.use_case.use_case import UseCase


class PlainUseCase(UseCase[object, str]):
    async def execute(self) -> str:
        return "plain"
'''

_KNOWLEDGE_SOURCE = '''\
"""One use case declaring Mcp()."""

from __future__ import annotations

from loom.ai.abc import McpHandle
from loom.core.use_case import Mcp
from loom.core.use_case.use_case import UseCase


class LookUpKnowledgeUseCase(UseCase[object, str]):
    async def execute(self, gateway: McpHandle = Mcp("{server}", include=["search_*"])) -> str:
        return "ok"
'''


class _FakeDist:
    """Minimal stand-in for ``importlib.metadata.Distribution``."""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    """Entry point resolving the fake engine provider of these tests."""

    def __init__(self) -> None:
        self.name = _ENGINE_NAME
        self.group = _GROUP
        self.dist = _FakeDist("loom-zeroagentmcp-tests")

    def load(self) -> object:
        return CountingEngineProvider


@pytest.fixture(autouse=True)
def fresh_app() -> Iterator[None]:
    """Re-import the app per test: each writes a different package."""
    for name in (_APP_MODULE, f"{_APP_MODULE}.knowledge"):
        sys.modules.pop(name, None)
    yield
    for name in (_APP_MODULE, f"{_APP_MODULE}.knowledge"):
        sys.modules.pop(name, None)


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the in-process engine ``ai.engine`` resolves to."""
    monkeypatch.setattr(
        entrypoints_module, "entry_points", fake_entry_points(_GROUP, (_FakeEntryPoint(),))
    )


def _write_project(tmp_path: Path, *, declares_use_case: bool) -> str:
    """Write the fixture app package and the config.

    Args:
        tmp_path: Directory the project is written to.
        declares_use_case: Whether the package holds the ``Mcp()`` use case.

    Returns:
        Path of the written configuration file.
    """
    package = tmp_path / _APP_MODULE
    package.mkdir()
    (package / "__init__.py").write_text(_APP_SOURCE, encoding="utf-8")
    if declares_use_case:
        (package / "knowledge.py").write_text(
            _KNOWLEDGE_SOURCE.format(server=_SERVER), encoding="utf-8"
        )
    config: dict[str, Any] = {
        "app": {
            "name": _APP_NAME,
            "code_path": str(tmp_path),
            "packages": [_APP_MODULE],
        },
        "stores": {
            "default": {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///"},
            "settings": {"cursors": {"signing": "ephemeral"}},
        },
        "ai": {
            "engine": _ENGINE_NAME,
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
            "mcp_servers": {_SERVER: {"url": f"https://{_SERVER}.example.com/mcp"}},
        },
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _codes(failure: AgentCompilationError) -> tuple[AgentErrorCode, ...]:
    return tuple(issue.code for issue in failure.issues)


class TestOnlyMcpWithNoAgentArtefacts:
    """``ai.mcp_servers`` populated, zero artifacts, one use case declaring ``Mcp()``."""

    @pytest.mark.usefixtures("fake_engine")
    def test_starts_when_a_use_case_declares_the_marker(self, tmp_path: Path) -> None:
        config_path = _write_project(tmp_path, declares_use_case=True)

        create_app(config_path)  # no raise: D1 tolerates the empty artifact set

    @pytest.mark.usefixtures("fake_engine")
    def test_still_aborts_with_no_use_case_declaring_it(self, tmp_path: Path) -> None:
        config_path = _write_project(tmp_path, declares_use_case=False)

        with pytest.raises(AgentCompilationError) as failure:
            create_app(config_path)

        assert AgentErrorCode.AGENT_SPECS_MISSING in _codes(failure.value)

    @pytest.mark.usefixtures("fake_engine")
    def test_an_unknown_server_dies_as_marker_unknown_not_as_server_unknown(
        self, tmp_path: Path
    ) -> None:
        """``MarkerDeclarations.verify`` catches the unknown name before
        ``compile_mcp_capability`` sees it: ``MCP_SERVER_UNKNOWN`` never appears."""
        config_path = _write_project(tmp_path, declares_use_case=True)
        # A missing server in 'ai.mcp_servers' recreates the same YAML this
        # fixture writes, minus the one entry the use case's Mcp() names.
        broken_config: dict[str, Any] = yaml.safe_load(Path(config_path).read_text())
        broken_config["ai"]["mcp_servers"] = {}
        Path(config_path).write_text(yaml.safe_dump(broken_config), encoding="utf-8")

        with pytest.raises(AgentCompilationError) as failure:
            create_app(config_path)

        codes = _codes(failure.value)
        assert AgentErrorCode.MCP_MARKER_UNKNOWN in codes
        assert AgentErrorCode.MCP_SERVER_UNKNOWN not in codes

    @pytest.mark.usefixtures("fake_engine")
    def test_create_app_actually_calls_bind_mcp_resolver(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """H4: pins the ``create_app`` call site itself, not just the function it calls.

        ``tests/unit/rest/test_fastapi_auto_agent_markers.py`` covers
        ``bind_mcp_resolver``'s own behaviour by calling it directly, which
        would stay green even if ``create_app`` stopped calling it at all —
        exactly the mutation that leaves the ``Mcp()`` marker dead in
        production. This wraps the real method to record whether ``create_app``
        itself ever reaches it.
        """
        calls: list[Any] = []
        original = UseCaseExecutor.bind_mcp_resolver

        def _recording_bind(self: UseCaseExecutor, resolver: Any) -> None:
            calls.append(resolver)
            original(self, resolver)

        monkeypatch.setattr(UseCaseExecutor, "bind_mcp_resolver", _recording_bind)
        config_path = _write_project(tmp_path, declares_use_case=True)

        create_app(config_path)

        assert calls, "create_app never called UseCaseExecutor.bind_mcp_resolver"
