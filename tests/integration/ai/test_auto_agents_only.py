"""An agents-only application serves a use case with no persistence (AC4).

Drives the real :func:`loom.rest.fastapi.auto.create_app` over a manifest that
declares one keyed use case and one agent — no models, no interfaces, no
``stores`` section but ``stores.default.backend: none``. The engine provider is
defined here and does what a real engine does with a ``usecase`` grant: it
builds the per-invocation bundle through the ``DepsFactory`` it was handed and
runs the granted use case through the bound invoker, so the call travels the
kernel executor exactly as in production.

``ScriptedEngine`` (conftest) discards ``deps``/``container`` and the
``RecordingInvoker`` of ``test_capabilities.py`` bypasses the executor, so
neither can prove this path.
"""

from __future__ import annotations

import sys
import time
import types
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import jwt as pyjwt
import pytest
import yaml
from fastapi.testclient import TestClient

from loom.ai.abc import (
    AgentEvent,
    AgentResult,
    AgentUsage,
    Conversation,
    DepsFactory,
    FinalEvent,
    HealthStatus,
)
from loom.core.di.container import LoomContainer
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.plugins import entrypoints as entrypoints_module
from loom.core.use_case.keys import use_case_key
from loom.core.use_case.use_case import UseCase
from loom.rest.fastapi.auto import create_app
from tests.integration.ai._entrypoints import fake_entry_points

_MANIFEST_MODULE = "tests.integration.ai._agents_only_manifest"
_ENGINE_NAME = "agentsonly-inprocess-fake"
_AGENT = "ping-agent"
_GROUP = "loom.ai.engines"
_SECRET = "agents-only-integration-secret-of-32-bytes"
_AUDIENCE = "loom-api"

_AGENT_SPEC: dict[str, Any] = {
    "spec_version": 1,
    "name": _AGENT,
    "description": "Pings the application and reports the reply.",
    "instructions": "Run the ping operation and report its reply verbatim.",
    "output": {
        "kind": "json_schema",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["answer"],
            "properties": {"answer": {"type": "string", "description": "Ping reply."}},
        },
    },
    "capabilities": [{"kind": "usecase", "keys": ["ping"]}],
}

# Module-level side-effect log the use case appends to; reset per test.
PINGS: list[str] = []


@use_case_key("ping")
class PingUseCase(UseCase[Any, str]):
    """Model-less use case recording one side effect per execution."""

    async def execute(self, **kwargs: Any) -> str:
        del kwargs
        PINGS.append("pong")
        return "pong"


class _PingEngine:
    """Engine that runs the granted use case through the bound invoker.

    Attributes:
        executor: The kernel executor observed through the invoker of the last
            built bundle; ``None`` until the first run.
    """

    def __init__(self, deps: DepsFactory, container: LoomContainer) -> None:
        self._deps = deps
        self._container = container
        self.executor: RuntimeExecutor | None = None

    def run_stream(
        self,
        prompt: str,
        *,
        identity: Identity,
        conversation: Conversation | None = None,
        state: object | None = None,
    ) -> Any:
        del prompt, conversation, state

        @asynccontextmanager
        async def _stream() -> AsyncIterator[AsyncIterator[AgentEvent]]:
            yield self._events(identity)

        return _stream()

    async def _events(self, identity: Identity) -> AsyncIterator[AgentEvent]:
        bundle: Any = self._deps.build(identity, self._container)
        self.executor = bundle.invoker.executor
        reply = await bundle.invoker.invoke(PingUseCase)
        yield FinalEvent(
            output={"answer": reply},
            usage=AgentUsage(input_tokens=1, output_tokens=1, requests=1, duration_ms=0),
        )

    async def run(self, prompt: str, *, identity: Identity) -> AgentResult:
        async with self.run_stream(prompt, identity=identity) as stream:
            last: AgentEvent | None = None
            async for event in stream:
                last = event
        if isinstance(last, FinalEvent):
            return AgentResult(output=last.output, usage=last.usage)
        raise AssertionError("engine did not end in a FinalEvent")

    async def health(self) -> HealthStatus:
        return HealthStatus(status="ok")


class _EngineProvider:
    """Provider keeping ``deps``/``container`` and exposing the engines it built."""

    LOOM_AI_ENGINE_API = 2
    engines: list[_PingEngine] = []

    def create_engine(
        self, plan: object, *, deps: DepsFactory, container: LoomContainer
    ) -> _PingEngine:
        del plan
        engine = _PingEngine(deps, container)
        type(self).engines.append(engine)
        return engine

    def supported_capability_kinds(self) -> frozenset[str]:
        return frozenset({"usecase"})


class _FakeDist:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    def __init__(self) -> None:
        self.name = _ENGINE_NAME
        self.group = _GROUP
        self.dist = _FakeDist("loom-agentsonly-inprocess-tests")

    def load(self) -> object:
        return _EngineProvider


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the in-process engine ``ai.engine`` resolves to, with a clean log."""
    monkeypatch.setattr(
        entrypoints_module, "entry_points", fake_entry_points(_GROUP, (_FakeEntryPoint(),))
    )
    monkeypatch.setattr(_EngineProvider, "engines", [])
    PINGS.clear()


@pytest.fixture
def manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    """Register the agents-only manifest module: use cases and agents, nothing else."""
    module = types.ModuleType(_MANIFEST_MODULE)
    module.USE_CASES = [PingUseCase]  # type: ignore[attr-defined]
    module.AGENTS = ["agents/*.yaml"]  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, _MANIFEST_MODULE, module)


@pytest.fixture
def executed(monkeypatch: pytest.MonkeyPatch) -> list[type[Any]]:
    """Spy on ``RuntimeExecutor.execute`` and collect the use-case types it ran."""
    seen: list[type[Any]] = []
    original = RuntimeExecutor.execute

    async def _spy(self: RuntimeExecutor, compilable: Any, *args: Any, **kwargs: Any) -> Any:
        seen.append(type(compilable))
        return await original(self, compilable, *args, **kwargs)

    monkeypatch.setattr(RuntimeExecutor, "execute", _spy)
    return seen


def _write_project(tmp_path: Path) -> str:
    """Write the agent artifact, the JWT secret and the YAML config; return the config path.

    A ``usecase`` grant refuses anonymous callers at compile time, so the
    endpoint is verified by the same HS256/``aud`` gate ``test_auto_gate.py``
    boots with.
    """
    agents = tmp_path / "agents"
    agents.mkdir()
    (agents / f"{_AGENT}.yaml").write_text(yaml.safe_dump(_AGENT_SPEC), encoding="utf-8")
    secret_path = tmp_path / "hs.key"
    secret_path.write_text(_SECRET, encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "agentsonly-inprocess-demo",
            "code_path": str(tmp_path),
            "discovery": {
                "mode": "manifest",
                "manifest": {"module": _MANIFEST_MODULE},
            },
            "rest": {
                "auth": {
                    "jwt": {
                        "secret_path": str(secret_path),
                        "algorithms": ["HS256"],
                        "audience": _AUDIENCE,
                    }
                }
            },
        },
        "stores": {"default": {"backend": "none"}},
        "ai": {
            "engine": _ENGINE_NAME,
            "models": {"default": {"provider": "fake", "model": "fake-model"}},
            "endpoints": {_AGENT: {"enabled": True, "auth": "identity"}},
        },
    }
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)


def _bearer() -> dict[str, str]:
    """Mint one HS256 token the configured gate accepts."""
    token = pyjwt.encode(
        {"sub": "operator-1", "aud": _AUDIENCE, "exp": int(time.time()) + 3600},
        _SECRET,
        algorithm="HS256",
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.usefixtures("fake_engine", "manifest")
class TestAgentsOnlyApp:
    """AC4: a manifest with use cases and agents alone serves them without persistence."""

    def test_the_agent_runs_the_use_case_through_the_executor_without_a_uow(
        self, tmp_path: Path, executed: list[type[Any]]
    ) -> None:
        """The endpoint answers, the side effect happens once, and no UoW is involved."""
        config_path = _write_project(tmp_path)

        with TestClient(create_app(config_path)) as client:
            response = client.post(
                f"/agents/{_AGENT}/run", json={"prompt": "ping"}, headers=_bearer()
            )

        assert response.status_code == 200, response.text
        assert response.json()["output"] == {"answer": "pong"}
        assert PINGS == ["pong"]
        assert executed == [PingUseCase]
        (engine,) = _EngineProvider.engines
        assert isinstance(engine.executor, RuntimeExecutor)
        assert engine.executor._coordinator is None  # noqa: SLF001
