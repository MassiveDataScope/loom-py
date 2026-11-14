"""A native carrier's tools act as the identity acting, and are refused past its delegation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest

from loom.ai.compiler import CompiledMcpCapability
from loom.ai.engines.pydantic_ai import NativeAgent, _capabilities
from loom.core.di import LoomContainer
from loom.core.errors import Unauthenticated
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from loom.core.observability.event import EventKind, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from tests.integration.ai.conftest import RecordingObserver
from tests.integration.ai.test_capabilities import (
    CapabilityDepsFactory,
    ScriptedToolModel,
    build_engine,
    python_capability,
    stub_mcp_server,
)
from tests.unit.ai.engines.test_a2a_capability import REMOTE_TOOL, a2a_capability

from .doubles import ALICE

BOB = Identity(subject="acme-bob", roles=("analyst",), mechanism="test")


def _python(calls: list[str], monkeypatch: pytest.MonkeyPatch) -> tuple[Any, str]:
    del monkeypatch
    return python_capability(calls), "ping"


def _mcp(calls: list[str], monkeypatch: pytest.MonkeyPatch) -> tuple[Any, str]:
    monkeypatch.setattr(_capabilities, "_mcp_server", stub_mcp_server(calls))
    return CompiledMcpCapability(server="tools", url="https://tools.internal/mcp"), "remote_ping"


def _a2a(calls: list[str], monkeypatch: pytest.MonkeyPatch) -> tuple[Any, str]:
    async def transport(capability: object, prompt: str) -> str:
        del capability
        calls.append(prompt)
        return "42"

    monkeypatch.setattr(_capabilities, "send_to_remote_agent", transport)
    return a2a_capability(), REMOTE_TOOL


_GRANTS = {"python": _python, "mcp": _mcp, "a2a": _a2a}


@asynccontextmanager
async def _alice_guard() -> AsyncIterator[None]:
    """The guard the runtime hands over: the run's body is an execution acting as Alice."""
    with ExecutionScope(ALICE):
        yield


@pytest.fixture(params=list(_GRANTS), ids=list(_GRANTS))
def carrier(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Build Alice's carrier over one granted toolset, with the backend's calls and tool spans."""
    calls: list[str] = []
    recorder = RecordingObserver()
    container = LoomContainer()
    container.register_instance(ObservabilityRuntime, ObservabilityRuntime([recorder]))
    capability, tool = _GRANTS[request.param](calls, monkeypatch)
    model = ScriptedToolModel(calls=((tool, {"prompt": "p"} if tool == REMOTE_TOOL else {}),))
    engine: Any = build_engine(
        capabilities=(capability,),
        model=model,
        container=container,
        deps=CapabilityDepsFactory(),
    )
    access = engine.native(identity=ALICE, guard=_alice_guard)
    return access.native, access.guard, calls, recorder


def _drive(native: NativeAgent) -> Any:
    return native.agent.run("hello", deps=native.deps)


def _tool_subjects(recorder: RecordingObserver) -> list[str]:
    return [
        event.meta["subject"]
        for event in recorder.events
        if event.scope is Scope.TOOL and event.kind is EventKind.START
    ]


def _leaf(error: BaseException) -> BaseException:
    while isinstance(error, BaseExceptionGroup) and len(error.exceptions) == 1:
        error = error.exceptions[0]
    return error


class TestARetainedCarrier:
    async def test_is_refused_past_the_delegation_with_no_backend_call(
        self, carrier: Any, past_the_window: Callable[[], None]
    ) -> None:
        native, _guard, calls, _recorder = carrier
        release = asyncio.Event()

        async def later() -> object:
            await release.wait()
            return await _drive(native)

        with ExecutionScope(ALICE):
            task = asyncio.get_running_loop().create_task(later())
        past_the_window()
        release.set()

        with pytest.raises(BaseException) as refused:  # noqa: PT011 - the group is unwrapped below
            await task

        assert isinstance(_leaf(refused.value), Unauthenticated)
        assert calls == []

    async def test_runs_its_tools_inside_its_guard_as_its_caller(self, carrier: Any) -> None:
        native, guard, calls, recorder = carrier

        async with guard():
            await _drive(native)

        assert len(calls) == 1
        assert _tool_subjects(recorder) == [ALICE.subject]

    @pytest.mark.parametrize("acting", [ALICE, BOB], ids=["its-caller", "another-caller"])
    async def test_runs_its_tools_as_the_identity_acting(
        self, carrier: Any, acting: Identity
    ) -> None:
        native, _guard, calls, recorder = carrier

        with ExecutionScope(acting):
            await _drive(native)

        assert len(calls) == 1
        assert _tool_subjects(recorder) == [acting.subject]
