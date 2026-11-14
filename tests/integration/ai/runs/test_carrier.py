"""A native carrier's tools act as the identity acting, and are refused past its delegation."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest

from loom.ai.compiler import CompiledMcpCapability, CompiledUsecaseCapability
from loom.ai.engines.pydantic_ai import NativeAgent, _capabilities
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.di import LoomContainer
from loom.core.errors import Unauthenticated
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from loom.core.observability.event import EventKind, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.service import SqlQueryService
from loom.core.use_case import Caller, UseCase
from tests.integration.ai.conftest import RecordingDepsFactory, RecordingObserver
from tests.integration.ai.test_capabilities import (
    ALLOWED_ROLE,
    ANALYST,
    ROLELESS,
    SQL_TOOL,
    CapabilityDepsFactory,
    RecordingSqlQueryService,
    ScriptedToolModel,
    build_engine,
    canary_result,
    python_capability,
    sql_capability,
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


@dataclass
class CallersSeen:
    """Every caller :class:`AcmeCallerProbe` ran as, in order."""

    callers: list[Identity] = field(default_factory=list)


class AcmeCallerProbe(UseCase[object, str]):
    """A granted use case recording the caller its execution acts as."""

    def __init__(self, seen: CallersSeen) -> None:
        self._seen = seen

    async def execute(self, caller: Identity = Caller()) -> str:
        self._seen.callers.append(caller)
        return caller.subject


class Governed:
    """A carrier retained for one caller over a ``usecase`` or ``sql`` grant, and what it reached.

    The bundles are the composition root's, over a real executor.

    Args:
        kind: ``usecase`` or ``sql``.
    """

    def __init__(self, kind: str) -> None:
        self.kind = kind
        self.seen = CallersSeen()
        self.sql = RecordingSqlQueryService(canary_result(1))
        self.recorder = RecordingObserver()
        self.deps = RecordingDepsFactory((AcmeCallerProbe,))

    def built_for(self, identity: Identity) -> NativeAgent:
        """Build the carrier, its bundle built for *identity*."""
        container = LoomContainer()
        container.register_instance(ObservabilityRuntime, ObservabilityRuntime([self.recorder]))
        container.register_instance(CallersSeen, self.seen)
        container.register_instance(SqlQueryService, self.sql)
        self.deps.registered_in(container)
        if self.kind == "usecase":
            capability: Any = CompiledUsecaseCapability(
                keys=("acme:probe",), use_cases=(AcmeCallerProbe,)
            )
            call: tuple[str, dict[str, Any]] = ("usecase_acme_probe", {})
        else:
            capability, call = sql_capability(), (SQL_TOOL, {"sql": "SELECT 1"})
        engine: Any = build_engine(
            capabilities=(capability,),
            model=ScriptedToolModel(calls=(call,)),
            container=container,
            deps=self.deps,
        )
        return engine.native(identity=identity, guard=_alice_guard).native

    @property
    def reached(self) -> list[object]:
        """What the backend saw each call act as: the use case's caller, or the query's roles."""
        if self.kind == "usecase":
            return list(self.seen.callers)
        return [call.roles for call in self.sql.calls]

    def reached_acting_as(self, identity: Identity) -> list[object]:
        """What :attr:`reached` holds for one call acting as *identity*."""
        return [identity] if self.kind == "usecase" else [(ALLOWED_ROLE,)]


@pytest.fixture(params=["usecase", "sql"])
def governed(request: pytest.FixtureRequest) -> Governed:
    """A ``usecase`` or ``sql`` carrier and its recording backends."""
    return Governed(request.param)


class TestARetainedGovernedCarrier:
    async def test_acts_as_the_identity_acting_not_the_one_it_was_built_for(
        self, governed: Governed
    ) -> None:
        native = governed.built_for(ROLELESS)

        with ExecutionScope(ANALYST):
            await _drive(native)

        assert governed.reached == governed.reached_acting_as(ANALYST)
        assert _tool_subjects(governed.recorder) == [ANALYST.subject]

    async def test_a_query_acting_without_an_allowed_role_is_refused(self) -> None:
        governed = Governed("sql")
        native = governed.built_for(ANALYST)

        with pytest.raises(AgentRunError) as refused, ExecutionScope(ROLELESS):
            await _drive(native)

        assert (refused.value.code, governed.reached) == (AgentRunErrorCode.UNAUTHORIZED, [])

    async def test_is_refused_past_the_delegation_with_no_backend_call(
        self, governed: Governed, past_the_window: Callable[[], None]
    ) -> None:
        native = governed.built_for(ANALYST)
        release = asyncio.Event()

        async def later() -> object:
            await release.wait()
            return await _drive(native)

        with ExecutionScope(ANALYST):
            task = asyncio.get_running_loop().create_task(later())
        past_the_window()
        release.set()

        with pytest.raises(BaseException) as refused:  # noqa: PT011 - the group is unwrapped below
            await task

        assert isinstance(_leaf(refused.value), Unauthenticated)
        assert governed.reached == []
