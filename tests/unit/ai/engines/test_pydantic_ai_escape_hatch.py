"""``PydanticAIEngine.native`` and ``native_agent``: the escape hatch out of loom.

Pins the properties the hatch is worth having for: it hands over the very
agent the engine runs — not a copy, not a rebuild — the dependency bundle it
comes with is built for the asking caller, the artefact's spend caps come
back as a copy no caller can use to raise the engine's own enforced cap, and
a plan declaring ``output_check`` is served by two distinct agents, one for
its own declared shape and one for a per-run override. The refusal pins the
other half: a foreign engine's native form is named as such rather than cast
into ours.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models import Model
from pydantic_ai.models.function import AgentInfo, FunctionModel

from loom.ai.abc import AgentHandle
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai import NativeAgent, PydanticAIEnginePlugin, native_agent
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.execution import AgentRuntime
from loom.ai.execution._chain import current_chain
from loom.core.di import LoomContainer
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import make_plan
from tests.integration.ai.conftest import make_ai_config

_IDENTITY = Identity(subject="caller")


@asynccontextmanager
async def _noop_guard() -> AsyncIterator[None]:
    """A guard entered by no test in this module: their runs stay unsupervised on purpose."""
    yield


def _accept(_: Mapping[str, Any]) -> str | None:
    """An ``output_check`` that never rejects; only its presence matters here."""
    return None


@dataclass(frozen=True, slots=True)
class _Bundle:
    """A dependency bundle that remembers what it was built from."""

    identity: Identity
    state: Mapping[str, Any] | None


class _RecordingDeps:
    """Dependency factory handing back the bundle's own inputs."""

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return a bundle carrying this invocation's identity and state."""
        del container
        return _Bundle(identity=identity, state=state)


class _ForeignHandle:
    """A handle whose agent is served by some other engine."""

    def native(self, *, state: object | None = None) -> object:
        """Return a native form this package cannot name."""
        del state
        return object()


class _NativeHandle:
    """A handle over the real engine, standing in for a resolved marker."""

    def __init__(self, engine: PydanticAIEngine, identity: Identity) -> None:
        self._engine = engine
        self._identity = identity

    def native(self, *, state: object | None = None) -> object:
        """Return the engine's native form for this handle's caller."""
        return self._engine.native(identity=self._identity, state=state, guard=_noop_guard)


def _silent_model() -> Model:
    """A model that never has to answer: no run is started in this module."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del messages, info
        return ModelResponse(parts=[TextPart(content="unused")])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        del messages, info
        yield "unused"

    return FunctionModel(respond, stream_function=stream)


def _engine(*, output_check: Any = None) -> PydanticAIEngine:
    model = _silent_model()
    provider = PydanticAIEnginePlugin(model_resolver=lambda target: model)
    engine = provider.create_engine(
        make_plan(output_check=output_check),
        deps=_RecordingDeps(),
        container=LoomContainer(),
    )
    assert isinstance(engine, PydanticAIEngine)
    return engine


def _native(
    engine: PydanticAIEngine, *, identity: Identity = _IDENTITY, **kwargs: Any
) -> NativeAgent:
    """Call the engine's ``native()`` with the module's own no-op guard, unwrapping its carrier."""
    return engine.native(identity=identity, guard=_noop_guard, **kwargs).native


def _as_handle(handle: object) -> AgentHandle[Any]:
    """Present a stub as the handle ``native_agent`` declares it takes."""
    return cast("AgentHandle[Any]", handle)


class TestTheHatchHandsOverTheRunningAgent:
    def test_the_agent_is_the_one_the_engine_itself_runs(self) -> None:
        engine = _engine()

        first = _native(engine)
        second = _native(engine)

        assert first.agent is second.agent

    def test_the_spend_caps_are_the_artefacts_own_projected_ones(self) -> None:
        policies = PolicySpec(max_requests=7, max_usd=Decimal("1.50"))
        engine = _engine_with_policies(policies)

        access = _native(engine)

        assert access.usage_limits.request_limit == 7
        assert access.usage_limits.cost_limit == Decimal("1.50")

    def test_mutating_the_returned_caps_never_reaches_the_engines_own(self) -> None:
        """A caller raising its own copy's cap must not raise every supervised run's."""
        engine = _engine()

        first = _native(engine)
        first.usage_limits.request_limit = 999_999
        second = _native(engine)

        assert second.usage_limits.request_limit != 999_999


class TestTheShapedAgent:
    """``shaped_agent``: the same spec, served with no output validator (T304)."""

    def test_with_no_output_check_both_agents_are_the_same_object(self) -> None:
        engine = _engine(output_check=None)

        access = _native(engine)

        assert access.agent is access.shaped_agent

    def test_with_an_output_check_the_two_agents_differ(self) -> None:
        engine = _engine(output_check=_accept)

        access = _native(engine)

        assert access.agent is not access.shaped_agent


class TestTheBundleBelongsToTheAskingCaller:
    def test_the_bundle_carries_the_asking_identity(self) -> None:
        engine = _engine()
        other = Identity(subject="someone-else")

        mine = _native(engine, identity=_IDENTITY)
        theirs = _native(engine, identity=other)

        assert isinstance(mine.deps, _Bundle)
        assert isinstance(theirs.deps, _Bundle)
        assert mine.deps.identity == _IDENTITY
        assert theirs.deps.identity == other

    def test_state_reaches_the_bundle_unchanged(self) -> None:
        engine = _engine()
        state = {"ticket": "INC-1"}

        access = _native(engine, state=state)

        assert isinstance(access.deps, _Bundle)
        assert access.deps.state == state


class TestTheTypedAccessor:
    async def test_a_handle_on_this_engine_yields_the_engines_own_carrier(self) -> None:
        engine = _engine()

        async with native_agent(_as_handle(_NativeHandle(engine, _IDENTITY))) as access:
            assert isinstance(access, NativeAgent)
            assert access.agent is _native(engine).agent

    async def test_a_handle_on_another_engine_is_named_rather_than_cast(self) -> None:
        with pytest.raises(TypeError) as excinfo:
            async with native_agent(_as_handle(_ForeignHandle())):
                pass

        assert "not served by the pydantic-ai engine" in str(excinfo.value)

    async def test_state_travels_through_the_accessor(self) -> None:
        async with native_agent(
            _as_handle(_NativeHandle(_engine(), _IDENTITY)), state={"a": 1}
        ) as access:
            assert isinstance(access.deps, _Bundle)
            assert access.deps.state == {"a": 1}


def _native_runtime() -> AgentRuntime:
    provider = PydanticAIEnginePlugin(model_resolver=lambda target: _silent_model())
    return AgentRuntime(
        plans=[make_plan()],
        config=make_ai_config(),
        engine_plugin=provider,
        deps=_RecordingDeps(),
        container=LoomContainer(),
    )


class _RuntimeHandle:
    """A handle over the real runtime, standing in for a resolved marker."""

    def __init__(self, runtime: AgentRuntime, name: str, identity: Identity) -> None:
        self._runtime = runtime
        self._name = name
        self._identity = identity

    def native(self, *, state: object | None = None) -> object:
        """Return the runtime's native form for this handle's agent and caller."""
        return self._runtime.native(self._name, identity=self._identity, state=state)


class TestTheAccessorRejoinsTheGuard:
    """``native_agent`` enters the runtime's own guard, not a no-op body."""

    async def test_the_chain_records_this_run_only_inside_the_block(self) -> None:
        async with _native_runtime() as runtime:
            handle = _as_handle(_RuntimeHandle(runtime, "contract", _IDENTITY))

            assert current_chain() == ()
            with ExecutionScope(_IDENTITY):
                async with native_agent(handle):
                    assert current_chain() == ("contract",)
            assert current_chain() == ()


class TestTheBodysExceptionsAreNeverReclassified:
    """Change 1's regression guard: ``native_agent`` funnels nothing through ``as_run_error``.

    Rejoining the guard is real supervision; reclassifying whatever the body
    raises is not — it would turn the application's own exceptions into a
    retriable provider outage. Both a raw exception and an already-coded
    ``AgentRunError`` must reach the caller as the very same instance.
    """

    async def test_a_raw_exception_escapes_unchanged(self) -> None:
        original = TimeoutError("the vendor call timed out")

        async with _native_runtime() as runtime:
            handle = _as_handle(_RuntimeHandle(runtime, "contract", _IDENTITY))
            with ExecutionScope(_IDENTITY), pytest.raises(TimeoutError) as excinfo:
                async with native_agent(handle):
                    raise original

        assert excinfo.value is original

    async def test_an_agent_run_error_escapes_unchanged(self) -> None:
        original = AgentRunError(AgentRunErrorCode.UNAUTHORIZED, "denied")

        async with _native_runtime() as runtime:
            handle = _as_handle(_RuntimeHandle(runtime, "contract", _IDENTITY))
            with ExecutionScope(_IDENTITY), pytest.raises(AgentRunError) as excinfo:
                async with native_agent(handle):
                    raise original

        assert excinfo.value is original


def _engine_with_policies(policies: PolicySpec) -> PydanticAIEngine:
    model = _silent_model()
    provider = PydanticAIEnginePlugin(model_resolver=lambda target: model)
    engine = provider.create_engine(
        make_plan(policies=policies), deps=_RecordingDeps(), container=LoomContainer()
    )
    assert isinstance(engine, PydanticAIEngine)
    return engine
