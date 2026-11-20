"""The state channel through the runtime (T303): FR-008, FR-009, FR-010.

``state`` reaches these seams already decoded and normalised by whichever
boundary received it (FR-008); the runtime's own job is to check it against
the target plan's declared shape and hand the result to the engine
unchanged, never to parse it again.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel, ConfigDict

from loom.ai.abc import StateShape
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.execution import AgentHost
from loom.ai.execution._handle import _AgentHandle
from loom.core.di import LoomContainer
from loom.core.execution import ExecutionScope
from loom.core.identity import Identity
from loom.core.model import loom_type, msgspec_type
from loom.core.stores.sql.service import NullSqlQueryService
from tests.integration.ai.conftest import (
    CountingEngineProvider,
    RecordingScriptedEngine,
    StubDepsFactory,
    make_ai_config,
    make_plan,
)
from tests.unit.ai.execution.doubles import AcmeDefaultedState, AcmeRequiredState

_AGENT = "appraiser"
_AUTHENTICATED = Identity(subject="ada", mechanism="test")


_SCHEMA_STATE = StateShape(schema={"type": "object"}, loom_type=msgspec_type(AcmeDefaultedState))


_REQUIRED_SCHEMA_STATE = StateShape(
    schema={"type": "object", "properties": {"marca": {"type": "string"}}, "required": ["marca"]},
    loom_type=msgspec_type(AcmeRequiredState),
)


class _DefaultedPydanticState(BaseModel):
    """A pydantic state shape whose every field carries a declared default."""

    model_config = ConfigDict(extra="forbid")

    marca: str = ""
    km: int = 0


_PYDANTIC_SCHEMA_STATE = StateShape(
    schema={"type": "object"}, loom_type=loom_type(_DefaultedPydanticState)
)


class _RequiredPydanticState(BaseModel):
    """A pydantic state shape with one field carrying no declared default."""

    model_config = ConfigDict(extra="forbid")

    marca: str


_REQUIRED_PYDANTIC_SCHEMA_STATE = StateShape(
    schema={"type": "object", "properties": {"marca": {"type": "string"}}, "required": ["marca"]},
    loom_type=loom_type(_RequiredPydanticState),
)


def _runtime(plan: Any, engine: RecordingScriptedEngine) -> AgentHost:
    return AgentHost(
        plans=[plan],
        config=make_ai_config(),
        engine_plugin=CountingEngineProvider(engines={_AGENT: engine}),  # type: ignore[arg-type]
        deps=StubDepsFactory(),
        container=LoomContainer(),
    )


class TestStateReachesTheEngine:
    async def test_a_non_empty_state_arrives_at_the_engine_intact(self) -> None:
        engine = RecordingScriptedEngine()
        runtime = _runtime(make_plan(_AGENT, state=_SCHEMA_STATE), engine)
        async with runtime:
            await runtime.run(
                _AGENT,
                "hola",
                identity=_AUTHENTICATED,
                state={"marca": "civic", "km": 12},
            )
        assert engine.states == [{"marca": "civic", "km": 12}]

    async def test_run_stream_resolves_state_the_same_way_as_run(self) -> None:
        engine = RecordingScriptedEngine()
        runtime = _runtime(make_plan(_AGENT, state=_SCHEMA_STATE), engine)
        async with (
            runtime,
            runtime.run_stream(
                _AGENT, "hola", identity=_AUTHENTICATED, state={"marca": "civic", "km": 3}
            ) as events,
        ):
            async for _ in events:
                pass
        assert engine.states == [{"marca": "civic", "km": 3}]

    async def test_state_reaches_the_engine_through_a_marker_resolved_handle(self) -> None:
        """The same channel, driven from ``AgentHandle.run`` (FR-007)."""
        engine = RecordingScriptedEngine()
        runtime = _runtime(make_plan(_AGENT, state=_SCHEMA_STATE), engine)
        handle = _AgentHandle(
            name=_AGENT,
            runtime=runtime,
            observability=None,
            sql_query_service=NullSqlQueryService(),
        )
        async with runtime:
            with ExecutionScope(_AUTHENTICATED):
                await handle.run("hola", state={"marca": "civic", "km": 7})
        assert engine.states == [{"marca": "civic", "km": 7}]


class TestNoneStateOnAStatefulArtefact:
    """A stateful artefact's own defaults fill in a ``None`` state, in either library."""

    @pytest.mark.parametrize(
        "shape", [_SCHEMA_STATE, _PYDANTIC_SCHEMA_STATE], ids=["msgspec", "pydantic"]
    )
    async def test_carries_the_shapes_declared_defaults_not_a_failure(
        self, shape: StateShape
    ) -> None:
        engine = RecordingScriptedEngine()
        runtime = _runtime(make_plan(_AGENT, state=shape), engine)
        async with runtime:
            await runtime.run(_AGENT, "hola", identity=_AUTHENTICATED)
        assert engine.states == [{"marca": "", "km": 0}]


class TestNoneStateAgainstARequiredField:
    """A ``None`` state against a required field fails coded, in either library."""

    @pytest.mark.parametrize(
        "shape",
        [_REQUIRED_SCHEMA_STATE, _REQUIRED_PYDANTIC_SCHEMA_STATE],
        ids=["msgspec", "pydantic"],
    )
    async def test_fails_coded_instead_of_crashing_with_a_raw_validation_error(
        self, shape: StateShape
    ) -> None:
        engine = RecordingScriptedEngine()
        runtime = _runtime(make_plan(_AGENT, state=shape), engine)
        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await runtime.run(_AGENT, "hola", identity=_AUTHENTICATED)

        assert excinfo.value.code == AgentRunErrorCode.STATE_REQUIRED
        assert engine.stream_count == 0
        assert engine.states == []


class TestRuntimeStateAgainstNoDeclaredShape:
    async def test_fails_before_any_provider_call_with_no_usage_and_no_health_record(
        self,
    ) -> None:
        engine = RecordingScriptedEngine()
        runtime = _runtime(make_plan(_AGENT), engine)
        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await runtime.run(_AGENT, "hola", identity=_AUTHENTICATED, state={"anything": 1})

        assert excinfo.value.code == AgentRunErrorCode.STATE_UNDECLARED
        assert excinfo.value.usage is None
        # No provider call: the scripted engine's own stream was never opened,
        # so it recorded neither usage nor a health outcome.
        assert engine.stream_count == 0
        assert engine.states == []

    async def test_names_the_agent_in_the_refusal(self) -> None:
        engine = RecordingScriptedEngine()
        runtime = _runtime(make_plan(_AGENT), engine)
        async with runtime:
            with pytest.raises(AgentRunError) as excinfo:
                await runtime.run(_AGENT, "hola", identity=_AUTHENTICATED, state={"x": 1})

        assert _AGENT in str(excinfo.value)

    async def test_a_none_state_is_unaffected(self) -> None:
        """FR-010 refuses a non-empty ``state``; ``None`` is always allowed."""
        engine = RecordingScriptedEngine()
        runtime = _runtime(make_plan(_AGENT), engine)
        async with runtime:
            await runtime.run(_AGENT, "hola", identity=_AUTHENTICATED)
        assert engine.states == [None]
