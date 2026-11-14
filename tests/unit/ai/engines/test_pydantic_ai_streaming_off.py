"""A binding declaring ``streaming: false`` asks the provider for each answer whole.

The run is still walked as events -- tool calls, tool results, the terminal
one -- but the model is never asked to stream, so a model whose streamed tool
calls arrive damaged can still be served.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import msgspec
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.output import StructuredDict
from pydantic_ai.toolsets import FunctionToolset

from loom.ai.abc import (
    ErrorEvent,
    FinalEvent,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.compiler import CompiledPythonCapability
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import build_engine, encode, make_plan
from tests.unit.ai.engines.doubles import damaged_stream_model

_IDENTITY = Identity(subject="caller")
_ANSWER: Mapping[str, str] = {"answer": "ok"}
_WHOLE = InferenceTarget(provider="bedrock", model="a-model", region="eu-west-1", streaming=False)
_STREAMED = msgspec.structs.replace(_WHOLE, streaming=True)


def _whole_only_model(payload: bytes) -> FunctionModel:
    """A model that answers only when asked whole: it declares no stream function."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, payload.decode())])

    return FunctionModel(respond)


@dataclass(frozen=True)
class _CapabilityGateBundle:
    """The attributes a capability call is gated on."""

    identity: Identity
    container: LoomContainer


class _CallerDeps:
    """A factory whose bundle lets the agent reach its tools."""

    def build(self, identity: Identity, container: LoomContainer, state: object = None) -> object:
        return _CapabilityGateBundle(identity=identity, container=container)


def _talking_model(payload: bytes) -> FunctionModel:
    """A model that says something before answering."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        said = TextPart("lo he mirado")
        return ModelResponse(
            parts=[said, ToolCallPart(info.output_tools[0].name, payload.decode())]
        )

    return FunctionModel(respond)


def _two_texts_model(payload: bytes) -> FunctionModel:
    """A model that says two things before answering."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        parts = [
            TextPart("lo he mirado"),
            TextPart("y esto es lo que veo"),
            ToolCallPart(info.output_tools[0].name, payload.decode()),
        ]
        return ModelResponse(parts=parts)

    return FunctionModel(respond)


def _tool_then_answer_model(payload: bytes) -> FunctionModel:
    """A model that calls ``lookup`` first and answers on the turn after."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        returned = any(
            isinstance(part, ToolReturnPart)
            for message in messages
            for part in getattr(message, "parts", ())
        )
        if not returned:
            return ModelResponse(parts=[ToolCallPart("_lookup", {"city": "Vigo"})])
        return ModelResponse(parts=[ToolCallPart(info.output_tools[0].name, payload.decode())])

    return FunctionModel(respond)


class TestWholeAnswer:
    async def test_the_text_of_a_whole_answer_reaches_the_caller_as_one_delta(self) -> None:
        """Whole or streamed, a caller watching the run still sees what the model said."""
        engine = build_engine(make_plan(inference=_WHOLE), _talking_model(encode(_ANSWER)))

        async with engine.run_stream("assess", identity=_IDENTITY) as events:
            seen = [event async for event in events]

        assert [event for event in seen if isinstance(event, TextDeltaEvent)] == [
            TextDeltaEvent(text="lo he mirado")
        ]
        assert isinstance(seen[-1], FinalEvent)
        assert seen[-1].output == _ANSWER

    async def test_a_whole_answer_with_two_text_parts_reaches_the_caller_as_two_deltas(
        self,
    ) -> None:
        """One delta per text part, not one per response: two parts, two deltas, in order."""
        engine = build_engine(make_plan(inference=_WHOLE), _two_texts_model(encode(_ANSWER)))

        async with engine.run_stream("assess", identity=_IDENTITY) as events:
            seen = [event async for event in events]

        assert [event for event in seen if isinstance(event, TextDeltaEvent)] == [
            TextDeltaEvent(text="lo he mirado"),
            TextDeltaEvent(text="y esto es lo que veo"),
        ]
        assert isinstance(seen[-1], FinalEvent)
        assert seen[-1].output == _ANSWER

    async def test_a_damaged_stream_is_what_asking_whole_is_for(self) -> None:
        model = damaged_stream_model(encode(_ANSWER))

        streamed = await _terminal(build_engine(make_plan(inference=_STREAMED), model))
        whole = await _terminal(build_engine(make_plan(inference=_WHOLE), model))

        assert isinstance(streamed, ErrorEvent)
        assert isinstance(whole, FinalEvent)
        assert whole.output == _ANSWER

    async def test_a_shaped_run_is_served_whole_too(self) -> None:
        """``expect=``/``run_text`` reach the same attempt, so they get the same road."""
        shape = StructuredDict({"type": "object"})
        engine = build_engine(make_plan(inference=_WHOLE), _whole_only_model(encode(_ANSWER)))

        async with engine.run_stream_shaped(
            "assess", identity=_IDENTITY, output_type=shape
        ) as events:
            seen = [event async for event in events]

        assert isinstance(seen[-1], FinalEvent)
        assert seen[-1].output == _ANSWER

    async def test_tool_calls_and_their_results_still_reach_the_caller(self) -> None:
        """A memoized node's stream must run its tool exactly once: ``run.next`` does not
        re-execute a node the caller already streamed, so an event-shape assertion alone would
        not catch a silent second execution of a granted operation."""
        calls = 0

        def _lookup(city: str) -> str:
            nonlocal calls
            calls += 1
            return f"{city}: sunny"

        capability = CompiledPythonCapability(
            factory_ref="tests:factory",
            factory=lambda context: FunctionToolset([_lookup], max_retries=0),
        )
        plan = msgspec.structs.replace(make_plan(inference=_WHOLE), capabilities=(capability,))
        engine = build_engine(plan, _tool_then_answer_model(encode(_ANSWER)), deps=_CallerDeps())

        async with engine.run_stream("weather?", identity=_IDENTITY) as events:
            seen = [event async for event in events]

        assert [type(event) for event in seen] == [ToolCallEvent, ToolResultEvent, FinalEvent]
        assert calls == 1


async def _terminal(engine: object) -> object:
    """The last event of a streamed run: what a supervised caller finally sees."""
    async with engine.run_stream("assess", identity=_IDENTITY) as events:  # type: ignore[attr-defined]
        return [event async for event in events][-1]
