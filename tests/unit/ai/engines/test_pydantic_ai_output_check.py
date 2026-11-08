"""``output_check`` (spec 011 L17 / spec 018): a declared predicate over the answer.

Drives the real engine adapter — :class:`~loom.ai.engines.pydantic_ai.PydanticAIEngineProvider`
and :class:`~loom.ai.engines.pydantic_ai._engine.PydanticAIEngine` — against a
``FunctionModel``, no network. Pins three claims:

* a rejection drives a real, bounded retry, and the correction text reaches
  the model, whether or not the agent holds a capability (spec 018 T804);
* a check's return value is never substituted for the answer — the engine
  decodes the model's own bytes, so a mapping the check built and returned
  would be discarded in silence (spec 011 L17, "Decision");
* under ``output_mode: native``, loom withholds an artefact's deltas until
  the answer passes the check, so a rejected attempt reaches no subscriber;
  an artefact with no check is unaffected (spec 018 T803);
* an artefact declaring ``output_check`` still serves a per-run shape
  override (``AgentHandle.run(expect=...)``, ``run_text``): pydantic-ai
  itself refuses a run ``output_type`` on an agent holding an output
  validator, so the override path runs on a second, unvalidated agent built
  alongside the checked one, never lazily (spec 018 regression).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from unittest.mock import patch

import msgspec
import pytest
from pydantic_ai import Agent
from pydantic_ai._agent_graph import GraphAgentState
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    RetryPromptPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.run import AgentRunResult
from pydantic_ai.toolsets import FunctionToolset

from loom.ai.abc import TextDeltaEvent
from loom.ai.compiler._plan import CompiledPythonCapability
from loom.ai.engines.pydantic_ai._engine import PydanticAIEngine
from loom.ai.engines.pydantic_ai._output import decode_output
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.identity import Identity
from tests.helpers.pydantic_ai_engine import (
    OPEN_OBJECT_SCHEMA,
    answering_model,
    build_engine,
    compiled_output,
    encode,
    make_plan,
    native_text_model,
    prose_model,
    tool_model,
)

_IDENTITY = Identity(subject="caller")

_BAD = encode({"answer": "bad"})
_GOOD = encode({"answer": "good"})
_PROSE = "a plain sentence with no declared shape at all"


def _reject_until_good(seen: list[Mapping[str, Any]]) -> Any:
    """Return an :data:`~loom.ai.abc.OutputCheck` that accepts only ``{"answer": "good"}``."""

    def check(answer: Mapping[str, Any]) -> str | None:
        seen.append(dict(answer))
        if answer.get("answer") != "good":
            return "the answer must be 'good'"
        return None

    return check


def _always_reject(seen: list[Mapping[str, Any]]) -> Any:
    def check(answer: Mapping[str, Any]) -> str | None:
        seen.append(dict(answer))
        return "never good enough"

    return check


def _python_capability() -> CompiledPythonCapability:
    """A minimal granted toolset, enough to make a plan capability-bearing."""

    def factory(*args: object, **kwargs: object) -> object:
        del args, kwargs
        return FunctionToolset()

    return CompiledPythonCapability(factory_ref="tests.contract:factory", factory=factory)


class TestOnRejectionTheEngineRetries:
    async def test_the_correction_text_reaches_the_model_and_the_answer_is_the_corrected_one(
        self,
    ) -> None:
        seen_by_check: list[Mapping[str, Any]] = []
        seen_by_model: list[list[ModelMessage]] = []
        plan = make_plan(retries=2, output_check=_reject_until_good(seen_by_check))
        engine = build_engine(plan, tool_model([_BAD, _GOOD], seen_by_model))

        result = await engine.run("hi", identity=_IDENTITY)

        assert result.output == {"answer": "good"}
        assert len(seen_by_check) == 2
        assert len(seen_by_model) == 2
        retried_request = seen_by_model[1][-1]
        # The engine carries the correction back as a retry prompt or, since
        # pydantic-ai 2.66, as the output tool's return; loom pins only that the
        # text reaches the model.
        carried = [
            part.content
            for part in retried_request.parts
            if isinstance(part, (RetryPromptPart, ToolReturnPart))
        ]
        assert any("answer must be 'good'" in str(content) for content in carried)

    async def test_the_retry_budget_is_bounded_by_retries(self) -> None:
        seen_by_check: list[Mapping[str, Any]] = []
        seen_by_model: list[list[ModelMessage]] = []
        plan = make_plan(retries=1, output_check=_always_reject(seen_by_check))
        engine = build_engine(plan, tool_model([_BAD], seen_by_model))

        with pytest.raises(AgentRunError) as excinfo:
            await engine.run("hi", identity=_IDENTITY)

        assert excinfo.value.code is AgentRunErrorCode.OUTPUT_SCHEMA_VIOLATION
        assert len(seen_by_model) == 2  # retries=1: the original attempt, plus one retry

    async def test_a_check_returning_none_accepts_the_models_answer_unchanged(self) -> None:
        seen_by_check: list[Mapping[str, Any]] = []
        plan = make_plan(output_check=_reject_until_good(seen_by_check))
        engine = build_engine(plan, answering_model(_GOOD))

        result = await engine.run("hi", identity=_IDENTITY)

        assert result.output == {"answer": "good"}
        assert seen_by_check == [{"answer": "good"}]

    async def test_a_capability_bearing_agent_still_retries_on_rejection(self) -> None:
        """The outer loop is not involved: the retry happens inside one engine run."""
        seen_by_check: list[Mapping[str, Any]] = []
        seen_by_model: list[list[ModelMessage]] = []
        plan = make_plan(retries=1, output_check=_reject_until_good(seen_by_check))
        plan = msgspec.structs.replace(plan, capabilities=(_python_capability(),))
        engine = build_engine(plan, tool_model([_BAD, _GOOD], seen_by_model))
        assert isinstance(engine, PydanticAIEngine)

        result = await engine.run("hi", identity=_IDENTITY)

        assert result.output == {"answer": "good"}
        assert len(seen_by_model) == 2


class TestTheCheckIsAPredicateNeverATransformer:
    def test_decode_output_ignores_what_the_engines_own_validator_returned(self) -> None:
        """The transformer fence (spec 018 T804).

        ``decode_output`` walks ``result.all_messages()``, never
        ``result.output``: a mismatched ``result.output`` — standing in for a
        validator that built and returned a different mapping — must not
        reach the answer.
        """
        output = compiled_output(OPEN_OBJECT_SCHEMA)
        raw_response = ModelResponse(parts=[ToolCallPart(tool_name="answer", args=_GOOD.decode())])
        result: AgentRunResult[Any] = AgentRunResult(
            output={"mutated": True},
            _state=GraphAgentState(message_history=[raw_response]),
        )

        decoded = decode_output(output, result)

        assert decoded == {"answer": "good"}
        assert decoded != result.output


class TestNativeModeWithholdsARejectedAttempt:
    async def test_a_rejected_attempt_reaches_no_subscriber(self) -> None:
        seen_by_check: list[Mapping[str, Any]] = []
        inference = InferenceTarget(provider="openai", model="gpt-5.2", output_mode="native")
        plan = make_plan(
            retries=1, output_check=_reject_until_good(seen_by_check), inference=inference
        )
        engine = build_engine(plan, native_text_model([_BAD, _GOOD]))

        async with engine.run_stream("hi", identity=_IDENTITY) as events:
            collected = [event async for event in events]

        deltas = [event.text for event in collected if isinstance(event, TextDeltaEvent)]
        assert deltas == [_GOOD.decode()]
        assert all("bad" not in text for text in deltas)

    async def test_an_artefact_with_no_check_is_unaffected(self) -> None:
        inference = InferenceTarget(provider="openai", model="gpt-5.2", output_mode="native")
        plan = make_plan(inference=inference)
        engine = build_engine(plan, native_text_model([_GOOD]))

        async with engine.run_stream("hi", identity=_IDENTITY) as events:
            collected = [event async for event in events]

        deltas = [event.text for event in collected if isinstance(event, TextDeltaEvent)]
        assert deltas == [_GOOD.decode()]


class TestToolModeStreamingIsUnaffectedByAnArtefactWithNoCheck:
    async def test_an_artefact_with_no_check_streams_exactly_as_before(self) -> None:
        inference = InferenceTarget(provider="openai", model="gpt-5.2", output_mode="tool")
        plan = make_plan(inference=inference)
        engine = build_engine(plan, answering_model(_GOOD))

        async with engine.run_stream("hi", identity=_IDENTITY) as events:
            collected = [event async for event in events]

        deltas = [event for event in collected if isinstance(event, TextDeltaEvent)]
        assert deltas == []
        final = collected[-1]
        assert final.output == {"answer": "good"}  # type: ignore[union-attr]


class TestARunLevelShapeOverrideSurvivesAnArtefactsOwnOutputCheck:
    """Regression: pydantic-ai refuses a run ``output_type`` on an agent that
    holds an output validator (``UserError: Cannot set a custom run
    'output_type' when the agent has output validators``). The engine's own
    ``AgentRuntime.run`` docstring already promises that an overridden shape
    skips the plan's own output check; this class pins that the *engine*
    itself never reaches pydantic-ai's refusal in the first place.
    """

    async def test_a_shape_override_serves_the_call_with_no_error_and_the_check_never_runs(
        self,
    ) -> None:
        seen_by_check: list[Mapping[str, Any]] = []
        plan = make_plan(retries=1, output_check=_always_reject(seen_by_check))
        engine = build_engine(plan, prose_model())
        assert isinstance(engine, PydanticAIEngine)

        async with engine.run_stream_shaped("hi", identity=_IDENTITY, output_type=str) as events:
            final = [event async for event in events][-1]

        assert final.output == _PROSE  # type: ignore[union-attr]
        assert seen_by_check == []

    async def test_the_same_artefact_still_applies_and_corrects_the_check_through_the_normal_route(
        self,
    ) -> None:
        seen_by_check: list[Mapping[str, Any]] = []
        seen_by_model: list[list[ModelMessage]] = []
        plan = make_plan(retries=1, output_check=_reject_until_good(seen_by_check))
        engine = build_engine(plan, tool_model([_BAD, _GOOD], seen_by_model))

        result = await engine.run("hi", identity=_IDENTITY)

        assert result.output == {"answer": "good"}
        assert len(seen_by_check) == 2
        assert len(seen_by_model) == 2


class TestAnArtefactWithNoOutputCheckBuildsOneAgent:
    """No hidden second construction when the plan declares no check at all."""

    def test_the_provider_builds_exactly_one_agent(self) -> None:
        plan = make_plan()

        with patch(
            "loom.ai.engines.pydantic_ai.provider.Agent.from_spec", wraps=Agent.from_spec
        ) as spy:
            build_engine(plan, answering_model(_GOOD))

        assert spy.call_count == 1
