"""Scripted pydantic-ai engines, shared by the contract, unit and integration tests.

Every engine built here talks to a ``FunctionModel``: no network, no
credentials, no tokens. The plan is a real compiled plan and the engine is the
real adapter — only the model object is scripted, which is the seam
:class:`~loom.ai.engines.pydantic_ai.PydanticAIEnginePlugin` exposes for
exactly this purpose.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import msgspec
from pydantic_ai import RunContext
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import (
    ModelMessage,
    ModelResponse,
    ModelResponseStreamEvent,
    TextPart,
    ToolCallPart,
)
from pydantic_ai.models import Model, ModelRequestParameters, StreamedResponse
from pydantic_ai.models.function import AgentInfo, DeltaToolCall, DeltaToolCalls, FunctionModel
from pydantic_ai.settings import ModelSettings
from pydantic_ai.usage import RequestUsage

from loom.ai.abc import AgentEngine, DepsFactory, OutputCheck, StateShape
from loom.ai.compiler._plan import AgentPlan, CompiledInstruction, CompiledOutput
from loom.ai.compiler.phases._instructions import compile_instructions
from loom.ai.compiler.phases._output import compile_output
from loom.ai.declarative import JsonSchemaOutput, PolicySpec, TypeRefOutput
from loom.ai.engines.pydantic_ai import PydanticAIEnginePlugin
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity

OPEN_OBJECT_SCHEMA: Mapping[str, Any] = {"type": "object"}
"""Schema of an object with no declared properties: decodes to a ``dict``."""

_SCRIPTED_NAME = "scripted"
"""Model, provider and system name of :class:`ScriptedUsageModel`."""

STRICT_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
}
"""Schema whose compiled decoder rejects an unknown field (invariant 5)."""


class NullDeps:
    """Dependency factory the contract plans need: no service, no state."""

    def build(
        self,
        identity: Identity,
        container: LoomContainer,
        state: Mapping[str, Any] | None = None,
    ) -> object:
        """Return the empty dependency bundle.

        Args:
            identity: Verified caller of this invocation.
            container: Application container.

        Returns:
            ``None``: a pure-language agent depends on nothing.
        """
        return None


def compiled_output(schema: Mapping[str, Any]) -> CompiledOutput:
    """Compile ``schema`` through the real output phase."""
    output, issues = compile_output(JsonSchemaOutput(schema=schema), "contract")
    assert output is not None, issues
    return output


def compiled_instructions(text: str) -> tuple[CompiledInstruction, ...]:
    """Compile a literal string through the real instructions phase."""
    instructions, issues = compile_instructions(text, None, "contract")
    assert not issues, issues
    return instructions


def make_plan(
    *,
    schema: Mapping[str, Any] = OPEN_OBJECT_SCHEMA,
    retries: int = 0,
    policies: PolicySpec | None = None,
    inference: InferenceTarget | None = None,
    state: StateShape | None = None,
    output_check: OutputCheck | None = None,
) -> AgentPlan:
    """Build a compiled plan for a pure-language agent.

    ``policies`` overrides ``retries`` when given, so a test declaring a spend
    cap does not also have to restate the retry count. ``inference`` defaults
    to a binding ``genai-prices`` can price; override it when a test cares
    about what the *build-time* probe (``_limits.py:warn_if_model_not_priceable``)
    logs for a given model or provider name — it no longer refuses to boot,
    so the override is not load-bearing for building the engine, only for
    inspecting the notice. ``state`` declares the artefact's state shape;
    absent, this plan declares none, matching ``NullDeps``' own "no state".
    ``output_check`` is carried unresolved from the artefact's own field name,
    matching how the compiler resolves it (``compile_output_check``); absent,
    this plan declares no check.
    """
    return AgentPlan(
        name="contract",
        description="contract agent",
        instructions=compiled_instructions("answer the question"),
        state=state,
        spec_version=1,
        inference=inference or InferenceTarget(provider="openai", model="gpt-5.2"),
        output=compiled_output(schema),
        output_check=output_check,
        policies=policies if policies is not None else PolicySpec(retries=retries),
        metadata={},
    )


def plan_with_pydantic_type_ref_output(
    ref: str,
    *,
    mode: str | None = "tool",
    retries: int = 0,
    output_check: OutputCheck | None = None,
) -> AgentPlan:
    """A plan whose output is a compiled pydantic ``type_ref`` (D7, FR-012).

    ``output_mode`` is pinned to *mode* (default ``"tool"``) so a caller can
    pass the plan straight to
    :func:`~loom.ai.engines.pydantic_ai._spec.build_output_type`.
    """
    output, issues = compile_output(TypeRefOutput(ref=ref), "contract")
    assert output is not None, issues
    plan = make_plan(schema=STRICT_SCHEMA, retries=retries, output_check=output_check)
    plan = msgspec.structs.replace(plan, output=output)
    inference = msgspec.structs.replace(plan.inference, output_mode=mode)
    return msgspec.structs.replace(plan, inference=inference)


_PROSE: str = "a plain sentence with no declared shape at all"
"""Text every prose-only scripted model answers, with no declared shape."""


def tool_model(payloads: list[bytes], seen: list[list[ModelMessage]]) -> Model:
    """A model answering one payload per call, by an output-tool call."""
    calls = {"n": -1}

    def _payload() -> bytes:
        calls["n"] += 1
        return payloads[min(calls["n"], len(payloads) - 1)]

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        seen.append(list(messages))
        tool = info.output_tools[0].name
        return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=_payload().decode())])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        seen.append(list(messages))
        tool = info.output_tools[0].name
        yield {0: DeltaToolCall(name=tool, json_args=_payload().decode(), tool_call_id="call")}

    return FunctionModel(respond, stream_function=stream)


def native_text_model(payloads: list[bytes]) -> Model:
    """A model answering one payload per call, as a text part (native output)."""
    calls = {"n": -1}

    def _payload() -> str:
        calls["n"] += 1
        return payloads[min(calls["n"], len(payloads) - 1)].decode()

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del messages, info
        return ModelResponse(parts=[TextPart(content=_payload())])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        del messages, info
        yield _payload()

    return FunctionModel(respond, stream_function=stream)


def prose_model() -> Model:
    """A model that answers free text, never a structured tool call.

    Free prose has no JSON shape at all, so it can only be served by a
    per-run ``output_type=str`` override — the strongest possible witness
    that a call is running unshaped.
    """

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del messages, info
        return ModelResponse(parts=[TextPart(content=_PROSE)])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        del messages, info
        yield _PROSE

    return FunctionModel(respond, stream_function=stream)


def answering_model(payload: bytes) -> Model:
    """A model that answers ``payload`` verbatim, in both run modes."""
    text = payload.decode()

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        tool = info.output_tools[0].name
        return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=text)])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        tool = info.output_tools[0].name
        yield {0: DeltaToolCall(name=tool, json_args=text, tool_call_id="contract-call")}

    return FunctionModel(respond, stream_function=stream)


def recording_model(payload: bytes, seen: list[list[ModelMessage]]) -> Model:
    """A model answering like :func:`answering_model` that records what it is sent.

    Args:
        payload: JSON arguments of the output-tool call the model answers.
        seen: Receives a copy of the messages of every request, in order.
    """
    return _answering(payload, seen, failures=0)


def flaky_model(
    failures: int, payload: bytes, *, seen: list[list[ModelMessage]] | None = None
) -> Model:
    """A model failing with HTTP 503 ``failures`` times, then answering ``payload``.

    Both run modes share one counter, so a run that falls back from one mode
    to the other still sees the scripted number of outages.

    Args:
        failures: Requests that raise before the first that answers.
        payload: JSON arguments of the output-tool call the model answers.
        seen: When given, receives a copy of the messages of every request,
            failing ones included.
    """
    return _answering(payload, seen if seen is not None else [], failures=failures)


def _answering(payload: bytes, seen: list[list[ModelMessage]], *, failures: int) -> Model:
    text = payload.decode()
    outages = iter(range(failures))

    def observe(messages: list[ModelMessage]) -> None:
        seen.append(list(messages))
        if next(outages, None) is not None:
            raise ModelHTTPError(status_code=503, model_name="scripted", body=None)

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        observe(messages)
        tool = info.output_tools[0].name
        return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=text)])

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        observe(messages)
        tool = info.output_tools[0].name
        yield {0: DeltaToolCall(name=tool, json_args=text, tool_call_id="contract-call")}

    return FunctionModel(respond, stream_function=stream)


def failing_model(failure: Callable[[], Exception]) -> Model:
    """A model that raises the scripted failure, in both run modes."""

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        raise failure()

    async def stream(
        messages: list[ModelMessage], info: AgentInfo
    ) -> AsyncIterator[DeltaToolCalls]:
        raise failure()
        yield {}  # pragma: no cover - unreachable, makes the function a generator

    return FunctionModel(respond, stream_function=stream)


@dataclass
class _ScriptedUsageStream(StreamedResponse):
    """Streamed output-tool call whose usage is scripted, not estimated.

    ``FunctionModel`` estimates the usage of a streamed run from the deltas it
    saw, so it cannot express a run that reports a cost, a cache split or a
    counter the engine never declared. This response reports exactly what the
    test scripted, which is what makes the streaming and non-streaming usage
    of one run comparable.
    """

    _payload: str = ""
    _scripted: RequestUsage = field(default_factory=RequestUsage)
    _timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    _model_name: str = _SCRIPTED_NAME

    def __post_init__(self) -> None:
        self._usage = self._scripted

    async def _get_event_iterator(self) -> AsyncIterator[ModelResponseStreamEvent]:
        tool = self.model_request_parameters.output_tools[0].name
        event = self._parts_manager.handle_tool_call_delta(
            vendor_part_id=0, tool_name=tool, args=self._payload, tool_call_id="scripted-call"
        )
        if event is not None:
            yield event

    @property
    def model_name(self) -> str:
        """Return the scripted model name."""
        return self._model_name

    @property
    def provider_name(self) -> str:
        """Return the scripted provider name."""
        return _SCRIPTED_NAME

    @property
    def provider_url(self) -> str:
        """Return a URL no test ever reaches."""
        return "https://scripted.invalid"

    @property
    def timestamp(self) -> datetime:
        """Return when this response was built."""
        return self._timestamp


class ScriptedUsageModel(Model):
    """A model answering the same payload and the same usage in both run modes.

    The pricing path is deliberately not exercised: this model reports a cost
    where a real provider has one computed by genai-prices, so a test built on
    it proves that a reported cost is propagated, never that a given model is
    priced correctly. ``tool_calls`` cannot be scripted here at all — it is a
    run-level counter the engine increments itself.

    Args:
        payload: JSON arguments of the output-tool call, as bytes on the wire.
        usage: Accounting reported for the single request of the run.
        model_name: Identifier the build-time pricing notice would see, and
            also stamped onto every response this model returns, in both run
            modes — a real provider always names itself on its own response,
            and the engine's own unpriced-response count
            (``_engine.py:_count_unpriced_responses``) now reads that field
            to tell a provider response from a capability's synthetic one.
            Defaults to ``"scripted"``, unpriceable by ``genai-prices``; a
            test asserting on that notice's text overrides it. It has no
            bearing on whether the engine builds — the notice never blocks a
            build — only on whether it fires.
    """

    def __init__(
        self, payload: bytes, usage: RequestUsage, *, model_name: str = _SCRIPTED_NAME
    ) -> None:
        super().__init__()
        self._payload = payload.decode()
        self._scripted = usage
        self._model_name = model_name

    @property
    def model_name(self) -> str:
        """Return the scripted model name."""
        return self._model_name

    @property
    def system(self) -> str:
        """Return the scripted provider system."""
        return _SCRIPTED_NAME

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        """Answer the payload as an output-tool call carrying the scripted usage."""
        del messages, model_settings
        tool = model_request_parameters.output_tools[0].name
        return ModelResponse(
            parts=[ToolCallPart(tool_name=tool, args=self._payload)],
            usage=self._scripted,
            model_name=self._model_name,
        )

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncIterator[StreamedResponse]:
        """Stream the same answer and the same usage the request mode reports."""
        del messages, model_settings, run_context
        yield _ScriptedUsageStream(
            model_request_parameters=model_request_parameters,
            _payload=self._payload,
            _scripted=self._scripted,
            _model_name=self._model_name,
        )


def build_engine(plan: AgentPlan, model: Model, *, deps: DepsFactory | None = None) -> AgentEngine:
    """Build the real adapter over a scripted model.

    Args:
        plan: Compiled plan the engine serves.
        model: Scripted model the engine talks to.
        deps: Dependency factory the engine builds its bundle from; defaults
            to :class:`NullDeps` — no service, no state — matching every
            caller that does not care what the bundle carries.
    """
    provider = PydanticAIEnginePlugin(model_resolver=lambda target: model)
    return provider.create_engine(
        plan, deps=deps if deps is not None else NullDeps(), container=LoomContainer()
    )


def encode(payload: Mapping[str, Any]) -> bytes:
    """Encode a scripted answer; the test writes bytes, the engine reads them."""
    return msgspec.json.encode(dict(payload))
