"""End-to-end coverage of an artifact's declared state and instruction blocks.

Every test here drives the real pipeline a deployment runs: an
:class:`~loom.ai.declarative.AgentSpecV1` decodes and compiles through
:class:`~loom.ai.compiler.AgentCompiler` into an
:class:`~loom.ai.compiler.AgentPlan`, and
:class:`~loom.ai.engines.pydantic_ai.PydanticAIEngineProvider` builds the real
``pydantic_ai.Agent`` that plan binds to. Only the model is a scripted
``FunctionModel`` (:func:`tests.helpers.pydantic_ai_engine.recording_model`) —
the one seam every engine test in this tree substitutes — so every assertion
below reads what the real engine actually built and actually sent.

The three declarable state spellings (FR-003) each get one artifact carrying
a literal block and a templated block: ``deps_type: <symbol>``
(:class:`TestSymbolState`), ``deps_schema`` (:class:`TestSchemaState`) and
``deps_type: dict`` (:class:`TestOpenState`). :class:`TestHostileTemplate`
is the security boundary the whole train exists for: a template trying every
attribute name on the dependency bundle, asserting none of them renders and
that the caller's subject reaches no instruction the model receives.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, cast

import msgspec
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic_ai import Agent, RunContext
from pydantic_ai.capabilities.abstract import AbstractCapability
from pydantic_ai.capabilities.instrumentation import Instrumentation
from pydantic_ai.messages import ModelRequest, ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.instrumented import InstrumentationSettings

from loom.ai.compiler import AgentCompiler, AgentPlan
from loom.ai.config import AiConfig
from loom.ai.declarative import AgentSpecV1, InstructionBlock, JsonSchemaOutput
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.engines.pydantic_ai._guards import capability_deps
from loom.ai.errors import AgentCompilationError, AgentErrorCode, AgentRunError, AgentRunErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.use_case.registry import UseCaseRegistry
from tests.helpers.pydantic_ai_engine import recording_model

_ANSWER_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
}
_ANSWER_PAYLOAD = msgspec.json.encode({"answer": "ok"})

_ALICE = Identity(subject="alice", roles=("analyst",), mechanism="test")


class AppraisalDeps(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """State shape the symbol-form artifacts declare through ``deps_type``."""

    marca: str
    km: int


_SCHEMA_DEPS: Mapping[str, Any] = {
    "type": "object",
    "properties": {"marca": {"type": "string"}, "km": {"type": "integer"}},
    "required": ["marca", "km"],
}

_HOSTILE_TEXT = "{{identity}} {{container}} {{invoker}} {{subject}} {{mechanism}}"


@dataclass(frozen=True)
class _Bundle:
    """A dependency bundle exposing every attribute name :class:`TestHostileTemplate` tries.

    Shaped like ``AgentDeps`` (``loom.composition._ai``): a verified
    caller, a container and a bound invoker, plus the caller's declared
    state. ``subject`` and ``mechanism`` are added on top of that shape so
    the hostile template has a marker for every plausible name an author
    might guess, not only the three the composition root actually carries.
    """

    identity: Identity
    container: LoomContainer
    invoker: str
    subject: str
    mechanism: str
    state: Mapping[str, Any] | None


class _RecordingDepsFactory:
    """Deps factory building a :class:`_Bundle` around one caller identity."""

    def build(
        self, identity: Identity, container: LoomContainer, state: Mapping[str, Any] | None = None
    ) -> object:
        """Return the bundle for one invocation, exposing every hostile marker."""
        return _Bundle(
            identity=identity,
            container=container,
            invoker="prod-invoker-token",
            subject=identity.subject,
            mechanism=identity.mechanism,
            state=state,
        )


def _compile(spec: AgentSpecV1) -> AgentPlan:
    """Compile *spec* through the real compiler, exactly as a deployment does."""
    compiler = AgentCompiler(
        config=AiConfig(
            engine="pydantic-ai",
            specs=("ai/agents/*/agent.yaml",),
            models={"default": InferenceTarget(provider="openai", model="gpt-5.2")},
        ),
        registry=UseCaseRegistry({}, {}),
        supported_kinds=PydanticAIEngineProvider().supported_capability_kinds(),
    )
    return compiler.compile(spec, source_path=f"agents/{spec.name}.agent.yaml")


def _engine_and_recorder(plan: AgentPlan) -> tuple[Any, list[list[Any]]]:
    """Build the real engine over a scripted model that records every request."""
    seen: list[list[Any]] = []
    model = recording_model(_ANSWER_PAYLOAD, seen)
    provider = PydanticAIEngineProvider(model_resolver=lambda target: model)
    engine = provider.create_engine(plan, deps=_RecordingDepsFactory(), container=LoomContainer())
    return engine, seen


def _instructions_sent(seen: list[list[Any]]) -> str:
    """Return the combined instructions text of the last recorded request."""
    request = seen[-1][0]
    assert isinstance(request, ModelRequest)
    return request.instructions or ""


def _output_spec(schema: Mapping[str, Any] = _ANSWER_SCHEMA) -> JsonSchemaOutput:
    return JsonSchemaOutput(schema=schema)


class TestSymbolState:
    """``deps_type: <symbol>`` — sugar over ``deps_schema`` (FR-003)."""

    def _spec(self, *, context_text: str, name: str = "appraiser-symbol") -> AgentSpecV1:
        return AgentSpecV1(
            spec_version=1,
            name=name,
            description="appraises a vehicle from its declared state",
            deps_type=f"{__name__}:AppraisalDeps",
            instructions=(
                InstructionBlock(text="Never invent a figure.", name="tone"),
                InstructionBlock(text=context_text, name="context", template="handlebars"),
            ),
            output=_output_spec(),
        )

    def test_a_misspelt_marker_fails_at_start_up_ac001(self) -> None:
        """AC-001: the issue names the artifact, the block and the marker with its path."""
        plan = _compile(self._spec(context_text="Appraising a {{marcaa}}, {{km}} km."))

        with pytest.raises(AgentCompilationError) as failure:
            _engine_and_recorder(plan)

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.TEMPLATE_COMPILATION_FAILED
        assert plan.name in issue.message
        assert "context" in issue.message
        assert "marcaa" in issue.message

    async def test_correct_markers_boot_and_send_the_supplied_values_ac002(self) -> None:
        """AC-002: a run supplying ``marca``/``km`` sends instructions containing both."""
        plan = _compile(self._spec(context_text="Appraising a {{marca}}, {{km}} km."))
        engine, seen = _engine_and_recorder(plan)

        await engine.run("assess", identity=_ALICE, state={"marca": "X", "km": 1})

        text = _instructions_sent(seen)
        assert "Never invent a figure." in text
        assert "Appraising a X, 1 km." in text


class TestSchemaState:
    """``deps_schema`` — the canonical spelling of the one mechanism (FR-003)."""

    def _spec(self, name: str = "appraiser-schema") -> AgentSpecV1:
        return AgentSpecV1(
            spec_version=1,
            name=name,
            description="appraises a vehicle from its declared state",
            deps_schema=_SCHEMA_DEPS,
            instructions=(
                InstructionBlock(text="Never invent a figure.", name="tone"),
                InstructionBlock(
                    text="Appraising a {{marca}}, {{km}} km.", name="context", template="handlebars"
                ),
            ),
            output=_output_spec(),
        )

    async def test_boots_and_renders_the_supplied_state(self) -> None:
        plan = _compile(self._spec())
        engine, seen = _engine_and_recorder(plan)

        await engine.run("assess", identity=_ALICE, state={"marca": "civic", "km": 42})

        text = _instructions_sent(seen)
        assert "Appraising a civic, 42 km." in text


class TestOpenState:
    """``deps_type: dict`` — the explicit waiver of validation (FR-006)."""

    def _spec(self, name: str = "appraiser-open") -> AgentSpecV1:
        return AgentSpecV1(
            spec_version=1,
            name=name,
            description="appraises a vehicle with no declared shape",
            deps_type="dict",
            instructions=(
                InstructionBlock(text="Never invent a figure.", name="tone"),
                InstructionBlock(
                    text="Appraising a {{marca}}, {{km}} km.", name="context", template="handlebars"
                ),
            ),
            output=_output_spec(),
        )

    async def test_boots_with_no_schema_check_and_renders_what_it_was_given(self) -> None:
        plan = _compile(self._spec())
        engine, seen = _engine_and_recorder(plan)

        await engine.run("assess", identity=_ALICE, state={"marca": "civic", "km": 42})

        text = _instructions_sent(seen)
        assert "Appraising a civic, 42 km." in text

    async def test_an_undeclared_marker_survives_the_build_and_renders_empty(self) -> None:
        """FR-033: nothing checks a template's markers under the open waiver."""
        plan = _compile(self._spec(name="appraiser-open-typo"))
        # Swap the context block for one with a marker the caller never sends.
        plan = msgspec.structs.replace(
            plan,
            instructions=(
                plan.instructions[0],
                msgspec.structs.replace(plan.instructions[1], text="Model: {{modelo}}."),
            ),
        )
        engine, seen = _engine_and_recorder(plan)

        await engine.run("assess", identity=_ALICE, state={"marca": "civic"})

        assert "Model: ." in _instructions_sent(seen)


class TestHostileTemplate:
    """AC-003: the security boundary the whole train exists to enforce.

    A template trying every attribute name on the dependency bundle —
    ``identity``, ``container``, ``invoker``, ``subject``, ``mechanism`` —
    must render none of them, under any declared state form, and the
    caller's subject must reach no instruction the model receives.
    """

    def _spec(self, *, deps_type: str | None, deps_schema: Mapping[str, Any] | None) -> AgentSpecV1:
        return AgentSpecV1(
            spec_version=1,
            name="hostile",
            description="tries to read the caller's credentials from a template",
            deps_type=deps_type,
            deps_schema=deps_schema,
            instructions=(
                InstructionBlock(text=_HOSTILE_TEXT, name="hostile", template="handlebars"),
            ),
            output=_output_spec(),
        )

    def test_fails_at_start_up_under_a_declared_symbol(self) -> None:
        plan = _compile(self._spec(deps_type=f"{__name__}:AppraisalDeps", deps_schema=None))

        with pytest.raises(AgentCompilationError) as failure:
            _engine_and_recorder(plan)

        message = failure.value.issues[0].message
        for marker in ("identity", "container", "invoker", "subject", "mechanism"):
            assert marker in message

    def test_fails_at_start_up_under_a_declared_schema(self) -> None:
        plan = _compile(self._spec(deps_type=None, deps_schema=_SCHEMA_DEPS))

        with pytest.raises(AgentCompilationError) as failure:
            _engine_and_recorder(plan)

        message = failure.value.issues[0].message
        for marker in ("identity", "container", "invoker", "subject", "mechanism"):
            assert marker in message

    async def test_renders_empty_under_the_open_waiver_and_never_leaks_the_subject(self) -> None:
        plan = _compile(self._spec(deps_type="dict", deps_schema=None))
        engine, seen = _engine_and_recorder(plan)

        await engine.run("assess", identity=_ALICE, state={})

        text = _instructions_sent(seen)
        assert text.strip() == ""
        assert _ALICE.subject not in text
        assert "prod-invoker-token" not in text


class TestLiteralNeverInfers:
    """AC-004: with no ``template:``, ``{{`` is literal text, never inferred (FR-022)."""

    async def test_double_braces_reach_the_model_verbatim(self) -> None:
        spec = AgentSpecV1(
            spec_version=1,
            name="literal-only",
            description="never renders anything",
            instructions=(InstructionBlock(text="Say {{marca}}.", name="tone"),),
            output=_output_spec(),
        )
        plan = _compile(spec)
        engine, seen = _engine_and_recorder(plan)

        await engine.run("assess", identity=_ALICE)

        assert "Say {{marca}}." in _instructions_sent(seen)


class TestCapabilityDepsUnaffectedByTheWrap:
    """AC-010: ``capability_deps`` behaves identically before and after the state wrap."""

    def test_a_bundle_carrying_identity_and_container_passes(self) -> None:
        bundle = _Bundle(
            identity=_ALICE,
            container=LoomContainer(),
            invoker="prod-invoker-token",
            subject=_ALICE.subject,
            mechanism=_ALICE.mechanism,
            state={"marca": "civic"},
        )

        deps = capability_deps(cast("RunContext[Any]", SimpleNamespace(deps=bundle)))

        assert deps.identity is _ALICE

    def test_a_bundle_carrying_neither_fails_closed(self) -> None:
        with pytest.raises(AgentRunError) as failure:
            capability_deps(cast("RunContext[Any]", SimpleNamespace(deps=object())))

        assert failure.value.code is AgentRunErrorCode.UNAUTHORIZED


class TestDescriptionNeverTemplates:
    """AC-014: ``description`` reaches the model and the span verbatim (FR-060, FR-061)."""

    async def test_a_marker_in_the_description_reaches_the_span_literally(self) -> None:
        """The span carries the literal text, never the caller's subject.

        Builds the agent the way ``create_engine`` does — the plan's
        instructions through the keyword, the plan's description through the
        keyword — and adds pydantic-ai's own ``Instrumentation`` capability
        directly, since wiring a tracer provider into a deployment is outside
        this spec's scope: what this test proves is what reaches the span
        once instrumentation is on, not that loom turns it on.
        """
        spec = AgentSpecV1(
            spec_version=1,
            name="describer",
            description="Agent for {{identity}}",
            instructions="be terse",
            output=_output_spec(),
        )
        plan = _compile(spec)
        assert type(plan.description) is str

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        def respond(messages: Any, info: Any) -> ModelResponse:
            del messages
            return ModelResponse(parts=[TextPart(content="ok")])

        settings = InstrumentationSettings(tracer_provider=provider)
        agent = Agent(
            FunctionModel(respond),
            deps_type=object,
            instructions="be terse",
            description=plan.description,
            capabilities=[Instrumentation(settings=settings)],
        )

        await agent.run("go", deps=_RecordingDepsFactory().build(_ALICE, LoomContainer()))

        spans = [
            span
            for span in exporter.get_finished_spans()
            if span.attributes is not None and "gen_ai.agent.description" in span.attributes
        ]
        assert spans, "no span carried the description attribute"
        attributes = spans[0].attributes
        assert attributes is not None
        description = attributes["gen_ai.agent.description"]
        assert description == "Agent for {{identity}}"
        assert _ALICE.subject not in str(description)


@dataclass
class _InstructionContributingCapability(AbstractCapability[Any]):
    """Minimal capability contributing its own instruction.

    Stands in for a real one (e.g. a toolset-backed capability) for
    :class:`TestOverrideDropsCapabilityInstructions`.
    """

    text: str

    def get_instructions(self) -> str:
        """Return the fixed instruction this capability contributes."""
        return self.text


class TestOverrideDropsCapabilityInstructions:
    """AC-015: pins R6 so the day OD-1 reaches for ``Agent.override``, the loss is known.

    ``Agent.override(instructions=...)`` replaces the whole instructions
    list, capability-contributed ones included, while toolset registrations
    survive untouched — they are collected separately during run execution.
    This test asserts today's engine behaviour; loom builds no selection
    mechanism on top of it (OD-1 is signed and unbuilt).
    """

    async def test_override_replaces_capability_instructions_but_keeps_the_toolset(self) -> None:
        seen_tools: list[tuple[str, ...]] = []
        seen: list[list[Any]] = []

        def respond(messages: Any, info: Any) -> ModelResponse:
            seen.append(list(messages))
            seen_tools.append(tuple(tool.name for tool in info.function_tools))
            if len(seen_tools) == 1:
                return ModelResponse(
                    parts=[ToolCallPart(tool_name="echo", args={"x": "y"}, tool_call_id="1")]
                )
            return ModelResponse(parts=[TextPart(content="ok")])

        agent: Agent[Any, str] = Agent(
            FunctionModel(respond),
            deps_type=object,
            instructions="base instruction",
            capabilities=[_InstructionContributingCapability(text="capability instruction")],
        )

        @agent.tool_plain
        def echo(x: str) -> str:
            return x

        with agent.override(instructions="override instruction"):
            await agent.run("go", deps=None)

        # The tool is still offered on every request the override covered.
        assert seen_tools == [("echo",), ("echo",)]
        # Every request carried the override text alone — the capability's
        # own contribution, and the agent's declared one, are both gone.
        for request in seen[-1]:
            if isinstance(request, ModelRequest):
                assert request.instructions == "override instruction"
