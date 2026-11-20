"""One definition, three vendors, one config edit (US2 · SC-001, SC-002).

The same authored artifact is compiled against three deployment
configurations that differ only in ``ai.models.default``. The resulting plans
must differ **only** in ``inference``: everything the artifact declares —
instructions, output contract, policies — is vendor-free by construction
(FR-002, FR-017).

The second half pins FR-019a: one resolved provider serves the agent, and an
exhausted one fails the run instead of being re-routed to another vendor.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from loom.ai.compiler import AgentCompiler, AgentPlan
from loom.ai.config import AiConfig
from loom.ai.declarative import AgentSpecV1, JsonSchemaOutput, PolicySpec
from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
from loom.ai.errors import AgentRunErrorCode
from loom.ai.execution import AgentRunError
from loom.ai.inference import InferenceTarget
from loom.core.di import LoomContainer
from loom.core.identity import Identity
from loom.core.use_case.registry import UseCaseRegistry
from tests.helpers.pydantic_ai_engine import NullDeps, failing_model, make_plan

_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
}

_TARGETS: Mapping[str, InferenceTarget] = {
    "bedrock": InferenceTarget(
        provider="bedrock",
        model="anthropic.claude-sonnet-4-5-20250929-v1:0",
        region="eu-west-1",
    ),
    "openai": InferenceTarget(provider="openai", model="gpt-5.2"),
    "anthropic": InferenceTarget(provider="anthropic", model="claude-sonnet-4-5"),
}


def _spec() -> AgentSpecV1:
    """The one artifact, authored without any vendor vocabulary."""
    return AgentSpecV1(
        spec_version=1,
        name="analyst",
        description="answers questions about the reporting data",
        instructions="answer briefly and cite nothing you were not given",
        output=JsonSchemaOutput(schema=_SCHEMA),
        policies=PolicySpec(retries=1),
        metadata={"owner": "data-platform"},
    )


def _config(target: InferenceTarget) -> AiConfig:
    return AiConfig(
        engine="pydantic-ai", specs=("ai/agents/*/agent.yaml",), models={"default": target}
    )


def _compile(target: InferenceTarget) -> AgentPlan:
    compiler = AgentCompiler(
        config=_config(target),
        registry=UseCaseRegistry({}, {}),
        supported_kinds=PydanticAIEngineProvider().supported_capability_kinds(),
    )
    return compiler.compile(_spec(), source_path="agents/analyst.agent.yaml")


def _loom_type_shape(plan: AgentPlan) -> tuple[str, object]:
    """Identity of the built boundary type: its target type's name and fields.

    The boundary type is a compile-time artifact, so two compiles produce two
    distinct generated types; what must match is the shape they decode into.
    """
    target = plan.output.loom_type.type
    return (getattr(target, "__name__", str(target)), getattr(target, "__struct_fields__", None))


def _vendor_free_fields(plan: AgentPlan) -> Mapping[str, Any]:
    """Every plan field except the model binding, comparable across compiles."""
    fields = {
        name: getattr(plan, name)
        for name in plan.__struct_fields__
        if name not in {"inference", "output"}
    }
    fields["output.schema"] = dict(plan.output.schema)
    fields["output.loom_type"] = _loom_type_shape(plan)
    return fields


class TestProviderPortability:
    def test_the_plans_differ_only_in_inference_when_the_provider_changes(self) -> None:
        """SC-001: switching vendor is a configuration edit, nothing else."""
        plans = {name: _compile(target) for name, target in _TARGETS.items()}

        reference = _vendor_free_fields(plans["bedrock"])
        for name, plan in plans.items():
            assert _vendor_free_fields(plan) == reference, f"{name} plan diverges beyond inference"

    def test_each_plan_carries_its_own_binding_when_the_provider_changes(self) -> None:
        """SC-002: the binding — and only it — carries the vendor facts."""
        plans = {name: _compile(target) for name, target in _TARGETS.items()}

        assert plans["bedrock"].inference.provider == "bedrock"
        assert plans["bedrock"].inference.region == "eu-west-1"
        assert plans["openai"].inference.model == "gpt-5.2"
        assert plans["anthropic"].inference.provider == "anthropic"

    def test_the_artifact_names_no_provider_when_it_compiles(self) -> None:
        """FR-002: no vendor string reaches the artifact, in any field."""
        encoded = str(_spec()).lower()

        for vendor in ("bedrock", "openai", "anthropic", "gpt-", "claude-"):
            assert vendor not in encoded


class TestNoProviderFallback:
    async def test_the_run_fails_and_is_not_rerouted_when_the_provider_is_exhausted(self) -> None:
        """FR-019a: an exhausted provider fails the run; no vendor takeover."""
        resolved: list[InferenceTarget] = []
        plan = make_plan(retries=1)

        def resolver(target: InferenceTarget) -> Any:
            resolved.append(target)
            return failing_model(lambda: _rate_limited())

        provider = PydanticAIEngineProvider(model_resolver=resolver)
        engine = provider.create_engine(plan, deps=NullDeps(), container=LoomContainer())

        identity = Identity(subject="caller")

        with pytest.raises(AgentRunError) as failure:
            await engine.run("question", identity=identity)

        assert failure.value.code is AgentRunErrorCode.PROVIDER_RATE_LIMITED
        assert resolved == [plan.inference], "the model is bound once, to one provider"


def _rate_limited() -> Exception:
    from pydantic_ai.exceptions import ModelHTTPError

    return ModelHTTPError(status_code=429, model_name="scripted", body=None)


class TestRuntimeWiring:
    async def test_the_engine_is_built_once_per_plan_when_the_runtime_starts(
        self,
    ) -> None:
        """``create_engine`` runs once per plan, in ``__aenter__`` (FR-026)."""
        from loom.ai.config import AiConfig as _AiConfig
        from loom.ai.execution import AgentRuntime
        from tests.helpers.pydantic_ai_engine import answering_model, encode

        builds: list[str] = []
        plan = make_plan(schema=_SCHEMA)

        class CountingProvider(PydanticAIEngineProvider):
            def create_engine(self, plan: object, *, deps: Any, container: Any) -> Any:
                builds.append(getattr(plan, "name", "?"))
                return super().create_engine(plan, deps=deps, container=container)

        provider = CountingProvider(
            model_resolver=lambda target: answering_model(encode({"answer": "ok"}))
        )
        config = _AiConfig(
            engine="pydantic-ai",
            specs=("ai/agents/*/agent.yaml",),
            models={"default": _TARGETS["openai"]},
        )
        runtime = AgentRuntime(
            plans=[plan],
            config=config,
            engine_provider=provider,
            deps=NullDeps(),
            container=LoomContainer(),
        )

        async with runtime as live:
            first = await live.run("contract", "question", identity=Identity(subject="caller"))
            second = await live.run("contract", "question", identity=Identity(subject="caller"))

        assert builds == ["contract"], "one engine per plan, never per request"
        assert first.output.answer == "ok"  # type: ignore[attr-defined]
        assert second.output.answer == "ok"  # type: ignore[attr-defined]
