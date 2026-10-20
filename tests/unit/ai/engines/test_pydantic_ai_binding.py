"""``InferenceTarget`` → pydantic-ai model binding, and the spec translation.

No network: building a model object configures a client, it does not call one.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from msgspec import structs
from pydantic import TypeAdapter
from pydantic_ai import NativeOutput, ToolOutput
from pydantic_ai.models.bedrock import BedrockConverseModel
from pydantic_ai.models.test import TestModel
from pydantic_ai.models.typesafe import TypeSafeModel

from loom.ai.compiler._plan import AgentPlan, CompiledInstruction
from loom.ai.engines.pydantic_ai._models import SUPPORTED_PROVIDERS, resolve_model
from loom.ai.engines.pydantic_ai._spec import build_agent_spec, build_output_type
from loom.ai.engines.pydantic_ai.provider import PydanticAIEngineProvider
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.config import ConfigError
from loom.core.di import LoomContainer
from tests.helpers.pydantic_ai_engine import (
    STRICT_SCHEMA,
    NullDeps,
    make_plan,
    plan_with_pydantic_type_ref_output,
)


def _vendor_client(model: object) -> Any:
    """Return the vendor SDK client a resolved model wraps."""
    return cast(Any, model).client


def _typesafe_config(model: object) -> Any:
    """Return the configuration the TypeSafe SDK client was built with."""
    return _vendor_client(model)._config


@pytest.fixture
def aws_profile(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str:
    """A named AWS profile the Bedrock binding can actually resolve."""
    credentials = tmp_path / "credentials"
    credentials.write_text(
        "[reporting-profile]\naws_access_key_id = AKIAPROFILE\naws_secret_access_key = secret\n"
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(credentials))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "config"))
    return "reporting-profile"


class TestModelBinding:
    def test_bedrock_carries_region_and_credentials_when_resolved(self, aws_profile: str) -> None:
        """The Bedrock binding carries region and profile into the client."""
        target = InferenceTarget(
            provider="bedrock",
            model="anthropic.claude-sonnet-4-5-20250929-v1:0",
            region="eu-west-1",
            credentials_ref=aws_profile,
        )

        model = resolve_model(target)

        assert isinstance(model, BedrockConverseModel)
        assert model.model_name == target.model
        assert model.client.meta.region_name == "eu-west-1"
        assert model.client._request_signer._credentials.access_key == "AKIAPROFILE"

    @pytest.mark.parametrize(
        ("provider", "model_id"),
        [("openai", "gpt-5.2"), ("anthropic", "claude-sonnet-4-5")],
    )
    def test_the_model_carries_the_vendor_id_when_resolved(
        self, provider: str, model_id: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """OpenAI and Anthropic bind the vendor model id unchanged."""
        monkeypatch.setenv("VENDOR_API_KEY", "a-key")
        target = InferenceTarget(
            provider=provider, model=model_id, credentials_ref="VENDOR_API_KEY"
        )

        assert resolve_model(target).model_name == model_id

    @pytest.mark.parametrize("provider", ["openai", "anthropic"])
    def test_reads_the_key_from_the_variable_credentials_ref_names(
        self, provider: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``credentials_ref`` names the variable holding the key, never the key."""
        monkeypatch.setenv("VENDOR_API_KEY", "the-real-key")
        target = InferenceTarget(
            provider=provider, model="a-model", credentials_ref="VENDOR_API_KEY"
        )

        model = resolve_model(target)

        assert _vendor_client(model).api_key == "the-real-key"

    def test_a_credentials_ref_naming_a_loom_variable_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``LOOM_*`` variables are loom's own; none holds a vendor key."""
        monkeypatch.setenv("LOOM_VENDOR_API_KEY", "the-real-key")
        target = InferenceTarget(
            provider="openai", model="a-model", credentials_ref="LOOM_VENDOR_API_KEY"
        )

        with pytest.raises(ConfigError, match="LOOM_VENDOR_API_KEY") as excinfo:
            resolve_model(target)

        assert "the-real-key" not in str(excinfo.value)

    def test_typesafe_carries_the_key_and_the_endpoint_when_resolved(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The TypeSafe binding follows the API-key convention; ``endpoint`` is its base URL."""
        monkeypatch.setenv("VENDOR_API_KEY", "the-real-key")
        target = InferenceTarget(
            provider="typesafe",
            model="jev-latest",
            endpoint="https://gateway.example.com/v1",
            credentials_ref="VENDOR_API_KEY",
        )

        model = resolve_model(target)

        assert isinstance(model, TypeSafeModel)
        assert model.model_name == "jev-latest"
        assert _typesafe_config(model).api_key == "the-real-key"
        assert _typesafe_config(model).base_url == "https://gateway.example.com/v1"

    def test_typesafe_reads_the_sdks_default_variable_when_there_is_no_credentials_ref(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without a reference the TypeSafe SDK reads ``TYPESAFE_API_KEY`` itself."""
        monkeypatch.setenv("TYPESAFE_API_KEY", "sdk-default")
        target = InferenceTarget(provider="typesafe", model="jev-latest")

        assert _typesafe_config(resolve_model(target)).api_key == "sdk-default"

    def test_typesafe_refuses_a_native_output_pin_at_start_up(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Jev fills an output by tool only: a ``native`` pin fails before any request."""
        monkeypatch.setenv("TYPESAFE_API_KEY", "a-key")
        target = InferenceTarget(provider="typesafe", model="jev-latest", output_mode="native")

        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(target)

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.OUTPUT_MODE_UNSUPPORTED
        assert "native" in issue.message
        assert "tool" in issue.message
        assert issue.field == "output_mode"

    @pytest.mark.parametrize("output_mode", ["tool", None])
    def test_typesafe_serves_a_tool_pin_and_an_unpinned_binding(
        self, output_mode: str | None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``tool`` is what pydantic-ai fills a TypeSafe output with, so both pins resolve."""
        monkeypatch.setenv("TYPESAFE_API_KEY", "a-key")
        target = InferenceTarget(provider="typesafe", model="jev-latest", output_mode=output_mode)

        assert isinstance(resolve_model(target), TypeSafeModel)

    def test_reads_the_sdks_default_variable_when_there_is_no_credentials_ref(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without a reference the vendor SDK reads its own variable."""
        monkeypatch.setenv("OPENAI_API_KEY", "sdk-default")
        target = InferenceTarget(provider="openai", model="a-model")

        assert _vendor_client(resolve_model(target)).api_key == "sdk-default"

    @pytest.mark.parametrize("provider", ["openai", "anthropic", "gateway", "typesafe"])
    def test_fails_naming_the_variable_when_it_is_unset(
        self, provider: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unset variable is a start-up failure, not a 401 on the first call."""
        monkeypatch.delenv("VENDOR_API_KEY", raising=False)
        target = InferenceTarget(
            provider=provider,
            model="a-model",
            endpoint="https://gateway.example.com/v1" if provider == "gateway" else None,
            credentials_ref="VENDOR_API_KEY",
        )

        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(target)

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.PROVIDER_SETTING_MISSING
        assert "VENDOR_API_KEY" in issue.message
        assert provider in issue.message

    def test_fails_naming_the_providers_for_an_unknown_vendor(self) -> None:
        """An unknown provider dies at start-up, naming what this release binds."""
        target = InferenceTarget(provider="unheard-of", model="x")

        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(target)

        issue = failure.value.issues[0]
        assert issue.code is AgentErrorCode.PROVIDER_UNKNOWN
        assert "openai" in issue.message
        assert "extra" not in issue.message
        assert "not installed" not in issue.message

    def test_fails_asking_for_the_region_when_bedrock_lacks_it(self) -> None:
        """A Bedrock binding without a region fails before any request."""
        target = InferenceTarget(provider="bedrock", model="x")

        with pytest.raises(AgentCompilationError) as failure:
            resolve_model(target)

        assert failure.value.issues[0].code is AgentErrorCode.PROVIDER_SETTING_MISSING

    def test_the_supported_providers_are_the_documented_ones(self) -> None:
        """The dispatch map is the whole vendor surface of this release."""
        assert (
            frozenset({"bedrock", "openai", "anthropic", "gateway", "typesafe"})
            == SUPPORTED_PROVIDERS
        )


class TestSpecTranslation:
    def test_the_spec_carries_the_schema_and_the_policies_when_translated(self) -> None:
        """The plan's output schema and limits reach the engine's own spec."""
        plan: AgentPlan = make_plan(retries=3)

        spec = build_agent_spec(plan)

        assert spec.output_schema == dict(plan.output.schema)
        assert spec.retries == 3

    def test_the_spec_carries_no_tool_timeout_since_loom_already_applies_it(self) -> None:
        """``tool_timeout_ms`` has one enforcer, so the engine gets no deadline.

        Projecting it here as well raced loom's own ``asyncio.timeout`` over
        the same value: the engine's expiry classifies as
        ``PROVIDER_UNAVAILABLE`` and is retried, loom's raises ``TOOL_TIMEOUT``
        and is not. Which one fired was up to the event loop.
        """
        spec = build_agent_spec(make_plan())

        assert spec.tool_timeout is None

    def test_the_spec_carries_no_metadata_or_model_when_translated(self) -> None:
        """Ownership facts and the concrete model never travel in the spec."""
        spec = build_agent_spec(make_plan())

        assert spec.metadata is None
        assert spec.model is None

    def test_the_spec_carries_no_instructions_or_description(self) -> None:
        """Both fields travel as ``from_spec`` keywords, never through the spec (T403).

        A ``description`` or an instruction block's text containing ``{{``
        would otherwise pass through ``AgentSpec``'s own ``TemplateStr``
        validator and compile into a live template; leaving the spec fields
        unset is what keeps every block's text — and the description —
        literal at this layer.
        """
        plan = structs.replace(
            make_plan(),
            description="Agent for {{identity}}",
            instructions=(
                CompiledInstruction(text="First {{name}}.", name="opening"),
                CompiledInstruction(text="Second."),
            ),
        )

        spec = build_agent_spec(plan)

        assert spec.instructions is None
        assert spec.description is None


def _plan_with_output_mode(mode: str | None) -> AgentPlan:
    """A plan whose binding pins ``mode`` (or leaves it to the engine)."""
    plan = make_plan(schema=STRICT_SCHEMA)
    return structs.replace(plan, inference=structs.replace(plan.inference, output_mode=mode))


def _wrapped_schema(wrapped: Any) -> dict[str, Any]:
    """The schema pydantic derives from the marker's payload, sans decoration."""
    schema = TypeAdapter(wrapped).json_schema()
    schema.pop("title", None)
    schema.pop("description", None)
    return schema


class _FromSpecRecorder:
    """Stands in for ``Agent.from_spec`` and keeps the keywords it received.

    Also keeps the marker the provider built, because ``StructuredDict`` mints
    a new class per call: only identity proves the provider passed on the
    very object :func:`build_output_type` returned.
    """

    def __init__(self) -> None:
        self.kwargs: dict[str, Any] | None = None
        self.built: ToolOutput[Any] | NativeOutput[Any] | None = None

    def __call__(self, spec: object, **kwargs: Any) -> object:
        self.kwargs = kwargs
        return object()

    def build(self, plan: AgentPlan) -> ToolOutput[Any] | NativeOutput[Any] | None:
        self.built = build_output_type(plan)
        return self.built


@pytest.fixture
def from_spec(monkeypatch: pytest.MonkeyPatch) -> _FromSpecRecorder:
    """Patch ``Agent.from_spec`` and record the provider's marker, in its module."""
    recorder = _FromSpecRecorder()
    monkeypatch.setattr("loom.ai.engines.pydantic_ai.provider.Agent.from_spec", recorder)
    monkeypatch.setattr("loom.ai.engines.pydantic_ai.provider.build_output_type", recorder.build)
    return recorder


def _create_engine(plan: AgentPlan) -> None:
    provider = PydanticAIEngineProvider(model_resolver=lambda target: TestModel())
    provider.create_engine(plan, deps=NullDeps(), container=LoomContainer())


class TestOutputMode:
    def test_tool_wraps_the_schema_in_tool_output_and_reaches_from_spec(
        self, from_spec: _FromSpecRecorder
    ) -> None:
        """``tool`` pins the tool-call mode around the plan's own schema."""
        plan = _plan_with_output_mode("tool")

        _create_engine(plan)

        marker = from_spec.built
        assert marker is not None
        assert type(marker) is ToolOutput
        assert _wrapped_schema(marker.output) == dict(plan.output.schema)
        assert from_spec.kwargs is not None
        assert from_spec.kwargs["output_type"] is marker

    def test_native_wraps_the_schema_in_native_output_and_reaches_from_spec(
        self, from_spec: _FromSpecRecorder
    ) -> None:
        """``native`` pins the provider's structured-output mode, one type only."""
        plan = _plan_with_output_mode("native")

        _create_engine(plan)

        marker = from_spec.built
        assert marker is not None
        assert type(marker) is NativeOutput
        assert not isinstance(marker.outputs, list | tuple)
        assert _wrapped_schema(marker.outputs) == dict(plan.output.schema)
        assert from_spec.kwargs is not None
        assert from_spec.kwargs["output_type"] is marker

    def test_passes_no_output_type_when_the_binding_declares_no_mode(
        self, from_spec: _FromSpecRecorder
    ) -> None:
        """Absent mode: the call is today's call, no ``output_type`` keyword at all."""
        plan = _plan_with_output_mode(None)

        assert build_output_type(plan) is None
        _create_engine(plan)

        assert from_spec.kwargs is not None
        assert "output_type" not in from_spec.kwargs

    def test_fails_when_the_mode_is_neither_of_the_two(self) -> None:
        """Fail closed: an unhandled mode raises instead of degrading to ``native``.

        Reachable only past the config check (a plan built in process), which
        is exactly the path the defaulted dispatch used to serve silently.
        """
        plan = _plan_with_output_mode("prompted")

        with pytest.raises(AssertionError):
            build_output_type(plan)

    def test_the_marker_carries_no_name_or_description(self) -> None:
        """Mirrors the engine's own wrapping of ``output_schema``: bare marker."""
        tool = build_output_type(_plan_with_output_mode("tool"))
        native = build_output_type(_plan_with_output_mode("native"))

        assert tool is not None
        assert native is not None
        assert isinstance(tool, ToolOutput)
        assert isinstance(native, NativeOutput)
        assert (tool.name, tool.description) == (None, None)
        assert (native.name, native.description) == (None, None)


class TestPydanticTypeRefOutputReachesTheEngineSpec:
    """A pydantic ``type_ref`` output must survive the same two projections a

    msgspec one does: ``AgentSpec.output_schema`` (``build_agent_spec``) and
    the ``StructuredDict`` wrapping (``build_output_type``); R1 is pinned
    here too — the model's own class name must not leak as the output tool
    name through ``model_json_schema()``'s root ``title``.
    """

    def test_build_agent_spec_accepts_the_schema_of_a_pydantic_type_ref(
        self, fake_myapp_path: Path
    ) -> None:
        del fake_myapp_path
        plan = plan_with_pydantic_type_ref_output(
            "myapp.domain.pydantic_invoices:InvoiceSummaryModel"
        )

        spec = build_agent_spec(plan)

        assert spec.output_schema == dict(plan.output.schema)

    def test_build_output_type_hands_the_model_class_under_the_default_tool_name(
        self, fake_myapp_path: Path
    ) -> None:
        from myapp.domain.pydantic_invoices import InvoiceSummaryModel

        del fake_myapp_path
        plan = plan_with_pydantic_type_ref_output(
            "myapp.domain.pydantic_invoices:InvoiceSummaryModel"
        )

        marker = build_output_type(plan)

        assert "title" not in dict(plan.output.schema)
        assert type(marker) is ToolOutput
        assert marker.output is InvoiceSummaryModel
        assert marker.name is None


class TestInstructionsAndDescriptionKeywords:
    def test_from_spec_receives_both_as_keywords_never_through_the_spec(
        self, from_spec: _FromSpecRecorder
    ) -> None:
        """T403, AC-014: ``instructions=`` and ``description=`` reach ``from_spec`` directly.

        A description containing ``{{identity}}`` reaches ``from_spec`` as a
        plain ``str`` keyword: it never becomes a ``TemplateStr`` rendered
        against the dependency bundle (FR-061).
        """
        plan = structs.replace(
            make_plan(),
            description="Agent for {{identity}}",
            instructions=(CompiledInstruction(text="Be terse.", name="tone"),),
        )

        _create_engine(plan)

        assert from_spec.kwargs is not None
        assert from_spec.kwargs["description"] == "Agent for {{identity}}"
        assert isinstance(from_spec.kwargs["description"], str)
        assert len(from_spec.kwargs["instructions"]) == 1


class TestEntryPoint:
    def test_the_engine_resolves_by_entry_point_when_requested_by_name(self) -> None:
        """``ai.engine: pydantic-ai`` resolves, handshake included (FR-021)."""
        from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
        from loom.ai.registry import resolve_engine_provider

        provider = resolve_engine_provider("pydantic-ai")

        assert isinstance(provider, PydanticAIEngineProvider)
        assert PydanticAIEngineProvider.LOOM_AI_ENGINE_API == 2

    # The kinds this adapter declares are asserted by
    # ``tests/integration/ai/test_capabilities.py::TestSupportedCapabilityKinds``,
    # which supersedes the empty-set assertion that stood here while the
    # capability toolsets did not exist yet.
