"""The engine maps loom tool names to provider classes and registers them."""

from __future__ import annotations

from typing import cast

import pytest

from loom.ai.compiler._plan import CompiledNativeCapability
from loom.ai.declarative import PolicySpec
from loom.ai.engines.pydantic_ai._capabilities import build_capabilities, build_toolsets
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.engines.pydantic_ai._native import TOOL_CLASSES, supported_native_tools
from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.ai.registry import engine_native_tool_support
from loom.core.di import LoomContainer


class _Plan:
    """Stand-in carrying only what the builder reads."""

    def __init__(self, *tools: str) -> None:
        self.name = "searcher"
        self.policies = PolicySpec()
        self.capabilities = tuple(CompiledNativeCapability(tool=tool) for tool in tools)


def _bedrock_admits_web_search() -> bool:
    """Whether the installed Bedrock class has gained provider-run web search."""
    from pydantic_ai.models.bedrock import BedrockConverseModel
    from pydantic_ai.native_tools import WebSearchTool

    return WebSearchTool in BedrockConverseModel.supported_native_tools()


_BEDROCK_ADMITS_WEB_SEARCH = _bedrock_admits_web_search()


def test_the_engines_map_covers_exactly_the_artifacts_names() -> None:
    """Every name the artifact may declare indexes a class, and no other does."""
    from loom.ai.declarative import NATIVE_TOOLS

    assert set(TOOL_CLASSES) == set(NATIVE_TOOLS)


def test_each_class_declares_the_same_name_that_indexes_it() -> None:
    """The engine's own identifier for each tool matches the loom name."""
    assert all(TOOL_CLASSES[name]().kind == name for name in TOOL_CLASSES)


@pytest.mark.skipif(
    _BEDROCK_ADMITS_WEB_SEARCH,
    reason="bedrock already admits web_search: review the table in docs/ai/artifacts.md",
)
def test_bedrock_does_not_admit_web_search_but_does_admit_code_execution() -> None:
    """The truth comes from the model class, not from a table in loom."""
    target = InferenceTarget(provider="bedrock", model="anthropic.claude-x", region="eu-west-1")

    admitted = supported_native_tools(target)

    assert "code_execution" in admitted
    assert "web_search" not in admitted


@pytest.mark.parametrize("provider", ["openai", "anthropic"])
def test_openai_and_anthropic_admit_web_search(provider: str) -> None:
    """Both vendor classes advertise web search at class level."""
    target = InferenceTarget(provider=provider, model="a-model")

    assert "web_search" in supported_native_tools(target)


def test_fails_naming_the_providers_for_an_unknown_vendor() -> None:
    """An unknown provider is refused before any tool is considered."""
    target = InferenceTarget(provider="unheard-of", model="x")

    with pytest.raises(AgentCompilationError) as failure:
        supported_native_tools(target)

    assert failure.value.issues[0].code is AgentErrorCode.PROVIDER_UNKNOWN


def test_builds_one_capability_per_grant_in_the_plans_order() -> None:
    """Order is the artifact's, and each grant becomes exactly one capability."""
    from pydantic_ai.native_tools import CodeExecutionTool, WebSearchTool

    # The plan doubles are structural, not subclasses.
    # Pre-existing: the AI test tree is not under pyright (task A7).
    built = build_capabilities(_Plan("web_search", "code_execution"), LoomContainer())  # pyright: ignore[reportArgumentType]

    assert [type(capability.tool) for capability in built] == [  # pyright: ignore[reportAttributeAccessIssue]
        WebSearchTool,
        CodeExecutionTool,
    ]


def test_builds_nothing_when_the_plan_grants_none() -> None:
    """A plan without native grants leaves the engine call untouched."""
    assert build_capabilities(_Plan(), LoomContainer()) == ()  # pyright: ignore[reportArgumentType]


def test_the_provider_exposes_the_oracle_the_registry_reads() -> None:
    """The bootstrap finds the oracle through the documented handshake."""
    from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider

    support = engine_native_tool_support(PydanticAIEngineProvider())

    assert support is not None
    assert "web_search" in support(InferenceTarget(provider="anthropic", model="a-model"))


def test_the_registry_returns_none_when_the_engine_does_not_provide_it() -> None:
    """An engine that serves no native grant declares no oracle."""

    class _Engine:
        pass

    assert engine_native_tool_support(_Engine()) is None


def test_the_native_grant_produces_no_toolset_and_arrives_as_a_capability() -> None:
    """A native grant is served as an engine capability, never as a toolset."""

    class _NativePlan:
        name = "searcher"
        policies = PolicySpec()
        capabilities = (CompiledNativeCapability(tool="web_search"),)

    plan = _NativePlan()

    assert build_toolsets(plan, LoomContainer(), mcp=SharedMcpToolsets()) == ()  # pyright: ignore[reportArgumentType]
    assert len(build_capabilities(plan, LoomContainer())) == 1  # pyright: ignore[reportArgumentType]


def test_the_registry_retires_native_when_the_engine_provides_no_oracle(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An engine that cannot check a grant does not get to serve the kind."""
    import logging

    from loom.ai.registry import engine_supported_kinds

    class _Engine:
        def supported_capability_kinds(self) -> frozenset[str]:
            return frozenset({"sql", "native"})

    with caplog.at_level(logging.WARNING, logger="loom.ai.registry"):
        kinds = engine_supported_kinds(_Engine(), "third-party")

    assert kinds == frozenset({"sql"})
    assert "third-party" in caplog.text


def test_the_registry_keeps_native_when_the_engine_provides_it() -> None:
    """The real engine keeps the kind, because it supplies the oracle."""
    from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
    from loom.ai.registry import engine_supported_kinds

    assert "native" in engine_supported_kinds(PydanticAIEngineProvider(), "pydantic-ai")


def test_the_announced_kinds_are_exactly_those_of_the_table() -> None:
    """The adapter cannot announce a kind it has no builder for, or hide one it has."""
    from loom.ai.engines.pydantic_ai import PydanticAIEngineProvider
    from loom.ai.engines.pydantic_ai._capabilities import _KINDS

    announced = PydanticAIEngineProvider().supported_capability_kinds()

    assert announced == frozenset(compiled.kind for compiled in _KINDS)


def test_fails_naming_the_kind_when_a_grant_has_no_builder() -> None:
    """A compiled grant of an unserved kind is refused, not silently dropped."""
    from loom.ai.compiler._plan import CompiledCapability
    from loom.ai.engines.pydantic_ai._capabilities import build_capabilities
    from loom.ai.errors import AgentCompilationError

    class _Unserved:
        kind = "telepathy"

    class _PlanWithUnserved:
        name = "searcher"
        policies = PolicySpec()
        capabilities = (cast("CompiledCapability", _Unserved()),)

    plan = _PlanWithUnserved()
    container = LoomContainer()

    with pytest.raises(AgentCompilationError, match="telepathy") as failure:
        build_capabilities(plan, container)  # type: ignore[arg-type]

    assert [issue.code for issue in failure.value.issues] == [
        AgentErrorCode.CAPABILITY_KIND_NO_BUILDER
    ]
