"""A ``native`` grant is checked against the model bound to the agent's role."""

from __future__ import annotations

from collections.abc import Mapping

import pytest

from loom.ai.abc import NativeToolSupport
from loom.ai.compiler import CompiledCapability, CompiledNativeCapability
from loom.ai.compiler.phases._capabilities import compile_capabilities
from loom.ai.config import AgentEndpointConfig, AiConfig
from loom.ai.declarative import AgentSpecV1, JsonSchemaOutput, NativeCapability
from loom.ai.errors import AgentCompilationError, AgentCompilationIssue, AgentErrorCode
from loom.ai.inference import InferenceTarget
from loom.core.use_case.registry import UseCaseRegistry

_KINDS = frozenset({"native"})
_SCHEMA = {"type": "object", "additionalProperties": False, "properties": {}}


def _spec(*tools: str) -> AgentSpecV1:
    """An artifact granting one ``native`` capability per tool named."""
    return AgentSpecV1(
        spec_version=1,
        name="searcher",
        description="Answers with the provider's own tools.",
        instructions="Use the granted provider tools.",
        output=JsonSchemaOutput(schema=dict(_SCHEMA)),
        capabilities=tuple(NativeCapability(tool=tool) for tool in tools),
    )


def _compile(
    spec: AgentSpecV1,
    *,
    inference: InferenceTarget | None = None,
    native_tools: NativeToolSupport | None = None,
    endpoints: Mapping[str, AgentEndpointConfig] | None = None,
) -> tuple[tuple[CompiledCapability, ...], list[AgentCompilationIssue]]:
    """Compile the capabilities of *spec* with the given deployment inputs."""
    return compile_capabilities(
        spec,
        component="agents/searcher/agent.yaml",
        config=AiConfig(engine="pydantic-ai", specs=(), models={}, endpoints=endpoints or {}),
        registry=UseCaseRegistry.build([]),
        sql=None,
        supported_kinds=_KINDS,
        inference=inference,
        native_tools=native_tools,
    )


_BEDROCK = InferenceTarget(provider="bedrock", model="anthropic.claude-x", region="eu-west-1")


def test_compiles_when_the_model_admits_the_tool() -> None:
    """A tool the binding admits becomes a compiled grant."""
    compiled, issues = _compile(
        _spec("web_search"),
        inference=_BEDROCK,
        native_tools=lambda _t: frozenset({"web_search"}),
    )

    assert issues == []
    assert compiled == (CompiledNativeCapability(tool="web_search"),)


def test_fails_naming_provider_model_role_and_admitted_tools() -> None:
    """The message says what was asked for and what the binding does admit."""
    _compiled, issues = _compile(
        _spec("web_search"),
        inference=_BEDROCK,
        native_tools=lambda _t: frozenset({"code_execution"}),
    )

    assert len(issues) == 1
    issue = issues[0]
    assert issue.code is AgentErrorCode.NATIVE_TOOL_UNSUPPORTED
    assert issue.field == "capabilities.tool"
    for expected in ("bedrock", "anthropic.claude-x", "default", "web_search", "code_execution"):
        assert expected in issue.message


def test_fails_once_when_the_same_tool_is_granted_twice() -> None:
    """A tool granted twice is one issue, not two grants."""
    _compiled, issues = _compile(
        _spec("web_search", "web_search"),
        inference=_BEDROCK,
        native_tools=lambda _t: frozenset({"web_search"}),
    )

    assert [issue.code for issue in issues] == [AgentErrorCode.NATIVE_TOOL_DUPLICATE]


def test_adds_no_issue_when_the_role_is_unbound() -> None:
    """An unbound role is reported by role resolution, not twice."""
    compiled, issues = _compile(_spec("web_search"), inference=None, native_tools=None)

    assert issues == []
    assert compiled == ()


def test_propagates_the_oracles_issue_when_the_sdk_is_missing() -> None:
    """A provider SDK missing is the oracle's issue, not a traceback."""

    def _missing(_target: InferenceTarget) -> frozenset[str]:
        raise AgentCompilationError(
            [AgentErrorCode.PROVIDER_NOT_INSTALLED.issue(component="openai", extra="ai-openai")]
        )

    _compiled, issues = _compile(_spec("web_search"), inference=_BEDROCK, native_tools=_missing)

    assert [issue.code for issue in issues] == [AgentErrorCode.PROVIDER_NOT_INSTALLED]


def test_rejects_the_kind_when_the_engine_provides_no_oracle() -> None:
    """Without an oracle the grant cannot be checked, so it is refused."""
    _compiled, issues = _compile(_spec("web_search"), inference=_BEDROCK, native_tools=None)

    assert [issue.code for issue in issues] == [AgentErrorCode.CAPABILITY_KIND_UNSUPPORTED]


@pytest.mark.parametrize("tool", ["web_search", "web_fetch", "code_execution"])
def test_accepts_every_tool_in_the_vocabulary(tool: str) -> None:
    """Every name the artifact schema admits compiles."""
    compiled, issues = _compile(
        _spec(tool), inference=_BEDROCK, native_tools=lambda _t: frozenset({tool})
    )

    assert issues == []
    assert compiled == (CompiledNativeCapability(tool=tool),)


def test_an_anonymous_agent_may_grant_a_provider_tool() -> None:
    """``native`` is exempt from the anonymous gate: it reads no application data."""
    compiled, issues = _compile(
        _spec("web_search"),
        inference=_BEDROCK,
        native_tools=lambda _t: frozenset({"web_search"}),
        endpoints={"searcher": AgentEndpointConfig(enabled=True, auth="jwt", allow_anonymous=True)},
    )

    assert issues == []
    assert compiled == (CompiledNativeCapability(tool="web_search"),)
