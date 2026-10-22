"""Projection of one compiled plan into its public description (US7).

``loom.ai.describe`` is the AI pillar's contribution to ``describe_app``: it
turns an :class:`~loom.ai.compiler._plan.AgentPlan` into the subset of itself
that is safe to publish.  The projection is an explicit allow-list per
capability kind — that is what makes the exclusions demonstrable rather than
incidental, so every kind is asserted here settings by settings.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from decimal import Decimal
from types import MappingProxyType
from typing import Any, ClassVar, cast

import msgspec
import pytest

from loom.ai.abc import ToolsetContext
from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledCapability,
    CompiledInstruction,
    CompiledMcpCapability,
    CompiledNativeCapability,
    CompiledOutput,
    CompiledPythonCapability,
    CompiledSkillsCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
)
from loom.ai.declarative import PolicySpec
from loom.ai.describe import _as_builtin, describe_agent, describe_agents
from loom.ai.inference import InferenceTarget, _RedactedOptions
from loom.core.engine.compilable import Compilable
from loom.core.introspection import IntrospectionError
from loom.core.model import LoomFrozenStruct, msgspec_type
from loom.core.model.redaction import Dsn
from loom.core.sql.config import SqlConnectionConfig

_AGENT = "describable"
_OUTPUT_SCHEMA: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["answer"],
    "properties": {"answer": {"type": "string"}},
}
_SOURCE_PATH = "/srv/app/ai/agents/describable/agent.yaml"

_USECASE_KEYS: tuple[str, ...] = ("orders.get_order_status", "customers.get_profile")
_SQL_CONNECTION = "reporting"
_MCP_SERVER = "tools"
_A2A_AGENT = "oncall"
_SKILLS_LIBRARY = "shared"
_SKILLS_DIRECTORY = "/srv/app/skills/shared"
_FACTORY_REF = "myapp.tools.geo:build_geo_toolset"


class _GrantedUseCase:
    """Stand-in for a use-case type the registry resolved a granted key to."""


class _GeoToolsetFactory:
    """Imported toolset factory a ``python`` grant resolved to."""

    def __call__(self, context: ToolsetContext) -> object:
        """Build the engine-facing toolset."""
        del context
        return object()


class _QuantumCapability(LoomFrozenStruct, frozen=True, kw_only=True):
    """Capability of a kind no projection knows how to publish."""

    kind: ClassVar[str] = "quantum"

    entanglement: str = "spooky"


def _usecase_capability() -> CompiledUsecaseCapability:
    """Build the ``usecase`` grant: keys are public, resolved types are not."""
    return CompiledUsecaseCapability(
        keys=_USECASE_KEYS,
        use_cases=cast("tuple[type[Compilable], ...]", (_GrantedUseCase, _GrantedUseCase)),
    )


def _sql_capability() -> CompiledSqlCapability:
    """Build the ``sql`` grant: the connection name is public, its DSN is not."""
    return CompiledSqlCapability(
        connection=_SQL_CONNECTION,
        config=SqlConnectionConfig(
            backend="clickhouse",
            url=Dsn("clickhouse://reports.internal:8123/reporting"),
            allowed_roles=(),
            readonly=True,
        ),
        max_rows=1000,
        max_result_bytes=1_000_000,
    )


def _mcp_capability() -> CompiledMcpCapability:
    """Build the ``mcp`` grant: the server name is public, its address is not."""
    return CompiledMcpCapability(
        server=_MCP_SERVER,
        url="https://tools.internal/mcp",
        headers_ref="mcp-headers",
        timeout_ms=15000,
        include=("search_*",),
        exclude=("delete_*",),
    )


def _skills_capability() -> CompiledSkillsCapability:
    """Build the ``skills`` grant: names are public, the deployment path is not."""
    return CompiledSkillsCapability(
        library=_SKILLS_LIBRARY,
        directory=_SKILLS_DIRECTORY,
        names=("tone-of-voice", "release-notes"),
    )


def _python_capability() -> CompiledPythonCapability:
    """Build the ``python`` grant: the reference is public, the callable is not."""
    return CompiledPythonCapability(
        factory_ref=_FACTORY_REF,
        factory=_GeoToolsetFactory(),
        params={"radius_km": 25, "max_results": 3},
    )


def _a2a_capability() -> CompiledA2ACapability:
    """Build the ``a2a`` grant: the agent name is public, its address is not."""
    return CompiledA2ACapability(
        agent=_A2A_AGENT,
        url="https://oncall.internal/a2a",
        headers_ref="a2a-headers",
        include=("page_oncall",),
        exclude=("close_incident",),
    )


def _make_plan(
    *,
    name: str = _AGENT,
    capabilities: tuple[CompiledCapability, ...],
    policies: PolicySpec | None = None,
) -> AgentPlan:
    """Build a compiled plan carrying every field the projection must consider."""
    return AgentPlan(
        name=name,
        description="Investigates incidents and proposes the next remediation step.",
        instructions=(CompiledInstruction(text="Never published: the artifact's private prompt."),),
        spec_version=1,
        inference=InferenceTarget(
            provider="bedrock",
            model="anthropic.claude",
            region="eu-west-1",
            endpoint="https://bedrock.eu-west-1.amazonaws.com",
            credentials_ref="prod/bedrock",
        ),
        output=CompiledOutput(schema=_OUTPUT_SCHEMA, loom_type=msgspec_type(dict)),
        capabilities=capabilities,
        policies=policies
        or PolicySpec(
            retries=3,
            tool_timeout_ms=30000,
            max_iterations=20,
            run_timeout_ms=300000,
        ),
        metadata={"owner": "platform-reliability"},
        source_path=_SOURCE_PATH,
    )


def _normalise(value: Any) -> Any:
    """Normalise sequences to tuples so list/tuple projections compare equal."""
    if isinstance(value, Mapping):
        return {key: _normalise(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return tuple(_normalise(item) for item in value)
    return value


def _settings_leaves(settings: Mapping[str, Any]) -> Iterator[Any]:
    """Yield every leaf of ``settings``, descending only through sequences.

    A nested mapping is a leaf on purpose: no projector produces one today,
    and a mapping there would be the shape a struct expands into.
    """
    for value in settings.values():
        yield from _sequence_leaves(value)


def _sequence_leaves(value: Any) -> Iterator[Any]:
    """Yield ``value``, or each of its items when it is a list or a tuple."""
    if isinstance(value, (list, tuple)):
        for item in value:
            yield from _sequence_leaves(item)
        return
    yield value


def _settings_of(capability: CompiledCapability) -> Mapping[str, Any]:
    """Describe a one-capability plan and return that capability's settings."""
    described = describe_agent(_make_plan(capabilities=(capability,)))
    if len(described.capabilities) != 1:
        raise AssertionError(f"expected one described capability, got {described.capabilities!r}")
    return cast("Mapping[str, Any]", _normalise(described.capabilities[0].settings))


@pytest.fixture
def full_plan() -> AgentPlan:
    """Plan granted one capability of every compiled kind."""
    return _make_plan(
        capabilities=(
            _usecase_capability(),
            _sql_capability(),
            _mcp_capability(),
            _skills_capability(),
            _python_capability(),
            _a2a_capability(),
        )
    )


class TestAgentProjection:
    """The agent-level projection publishes the contract and nothing else."""

    def test_publishes_the_name(self, full_plan: AgentPlan) -> None:
        """The name is how a caller addresses the agent."""
        assert describe_agent(full_plan).name == _AGENT

    def test_publishes_the_spec_version(self, full_plan: AgentPlan) -> None:
        """``spec_version`` tells a client which artifact format compiled."""
        assert describe_agent(full_plan).spec_version == 1

    def test_publishes_the_output_schema(self, full_plan: AgentPlan) -> None:
        """``output_schema`` is published; the built decoder never is."""
        assert _normalise(describe_agent(full_plan).output_schema) == _normalise(_OUTPUT_SCHEMA)

    def test_publishes_the_policies(self, full_plan: AgentPlan) -> None:
        """The execution limits are published as a plain mapping."""
        assert dict(describe_agent(full_plan).policies) == {
            "retries": 3,
            "tool_timeout_ms": 30000,
            "max_iterations": 20,
            "run_timeout_ms": 300000,
            "max_history_bytes": 1048576,
            "max_usd": None,
            "max_total_tokens": None,
            "max_input_tokens_per_request": None,
            "max_tool_calls": None,
            "max_requests": 50,
            "on_unpriced_spend": "serve",
        }

    def test_max_usd_is_published_as_a_number_when_declared(self) -> None:
        """``max_usd`` survives the projection as a number, matching the published schema.

        msgspec encodes a bare ``Decimal`` as a JSON *string*: the schema
        declares ``policies.max_usd`` as ``type: number``, so this projection
        fixes the wire form to match the published schema, rather than
        leaving it to whatever msgspec's default happens to be.
        """
        plan = _make_plan(
            capabilities=(),
            policies=PolicySpec(max_usd=Decimal("2.00")),
        )

        described = describe_agent(plan)
        assert described.policies["max_usd"] == 2.0

        encoded = msgspec.to_builtins(described, enc_hook=_as_builtin)
        payload = json.loads(msgspec.json.encode(encoded))
        assert payload["policies"]["max_usd"] == 2.0

    def test_publishes_the_source_path_when_the_plan_carries_one(
        self, full_plan: AgentPlan
    ) -> None:
        """``source_path`` is provenance, not secret material."""
        assert describe_agent(full_plan).source_path == _SOURCE_PATH

    def test_preserves_the_order_of_capabilities(self, full_plan: AgentPlan) -> None:
        """Capabilities are described in the order the plan carries them."""
        described = describe_agent(full_plan)

        assert tuple(item.kind for item in described.capabilities) == (
            "usecase",
            "sql",
            "mcp",
            "skills",
            "python",
            "a2a",
        )


class TestSettingsByKind:
    """Each kind publishes exactly its allow-listed settings, never the handles."""

    def test_publishes_only_the_keys_for_a_usecase_capability(self) -> None:
        """The granted keys are public; the resolved use-case types are not."""
        assert _settings_of(_usecase_capability()) == {"keys": _USECASE_KEYS}

    def test_publishes_the_tool_name_for_a_native_capability(self) -> None:
        """A provider tool publishes its name, which is the whole of the grant."""
        assert _settings_of(CompiledNativeCapability(tool="web_search")) == {"tool": "web_search"}

    def test_publishes_connection_and_limits_for_a_sql_capability(self) -> None:
        """The connection name and its caps are public; the DSN is not."""
        assert _settings_of(_sql_capability()) == {
            "connection": _SQL_CONNECTION,
            "max_rows": 1000,
            "max_result_bytes": 1_000_000,
        }

    def test_publishes_server_and_filter_for_a_mcp_capability(self) -> None:
        """Server name, transport, filter and deadline are public; URL and headers are not."""
        assert _settings_of(_mcp_capability()) == {
            "server": _MCP_SERVER,
            "transport": "http",
            "include": ("search_*",),
            "exclude": ("delete_*",),
            "timeout_ms": 15000,
        }

    def test_publishes_library_and_names_for_a_skills_capability(self) -> None:
        """The library and the selected skills are public; the directory is not."""
        assert _settings_of(_skills_capability()) == {
            "library": _SKILLS_LIBRARY,
            "names": ("tone-of-voice", "release-notes"),
        }

    def test_publishes_factory_ref_and_param_names_for_a_python_capability(self) -> None:
        """The reference and the sorted parameter names are public; callable and values are not."""
        assert _settings_of(_python_capability()) == {
            "factory_ref": _FACTORY_REF,
            "params": ("max_results", "radius_km"),
        }

    def test_publishes_agent_and_filter_for_a_a2a_capability(self) -> None:
        """The remote agent name and filter are public; URL and headers are not."""
        assert _settings_of(_a2a_capability()) == {
            "agent": _A2A_AGENT,
            "include": ("page_oncall",),
            "exclude": ("close_incident",),
        }


class TestUnknownKind:
    """An unknown kind is never dumped wholesale: that is how exclusion holds."""

    def test_fails_for_a_capability_of_an_unregistered_kind(self) -> None:
        """A capability with no registered projection is reported, not guessed."""
        plan = _make_plan(
            capabilities=cast("tuple[CompiledCapability, ...]", (_QuantumCapability(),))
        )

        with pytest.raises(IntrospectionError):
            describe_agent(plan)


class TestAgentsContributor:
    """``describe_agents`` is the callable ``describe_app`` resolves by reference."""

    def test_preserves_the_order_of_the_plans(self) -> None:
        """The contribution lists agents in the order the plans were compiled."""
        plans = (
            _make_plan(name="beta", capabilities=()),
            _make_plan(name="alpha", capabilities=()),
        )

        assert [agent["name"] for agent in describe_agents(plans)] == ["beta", "alpha"]

    def test_returns_json_encodable_builtins(self, full_plan: AgentPlan) -> None:
        """The contribution is JSON-encodable without a custom encoder."""
        described = describe_agents((full_plan,))

        assert json.loads(json.dumps(described))[0]["name"] == _AGENT


class TestEncodingHook:
    """The encoding hook is fail-closed: it converts by exact type, not by protocol."""

    def test_rejects_a_mapping_that_is_neither_dict_nor_mappingproxy(self) -> None:
        """A ``Mapping`` written to refuse encoding is rejected, not unwrapped.

        ``_RedactedOptions`` exists so msgspec cannot encode a resolved
        inference target; a protocol-wide hook would defeat that.
        """
        options = _RedactedOptions({"temperature": "secret"})

        with pytest.raises(IntrospectionError):
            _as_builtin(options)

    def test_converts_a_mappingproxy_into_a_plain_dict(self) -> None:
        """The read-only mapping a compiled schema carries becomes a plain dict."""
        assert _as_builtin(MappingProxyType({"type": "object"})) == {"type": "object"}


class TestSettingsLeaves:
    """``settings`` publishes scalars only: a struct there would encode natively."""

    def test_every_settings_leaf_is_a_scalar(self, full_plan: AgentPlan) -> None:
        """``msgspec`` encodes by runtime type, so only scalars may reach ``settings``.

        A ``Struct`` or nested mapping slipped into a projector would expand
        without ever passing through the hook; this pins every leaf instead.
        """
        described = describe_agents((full_plan,))

        leaves = [
            leaf
            for agent in described
            for capability in agent["capabilities"]
            for leaf in _settings_leaves(capability["settings"])
        ]
        assert leaves != []
        assert [leaf for leaf in leaves if not isinstance(leaf, (str, int, bool, type(None)))] == []
