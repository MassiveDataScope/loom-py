"""Public projection of compiled agents (US7).

The AI pillar's contribution to
:func:`~loom.core.introspection.describe_app`: it turns each compiled
:class:`~loom.ai.compiler._plan.AgentPlan` into the subset of itself that is
safe to publish.

The projection is an explicit allow-list per capability kind, not a dump of
the compiled struct.  A grant carries the resolved handle next to the public
name — the SQL connection config next to its name, the MCP URL next to its
server — so publishing "everything but a deny-list" would leak the moment a
new field appears.  A kind with no registered projection is therefore an
error, never a guess.

Excluded at agent level: ``instructions``, the resolved
``inference`` target, the compiled ``output.loom_type`` and the author's
free-form ``metadata``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from types import MappingProxyType
from typing import Any, cast

import msgspec

from loom.ai.compiler._plan import (
    AgentPlan,
    CompiledA2ACapability,
    CompiledCapability,
    CompiledMcpCapability,
    CompiledNativeCapability,
    CompiledPythonCapability,
    CompiledSkillsCapability,
    CompiledSqlCapability,
    CompiledUsecaseCapability,
)
from loom.ai.declarative import PolicySpec
from loom.core.introspection import IntrospectionError
from loom.core.model import LoomFrozenStruct

_BUILTIN_MAPPING_TYPES: frozenset[type] = frozenset({MappingProxyType, dict})

AGENTS_SECTION = "agents"
"""Section the agent descriptions appear under in the application document."""

AGENTS_CONTRIBUTOR = "loom.ai.describe:describe_agents"
"""``module:callable`` reference resolving to :func:`describe_agents`."""


class AgentCapabilityDescription(LoomFrozenStruct, frozen=True, kw_only=True):
    """One granted capability, reduced to its publishable settings.

    Attributes:
        kind: Capability kind, as declared in the artifact.
        settings: Allow-listed settings of that kind; never a resolved handle.
    """

    kind: str
    settings: Mapping[str, Any]


class AgentDescription(LoomFrozenStruct, frozen=True, kw_only=True):
    """Public description of one compiled agent.

    Attributes:
        name: Unique agent name within the application.
        description: The author's public sentence about the agent.
        spec_version: Artifact format the agent compiled from.
        output_schema: JSON Schema of the agent's structured answer.
        capabilities: Described grants, in the order the plan carries them.
        policies: Validated execution limits. An absent optional cap
            is published as ``None``. ``max_usd`` is published as a
            JSON number, matching the published schema's own
            ``policies.max_usd`` (``type: number``) — never as the string
            msgspec would otherwise encode a ``Decimal`` to.
        source_path: Artifact the agent was compiled from, when known.
    """

    name: str
    description: str
    spec_version: int
    output_schema: Mapping[str, Any]
    capabilities: tuple[AgentCapabilityDescription, ...]
    policies: Mapping[str, int | float | str | None]
    source_path: str | None


def _usecase_settings(capability: CompiledUsecaseCapability) -> Mapping[str, Any]:
    return {"keys": capability.keys}


def _sql_settings(capability: CompiledSqlCapability) -> Mapping[str, Any]:
    return {
        "connection": capability.connection,
        "max_rows": capability.max_rows,
        "max_result_bytes": capability.max_result_bytes,
    }


def _mcp_settings(capability: CompiledMcpCapability) -> Mapping[str, Any]:
    return {
        "server": capability.server,
        "transport": capability.transport,
        "include": capability.include,
        "exclude": capability.exclude,
        "timeout_ms": capability.timeout_ms,
    }


def _skills_settings(capability: CompiledSkillsCapability) -> Mapping[str, Any]:
    return {"library": capability.library, "names": capability.names}


def _python_settings(capability: CompiledPythonCapability) -> Mapping[str, Any]:
    return {"factory_ref": capability.factory_ref, "params": tuple(sorted(capability.params))}


def _a2a_settings(capability: CompiledA2ACapability) -> Mapping[str, Any]:
    return {
        "agent": capability.agent,
        "include": capability.include,
        "exclude": capability.exclude,
    }


def _native_settings(capability: CompiledNativeCapability) -> Mapping[str, Any]:
    """Publish the provider tool granted, which is the whole of the grant."""
    return {"tool": capability.tool}


_CapabilityProjector = Callable[[Any], Mapping[str, Any]]

_SETTINGS_PROJECTORS: Mapping[str, _CapabilityProjector] = {
    CompiledUsecaseCapability.kind: _usecase_settings,
    CompiledSqlCapability.kind: _sql_settings,
    CompiledMcpCapability.kind: _mcp_settings,
    CompiledSkillsCapability.kind: _skills_settings,
    CompiledPythonCapability.kind: _python_settings,
    CompiledA2ACapability.kind: _a2a_settings,
    CompiledNativeCapability.kind: _native_settings,
}


def _describe_capability(capability: CompiledCapability) -> AgentCapabilityDescription:
    kind = capability.kind
    projector = _SETTINGS_PROJECTORS.get(kind)
    if projector is None:
        raise IntrospectionError(
            f"capability kind {kind!r} has no published projection; "
            "a kind is never dumped wholesale."
        )
    return AgentCapabilityDescription(kind=kind, settings=projector(capability))


def describe_agent(plan: AgentPlan) -> AgentDescription:
    """Project one compiled plan into its public description.

    Args:
        plan: Compiled agent to describe.

    Returns:
        The publishable subset of the plan.

    Raises:
        IntrospectionError: When the plan grants a capability kind with no
            registered projection.

    Example::

        describe_agent(plan).capabilities[0].kind
        # 'usecase'
    """
    return AgentDescription(
        name=plan.name,
        description=plan.description,
        spec_version=plan.spec_version,
        output_schema=plan.output.schema,
        capabilities=tuple(_describe_capability(item) for item in plan.capabilities),
        policies=_describe_policies(plan.policies),
        source_path=plan.source_path,
    )


def _describe_policies(policies: PolicySpec) -> Mapping[str, int | float | str | None]:
    """Project ``policies`` onto JSON-encodable values, on the wire's own terms.

    ``msgspec.structs.asdict`` alone would leave ``max_usd`` a ``Decimal``,
    which ``msgspec.to_builtins`` then encodes as a string, diverging from
    the published schema's ``policies.max_usd`` (``type: number``).
    ``float(Decimal)`` is not exact in general
    (``Decimal(float(Decimal("0.1"))) != Decimal("0.1")``); this projection
    is a wire-friendly approximation for display and never reaches
    enforcement, which always checks the exact ``Decimal`` on
    ``PolicySpec.max_usd``.
    """
    described = dict(msgspec.structs.asdict(policies))
    if isinstance(described["max_usd"], Decimal):
        described["max_usd"] = float(described["max_usd"])
    return described


def _as_builtin(value: Any) -> Any:
    """Convert the read-only mappings a compiled schema carries into plain dicts.

    The guard matches the two exact types the projection is expected to carry
    — the ``mappingproxy`` of a compiled ``output.schema`` and a plain ``dict``
    — instead of the ``Mapping`` protocol.  A protocol check would also accept
    mappings written precisely so msgspec refuses to encode them, such as the
    redacted options of a resolved inference target, and would turn their
    fail-closed behaviour into a silent publication.

    Raises:
        IntrospectionError: For any other value, so nothing is published
            through an accidental conversion.
    """
    if type(value) in _BUILTIN_MAPPING_TYPES:
        return dict(cast("Mapping[str, Any]", value))
    raise IntrospectionError(f"value of type {type(value).__name__!r} is not publishable.")


def describe_agents(subject: Any, /) -> list[dict[str, Any]]:
    """Describe a sequence of compiled plans, as the AI pillar's contribution.

    This is the callable :func:`~loom.core.introspection.describe_app`
    resolves :data:`AGENTS_CONTRIBUTOR` to.

    Args:
        subject: Compiled plans, in the order they were compiled.

    Returns:
        One JSON-encodable mapping per plan, in that same order.

    Raises:
        IntrospectionError: When a plan grants a capability kind with no
            registered projection.

    Example::

        describe_agents(plans)[0]["name"]
        # 'triage'
    """
    plans = cast("Sequence[AgentPlan]", subject)
    described = [describe_agent(plan) for plan in plans]
    return cast("list[dict[str, Any]]", msgspec.to_builtins(described, enc_hook=_as_builtin))
