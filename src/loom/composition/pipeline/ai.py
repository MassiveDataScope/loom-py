"""Compose the optional agent runtime after core registrations."""

from __future__ import annotations

from loom.composition._ai import (
    AiWiring,
    bind_agent_resolver,
    bind_mcp_resolver,
    marker_declarations,
    reject_ai_markers,
    reject_manifest_agents_without_ai_section,
    resolve_ai,
    verify_agent_markers,
    verify_mcp_markers,
)
from loom.composition.pipeline.registration import Registered


def wire_ai(registered: Registered) -> AiWiring:
    """Build the agent runtime when the entry point hosts it, else nothing.

    Every ``Agent()`` and ``Mcp()`` declaration the analysed graph reaches
    counts, in a use case's ``execute()`` or on an adapter it is built
    with; the graph is walked once, here. An entry point that does not host
    agents still refuses, at start-up, any such declaration. One that
    serves no agent endpoints builds only the agents a declaration names. A
    lenient recipe that hosts no agents refuses none and walks nothing.

    Raises:
        AgentCompilationError: When a declaration cannot be satisfied.
    """
    planned = registered.persisted.planned
    source, recipe, kernel = planned.source, planned.recipe, registered.kernel
    if not recipe.agents and recipe.lenient:
        return AiWiring(config=None, runtime=None)
    declared = marker_declarations(registered.analysis, kernel.compiler, kernel.registry)
    if not recipe.agents:
        reject_ai_markers(declared)
        return AiWiring(config=None, runtime=None)
    ctx, discovered = source.config, recipe.discovered
    reject_manifest_agents_without_ai_section(ctx, discovered.agent_specs)
    verify_mcp_markers(declared.mcp, ctx)
    ai = resolve_ai(
        ctx,
        kernel=kernel,
        sql_cfg=registered.sql.config,
        code_path=source.code_path,
        manifest_agent_specs=discovered.agent_specs,
        mcp=declared.mcp,
        names=(
            None
            if recipe.agent_endpoints
            else frozenset(found.binding.declaration.name for _, found in declared.agents)
        ),
    )
    verify_agent_markers(declared.agents, ai)
    bind_agent_resolver(kernel, ai)
    bind_mcp_resolver(kernel, ai)
    return ai
