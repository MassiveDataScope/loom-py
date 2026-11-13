"""Compose the optional agent runtime after core registrations."""

from __future__ import annotations

from loom.composition._ai import (
    AiWiring,
    bind_agent_resolver,
    bind_mcp_resolver,
    marker_agents,
    reject_ai_markers,
    reject_manifest_agents_without_ai_section,
    resolve_ai,
    verify_agent_markers,
    verify_mcp_markers,
)
from loom.composition.pipeline.registration import Registered


def wire_ai(registered: Registered) -> AiWiring:
    """Build the agent runtime when the entry point hosts it, else nothing.

    An entry point that does not host agents still refuses, at start-up, any
    compiled use case declaring an ``Agent()`` or ``Mcp()`` marker. One that
    serves no agent endpoints builds only the agents a marker names. A
    lenient recipe that hosts no agents refuses no marker.

    Raises:
        AgentCompilationError: When a marker cannot be satisfied.
    """
    planned = registered.persisted.planned
    source, recipe, kernel = planned.source, planned.recipe, registered.kernel
    ctx = source.config
    discovered = recipe.discovered
    if not recipe.agents:
        if not recipe.lenient:
            reject_ai_markers(discovered.use_cases, kernel.compiler, kernel.registry)
        return AiWiring(config=None, runtime=None)
    reject_manifest_agents_without_ai_section(ctx, discovered.agent_specs)
    use_case_mcp_bindings = verify_mcp_markers(
        discovered.use_cases, kernel.compiler, kernel.registry, ctx
    )
    ai = resolve_ai(
        ctx,
        kernel=kernel,
        sql_cfg=registered.sql.config,
        code_path=source.code_path,
        manifest_agent_specs=discovered.agent_specs,
        use_case_mcp_bindings=use_case_mcp_bindings,
        names=(
            None if recipe.agent_endpoints else marker_agents(discovered.use_cases, kernel.compiler)
        ),
    )
    verify_agent_markers(discovered.use_cases, kernel.compiler, kernel.registry, ai)
    bind_agent_resolver(kernel, ai)
    bind_mcp_resolver(kernel, ai)
    return ai
