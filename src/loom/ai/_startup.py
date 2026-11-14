"""Start-up verification of reached ``Agent()`` and ``Mcp()`` declarations.

:mod:`loom.composition._ai` walks the dependency graph once per start-up
(:func:`~loom.composition._ai.marker_declarations`) and calls these checks
only when at least one declaration of their marker is reached, so a
deployment that declares none never imports this module. A
declaration may sit in a use case's ``execute()`` or constructor, or on an
adapter, factory or called use case it reaches; every message names where,
as the declaration's ``Reached.where``.

An entry point that hosts no agents (a task worker) never builds an AI
runtime, so it calls :func:`verify_agent_markers` with an empty
``plans_by_name`` and :func:`verify_mcp_markers` with an empty server
mapping — each reports every declared name as unknown, the same message a
typo gets under a real runtime.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import get_args

from loom.ai.compiler import AgentPlan
from loom.ai.config import McpServerConfig
from loom.ai.errors import AgentCompilationError, AgentCompilationIssue, AgentErrorCode
from loom.core.use_case.walk import Reached


def verify_agent_markers(
    agents: Sequence[tuple[str, Reached]],
    plans_by_name: Mapping[str, AgentPlan],
) -> None:
    """Abort start-up when an ``Agent()`` declaration cannot be satisfied.

    Args:
        agents: Every ``Agent()`` declaration reached, each with the
            component reaching it.
        plans_by_name: Every compiled agent plan reachable in this
            deployment, by its own name. Empty when no AI runtime was built
            at all, which reports every declared agent as unknown.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown agent name
            and per mismatched output type, so a single run reports every
            problem at once.
    """
    issues = list(_agent_issues(agents, plans_by_name))
    if issues:
        raise AgentCompilationError(issues)


def _agent_issues(
    agents: Sequence[tuple[str, Reached]],
    plans_by_name: Mapping[str, AgentPlan],
) -> Iterator[AgentCompilationIssue]:
    """Yield one issue per ``Agent()`` declaration that start-up cannot satisfy."""
    available = tuple(plans_by_name)
    for component, found in agents:
        agent = found.binding.declaration.name
        agent_plan = plans_by_name.get(agent)
        if agent_plan is None:
            yield AgentErrorCode.AGENT_MARKER_UNKNOWN.issue(
                component=component, parameter=found.where, agent=agent, available=available
            )
            continue
        expected_args = get_args(found.binding.annotation)
        if not expected_args:
            continue
        expected = expected_args[0]
        declared = agent_plan.output.loom_type.type
        if expected is declared:
            continue
        yield AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH.issue(
            component=component,
            parameter=found.where,
            agent=agent,
            expected=getattr(expected, "__name__", str(expected)),
            declared=getattr(declared, "__name__", str(declared)),
        )


def verify_mcp_markers(
    mcp: Sequence[tuple[str, Reached]],
    servers: Mapping[str, McpServerConfig],
) -> None:
    """Abort start-up when an ``Mcp()`` declaration names a server not configured.

    Only the name is checked here — whether ``include`` matches a published
    tool needs a live session and is checked later, inside the runtime's own
    listing pass (``_verify_tool_filters``), on the same ``startup_timeout_ms``
    an agent's own filter is checked against.

    Args:
        mcp: Every ``Mcp()`` declaration reached, each with the component
            reaching it.
        servers: Every MCP server configured for this deployment, from
            ``ai.mcp_servers``. Empty when no ``ai:`` section is present,
            which reports every declared server as unknown — the same
            fail-closed answer :func:`verify_agent_markers` gives with an
            empty ``plans_by_name``.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown server name,
            so a single run reports every problem at once.
    """
    available = tuple(servers)
    issues = [
        AgentErrorCode.MCP_MARKER_UNKNOWN.issue(
            component=component,
            parameter=found.where,
            server=found.binding.declaration.server,
            available=available,
        )
        for component, found in mcp
        if found.binding.declaration.server not in servers
    ]
    if issues:
        raise AgentCompilationError(issues)


__all__ = ["verify_agent_markers", "verify_mcp_markers"]
