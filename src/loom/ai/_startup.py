"""Start-up verification of use-case marker bindings, shared by every worker.

Both composition roots that admit a marker — the FastAPI auto-bootstrap
(:mod:`loom.rest.fastapi.auto`) and the Celery worker bootstrap
(:mod:`loom.celery.bootstrap`) — call :func:`verify_agent_markers` once every
compiled use case declaring at least one ``Agent()`` binding is already known
via :func:`~loom.core.use_case.agent_markers.declaring_agent_bindings`, so
neither ever imports this module for a deployment that declares no marker at
all (FR-050). :func:`verify_mcp_markers` follows the same shape for the
``Mcp()`` marker, driven by
:func:`~loom.core.use_case.mcp_markers.declaring_mcp_bindings` instead.

The Celery worker never builds an AI runtime (T402: task workers do not serve
the AI pillar), so it always calls :func:`verify_agent_markers` with an empty
``plans_by_name`` and :func:`verify_mcp_markers` with an empty server
mapping — each reports every declared name as unknown, the same message a
typo gets under a real runtime, naming the parameter and the use case that
could not run.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import get_args

from loom.ai.compiler import AgentPlan
from loom.ai.config import McpServerConfig
from loom.ai.errors import AgentCompilationError, AgentCompilationIssue, AgentErrorCode
from loom.core.engine.compilable import Compilable
from loom.core.engine.plan import AgentBinding, McpBinding
from loom.core.use_case.registry import UseCaseRegistry


def verify_agent_markers(
    declaring: Sequence[tuple[type[Compilable], tuple[AgentBinding, ...]]],
    registry: UseCaseRegistry,
    plans_by_name: Mapping[str, AgentPlan],
) -> None:
    """Abort start-up when an ``Agent()`` marker cannot be satisfied.

    Args:
        declaring: Use cases declaring at least one ``Agent()`` binding,
            paired with those bindings — see
            :func:`~loom.core.use_case.agent_markers.declaring_agent_bindings`.
        registry: Resolves a use case's registered key for the error message.
        plans_by_name: Every compiled agent plan reachable in this
            deployment, by its own name. Empty when no AI runtime was built
            at all, which reports every declared agent as unknown.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown agent name
            and per mismatched output type, so a single run reports every
            problem at once.
    """
    issues: list[AgentCompilationIssue] = [
        issue
        for uc_type, agent_bindings in declaring
        for issue in _agent_binding_issues(
            registry.key_for(uc_type) or uc_type.__qualname__, agent_bindings, plans_by_name
        )
    ]
    if issues:
        raise AgentCompilationError(issues)


def _agent_binding_issues(
    uc_name: str,
    agent_bindings: Sequence[AgentBinding],
    plans_by_name: Mapping[str, AgentPlan],
) -> Iterator[AgentCompilationIssue]:
    """Yield one issue per ``Agent()`` binding that start-up cannot satisfy.

    Args:
        uc_name: Registered key (or qualname) of the declaring use case.
        agent_bindings: Every ``Agent()`` parameter that use case declares.
        plans_by_name: Every compiled agent plan, by its own name.
    """
    available = tuple(plans_by_name)
    for binding in agent_bindings:
        agent_plan = plans_by_name.get(binding.agent)
        if agent_plan is None:
            yield AgentErrorCode.AGENT_MARKER_UNKNOWN.issue(
                component=uc_name, parameter=binding.name, agent=binding.agent, available=available
            )
            continue
        expected_args = get_args(binding.annotation)
        if not expected_args:
            # No type argument to check against — e.g. a bare 'AgentHandle'
            # annotation with no subscript. Nothing this pass can compare,
            # so it is not reported as a mismatch.
            continue
        expected = expected_args[0]
        declared = agent_plan.output.loom_type.type
        if expected is declared:
            continue
        yield AgentErrorCode.AGENT_MARKER_OUTPUT_MISMATCH.issue(
            component=uc_name,
            parameter=binding.name,
            agent=binding.agent,
            expected=getattr(expected, "__name__", str(expected)),
            declared=getattr(declared, "__name__", str(declared)),
        )


def verify_mcp_markers(
    declaring: Sequence[tuple[type[Compilable], tuple[McpBinding, ...]]],
    registry: UseCaseRegistry,
    servers: Mapping[str, McpServerConfig],
) -> None:
    """Abort start-up when an ``Mcp()`` marker names a server not configured.

    Only the name is checked here — whether ``include`` matches a published
    tool needs a live session and is checked later, inside the runtime's own
    listing pass (``_verify_tool_filters``), on the same ``startup_timeout_ms``
    an agent's own filter is checked against.

    Args:
        declaring: Use cases declaring at least one ``Mcp()`` binding, paired
            with those bindings — see
            :func:`~loom.core.use_case.mcp_markers.declaring_mcp_bindings`.
        registry: Resolves a use case's registered key for the error message.
        servers: Every MCP server configured for this deployment, from
            ``ai.mcp_servers``. Empty when no ``ai:`` section is present,
            which reports every declared server as unknown — the same
            fail-closed answer :func:`verify_agent_markers` gives with an
            empty ``plans_by_name``.

    Raises:
        AgentCompilationError: Aggregating one issue per unknown server name,
            so a single run reports every problem at once.
    """
    issues: list[AgentCompilationIssue] = [
        issue
        for uc_type, mcp_bindings in declaring
        for issue in _mcp_binding_issues(
            registry.key_for(uc_type) or uc_type.__qualname__, mcp_bindings, servers
        )
    ]
    if issues:
        raise AgentCompilationError(issues)


def _mcp_binding_issues(
    uc_name: str,
    mcp_bindings: Sequence[McpBinding],
    servers: Mapping[str, McpServerConfig],
) -> Iterator[AgentCompilationIssue]:
    """Yield one issue per ``Mcp()`` binding naming a server not configured.

    Args:
        uc_name: Registered key (or qualname) of the declaring use case.
        mcp_bindings: Every ``Mcp()`` parameter that use case declares.
        servers: Every configured MCP server, by its own name.
    """
    available = tuple(servers)
    for binding in mcp_bindings:
        if binding.server not in servers:
            yield AgentErrorCode.MCP_MARKER_UNKNOWN.issue(
                component=uc_name,
                parameter=binding.name,
                server=binding.server,
                available=available,
            )


__all__ = ["verify_agent_markers", "verify_mcp_markers"]
