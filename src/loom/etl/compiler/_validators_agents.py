"""Compile-time check of ``WithAgent`` declarations through the agent runner port.

Internal module — consumed only by :mod:`loom.etl.compiler._compiler`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Final

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    AgentBinding,
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    iter_all_steps,
    iter_steps_in_process,
)
from loom.etl.runtime.contracts import AgentBatchRunner, AgentIssue, AgentIssueKind

_NO_RUNNER = AgentIssue(
    kind=AgentIssueKind.NOT_FOUND,
    message="no agent runner is configured; declare an 'ai:' section in the runner config",
)

_ERRORS: Final[Mapping[AgentIssueKind, Callable[[type, str, str], ETLCompilationError]]] = (
    MappingProxyType(
        {
            AgentIssueKind.NOT_FOUND: ETLCompilationError.agent_not_found,
            AgentIssueKind.COMPILATION_FAILED: ETLCompilationError.agent_compilation_failed,
            AgentIssueKind.OUTPUT_MISMATCH: ETLCompilationError.agent_output_mismatch,
            AgentIssueKind.UNPRICED_BUDGET: ETLCompilationError.agent_unpriced_budget,
            AgentIssueKind.BUDGET_UNENFORCEABLE: ETLCompilationError.agent_budget_unenforceable,
            AgentIssueKind.UNSUPPORTED_ENGINE: ETLCompilationError.agent_unsupported_engine,
        }
    )
)


def validate_step_agents(plan: StepPlan, runner: AgentBatchRunner | None) -> None:
    """Check that every ``WithAgent`` of *plan* can be served by *runner*.

    Args:
        plan: Compiled step plan.
        runner: Agent runner of the pipeline, or ``None`` when none is configured.

    Raises:
        ETLCompilationError: For the first issue of the first declaration
            that cannot be served.
    """
    for binding in plan.agent_bindings:
        issues = _issues(binding, runner)
        if issues:
            first = issues[0]
            raise _ERRORS[first.kind](plan.step_type, binding.alias, first.message)


def _issues(binding: AgentBinding, runner: AgentBatchRunner | None) -> tuple[AgentIssue, ...]:
    if runner is None:
        return (_NO_RUNNER,)
    return runner.validate(binding.name, binding.output_type, max_usd=binding.max_usd)


def validate_process_agents(plan: ProcessPlan, runner: AgentBatchRunner | None) -> None:
    """Run :func:`validate_step_agents` on every step of a process plan."""
    for step in iter_steps_in_process(plan):
        validate_step_agents(step, runner)


def validate_plan_agents(plan: PipelinePlan, runner: AgentBatchRunner | None) -> None:
    """Run :func:`validate_step_agents` on every step of a pipeline plan."""
    for step in iter_all_steps(plan):
        validate_step_agents(step, runner)
