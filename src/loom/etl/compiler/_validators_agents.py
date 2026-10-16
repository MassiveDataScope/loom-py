"""Compile-time check of ``WithAgent`` declarations through the agent runner port.

Internal module — consumed only by :mod:`loom.etl.compiler._compiler`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Final

from loom.etl.compiler._errors import ETLCompilationError
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    iter_all_steps,
    iter_steps_in_process,
)
from loom.etl.runtime.contracts import AgentBatchRunner, AgentIssueKind

_ERRORS: Final[Mapping[AgentIssueKind, Callable[[type, str, str], ETLCompilationError]]] = (
    MappingProxyType(
        {
            AgentIssueKind.NOT_FOUND: ETLCompilationError.agent_not_found,
            AgentIssueKind.COMPILATION_FAILED: ETLCompilationError.agent_compilation_failed,
            AgentIssueKind.OUTPUT_MISMATCH: ETLCompilationError.agent_output_mismatch,
            AgentIssueKind.UNPRICED_BUDGET: ETLCompilationError.agent_unpriced_budget,
            AgentIssueKind.BUDGET_UNENFORCEABLE: ETLCompilationError.agent_budget_unenforceable,
            AgentIssueKind.UNSUPPORTED_ENGINE: ETLCompilationError.agent_unsupported_engine,
            AgentIssueKind.COLUMN_COLLISION: ETLCompilationError.agent_column_collision,
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
        if runner is None:
            raise ETLCompilationError.agent_runner_not_configured(plan.step_type, binding.alias)
        issues = runner.validate(binding.name, binding.output_type, max_usd=binding.max_usd)
        if issues:
            first = issues[0]
            raise _ERRORS[first.kind](plan.step_type, binding.alias, first.message)


def validate_process_agents(plan: ProcessPlan, runner: AgentBatchRunner | None) -> None:
    """Run :func:`validate_step_agents` on every step of a process plan."""
    for step in iter_steps_in_process(plan):
        validate_step_agents(step, runner)


def validate_plan_agents(plan: PipelinePlan, runner: AgentBatchRunner | None) -> None:
    """Run :func:`validate_step_agents` on every step of a pipeline plan."""
    for step in iter_all_steps(plan):
        validate_step_agents(step, runner)
