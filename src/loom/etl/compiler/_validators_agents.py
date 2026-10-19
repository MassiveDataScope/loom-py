"""Compile-time check of ``WithAgent`` declarations through the agent runner port.

Internal module — consumed only by :mod:`loom.etl.compiler._compiler`.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Final

from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    iter_all_steps,
    iter_steps_in_process,
)
from loom.etl.runtime.contracts import AgentBatchRunner, AgentIssueKind

_CODES: Final[Mapping[AgentIssueKind, ETLErrorCode]] = MappingProxyType(
    {
        AgentIssueKind.NOT_FOUND: ETLErrorCode.AGENT_NOT_FOUND,
        AgentIssueKind.COMPILATION_FAILED: ETLErrorCode.AGENT_COMPILATION_FAILED,
        AgentIssueKind.OUTPUT_MISMATCH: ETLErrorCode.AGENT_OUTPUT_MISMATCH,
        AgentIssueKind.UNPRICED_BUDGET: ETLErrorCode.AGENT_UNPRICED_BUDGET,
        AgentIssueKind.BUDGET_UNENFORCEABLE: ETLErrorCode.AGENT_BUDGET_UNENFORCEABLE,
        AgentIssueKind.UNSUPPORTED_ENGINE: ETLErrorCode.AGENT_UNSUPPORTED_ENGINE,
        AgentIssueKind.COLUMN_COLLISION: ETLErrorCode.AGENT_COLUMN_COLLISION,
    }
)
"""Code of each agent issue kind; every one takes ``component, alias, reason``."""


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
            raise ETLCompilationError.from_issue(
                ETLErrorCode.AGENT_RUNNER_NOT_CONFIGURED.issue(
                    component=plan.step_type, alias=binding.alias
                )
            )
        issues = runner.validate(binding.name, binding.output_type, max_usd=binding.max_usd)
        if issues:
            first = issues[0]
            issue = _CODES[first.kind].issue(
                component=plan.step_type, alias=binding.alias, reason=first.message
            )
            raise ETLCompilationError.from_issue(issue)


def validate_process_agents(plan: ProcessPlan, runner: AgentBatchRunner | None) -> None:
    """Run :func:`validate_step_agents` on every step of a process plan."""
    for step in iter_steps_in_process(plan):
        validate_step_agents(step, runner)


def validate_plan_agents(plan: PipelinePlan, runner: AgentBatchRunner | None) -> None:
    """Run :func:`validate_step_agents` on every step of a pipeline plan."""
    for step in iter_all_steps(plan):
        validate_step_agents(step, runner)
