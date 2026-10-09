"""Offline checks that one agent can serve an ETL step."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from loom.ai.compiler import AgentPlan
from loom.ai.errors import AgentCompilationError
from loom.etl.runtime.contracts import AgentIssue, AgentIssueKind


def validation_issues(
    name: str,
    output_type: type[Any],
    *,
    max_usd: Decimal | None,
    compile_plans: Callable[[], tuple[AgentPlan, ...]],
) -> tuple[AgentIssue, ...]:
    """Return every reason agent *name* cannot serve a step, without spending anything.

    Args:
        name: Agent name declared by the step.
        output_type: Type the step expects every answer to decode into.
        max_usd: Budget of one execution of the step, when declared.
        compile_plans: Compiles the artifacts declaring *name*.

    Returns:
        The issues found, empty when the agent can serve the step.
    """
    try:
        plans = compile_plans()
    except AgentCompilationError as error:
        return (AgentIssue(kind=AgentIssueKind.COMPILATION_FAILED, message=str(error)),)
    if not plans:
        return (
            AgentIssue(
                kind=AgentIssueKind.NOT_FOUND,
                message=f"no agent artifact declares an agent named {name!r}",
            ),
        )
    return ()
