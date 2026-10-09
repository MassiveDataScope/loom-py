"""Offline checks that one agent can serve an ETL step."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from loom.ai.abc import AgentEngineProvider
from loom.ai.compiler import AgentPlan
from loom.ai.errors import AgentCompilationError
from loom.ai.etl._columns import output_columns
from loom.ai.etl._frames import metadata_clashes
from loom.ai.etl._ledger import worst_case
from loom.ai.registry import engine_prices_model
from loom.etl.runtime.contracts import AgentIssue, AgentIssueKind


def validation_issues(
    name: str,
    output_type: type[Any],
    *,
    max_usd: Decimal | None,
    compile_plans: Callable[[], tuple[AgentPlan, ...]],
    engine_provider: Callable[[], AgentEngineProvider],
) -> tuple[AgentIssue, ...]:
    """Return every reason agent *name* cannot serve a step, without spending anything.

    Args:
        name: Agent name declared by the step.
        output_type: Type the step expects every answer to decode into.
        max_usd: Budget of one execution of the step, when declared.
        compile_plans: Compiles the artifacts declaring *name*.
        engine_provider: Builds the engine provider asked whether it prices the model.

    Returns:
        The issues found, empty when the agent can serve the step.
    """
    try:
        plans = compile_plans()
        if not plans:
            return (_issue(AgentIssueKind.NOT_FOUND, f"no agent artifact declares {name!r}"),)
        return _plan_issues(plans[0], output_type, max_usd, engine_provider())
    except AgentCompilationError as error:
        return (_issue(AgentIssueKind.COMPILATION_FAILED, _describe(error)),)


def _plan_issues(
    plan: AgentPlan,
    output_type: type[Any],
    max_usd: Decimal | None,
    provider: AgentEngineProvider,
) -> tuple[AgentIssue, ...]:
    issues: list[AgentIssue] = []
    if plan.output.loom_type.type is not output_type:
        issues.append(_issue(AgentIssueKind.OUTPUT_MISMATCH, _mismatch(plan, output_type)))
    elif clashes := metadata_clashes(output_columns(output_type)):
        issues.append(_issue(AgentIssueKind.COLUMN_COLLISION, _collision(plan, clashes)))
    if _budgeted(plan, max_usd) and not _priced(plan, provider):
        issues.append(_issue(AgentIssueKind.UNPRICED_BUDGET, _unpriced(plan)))
    if max_usd is not None:
        issues.extend(_ceiling_issues(plan, max_usd))
    return tuple(issues)


def _ceiling_issues(plan: AgentPlan, max_usd: Decimal) -> tuple[AgentIssue, ...]:
    worst = worst_case(plan)
    if worst is None:
        return (_issue(AgentIssueKind.BUDGET_UNENFORCEABLE, _uncapped(plan)),)
    if worst > max_usd:
        return (_issue(AgentIssueKind.BUDGET_UNENFORCEABLE, _over_budget(plan, worst, max_usd)),)
    return ()


def _budgeted(plan: AgentPlan, max_usd: Decimal | None) -> bool:
    return max_usd is not None or plan.policies.max_usd is not None


def _priced(plan: AgentPlan, provider: AgentEngineProvider) -> bool:
    return plan.price is not None or engine_prices_model(provider, plan.inference)


def _issue(kind: AgentIssueKind, message: str) -> AgentIssue:
    return AgentIssue(kind=kind, message=message)


def _describe(error: AgentCompilationError) -> str:
    return "; ".join(f"{issue.code}: {issue.message}" for issue in error.issues)


def _mismatch(plan: AgentPlan, output_type: type[Any]) -> str:
    return (
        f"agent {plan.name!r} does not answer {output_type.__qualname__}; its artifact must "
        f"declare 'output: {{kind: type_ref}}' naming that type"
    )


def _collision(plan: AgentPlan, clashes: list[str]) -> str:
    return (
        f"agent {plan.name!r} answers field(s) {clashes}, named like the run metadata "
        "columns the step writes; rename them in the output type"
    )


def _uncapped(plan: AgentPlan) -> str:
    return (
        f"agent {plan.name!r} has a step budget, but its artifact declares no "
        "'policies.max_usd', so no run has a worst case to reserve; declare one"
    )


def _over_budget(plan: AgentPlan, worst: Decimal, max_usd: Decimal) -> str:
    return (
        f"one run of agent {plan.name!r} reserves {worst} USD ('policies.max_usd'), "
        f"more than the step budget of {max_usd} USD; raise the budget or lower the cap"
    )


def _unpriced(plan: AgentPlan) -> str:
    return (
        f"agent {plan.name!r} has a spend budget, but model {plan.inference.model!r} has no "
        "known price; add it under 'ai.prices'"
    )
