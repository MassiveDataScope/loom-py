"""Compile-time check of ``FromConfig`` keys against the runner's config.

Internal module — consumed only by :mod:`loom.etl.compiler._compiler`.
"""

from __future__ import annotations

from loom.core.config import ConfigContext
from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.compiler._plan import (
    PipelinePlan,
    ProcessPlan,
    StepPlan,
    iter_all_steps,
    iter_steps_in_process,
)
from loom.etl.runtime._config_values import ConfigValueError, resolve_config_value


def validate_step_config(plan: StepPlan, context: ConfigContext) -> None:
    """Check that every ``FromConfig`` key of *plan* resolves and validates.

    Values are resolved and discarded; only the key and type reach an error.

    Args:
        plan: Compiled step plan.
        context: Config the runner was built from.

    Raises:
        ETLCompilationError: When a key is missing, cannot be resolved, or its
            value does not validate as the declared type.
    """
    for binding in plan.config_bindings:
        reason = _failure_reason(context, binding.key, binding.value_type)
        if reason is not None:
            raise ETLCompilationError.from_issue(
                ETLErrorCode.UNRESOLVED_CONFIG_VALUE.issue(
                    component=plan.step_type, alias=binding.alias, reason=reason
                )
            )


def _failure_reason(context: ConfigContext, key: str, value_type: object) -> str | None:
    """Return the sanitised failure message for *key*, or ``None`` when it resolves."""
    try:
        resolve_config_value(context, key, value_type)
    except ConfigValueError as exc:
        return str(exc)
    return None


def validate_process_config(plan: ProcessPlan, context: ConfigContext) -> None:
    """Run :func:`validate_step_config` on every step of a process plan."""
    for step in iter_steps_in_process(plan):
        validate_step_config(step, context)


def validate_plan_config(plan: PipelinePlan, context: ConfigContext) -> None:
    """Run :func:`validate_step_config` on every step of a pipeline plan."""
    for step in iter_all_steps(plan):
        validate_step_config(step, context)
