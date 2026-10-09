"""Compiler validators public surface.

This module re-exports step-level and plan-level validators to keep a stable
import path while implementation lives in smaller modules.
"""

from loom.etl.compiler._validators_plan import (
    validate_plan_catalog,
    validate_plan_temps,
    validate_process_catalog,
    validate_step_catalog,
)
from loom.etl.compiler._validators_step import (
    StepCompilationContext,
    validate_execute_signature,
    validate_historify_spec,
    validate_param_exprs,
    validate_params_compat,
    validate_upsert_spec,
)


def validate_step(ctx: StepCompilationContext) -> None:
    """Run all per-step compile-time validators against *ctx*."""
    validate_execute_signature(
        ctx.step_type,
        ctx.params_type,
        ctx.source_bindings,
        ctx.config_bindings,
        ctx.agent_bindings,
    )
    validate_upsert_spec(ctx.step_type, ctx.target_binding.spec)
    validate_historify_spec(ctx.step_type, ctx.target_binding.spec)
    validate_param_exprs(ctx.step_type, ctx.params_type, ctx.source_bindings, ctx.target_binding)


__all__ = [
    "StepCompilationContext",
    "validate_execute_signature",
    "validate_historify_spec",
    "validate_param_exprs",
    "validate_params_compat",
    "validate_plan_catalog",
    "validate_plan_temps",
    "validate_process_catalog",
    "validate_step",
    "validate_step_catalog",
    "validate_upsert_spec",
]
