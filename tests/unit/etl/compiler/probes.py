"""The compile probes the ETL compiler tests share: a declaration in, its compiled plan out."""

from __future__ import annotations

import types
from typing import Any

from loom.etl import ETLStep, IntoTemp, Sources
from loom.etl.compiler import ETLCompiler, StepPlan
from tests.unit.etl.engines.doubles import NoParams


def compile_target(target: Any) -> StepPlan:
    """Compile a step whose only declaration is *target*."""
    step = types.new_class(
        "TargetProbe",
        (ETLStep[NoParams],),
        exec_body=lambda body: body.update(target=target, execute=lambda self, params: None),
    )
    return ETLCompiler().compile_step(step)


def compile_source(source: Any) -> StepPlan:
    """Compile a step whose only declaration is *source*, named ``orders``."""
    step = types.new_class(
        "SourceProbe",
        (ETLStep[NoParams],),
        exec_body=lambda body: body.update(
            sources=Sources(orders=source),
            target=IntoTemp("probe_out"),
            execute=lambda self, params, *, orders: orders,
        ),
    )
    return ETLCompiler().compile_step(step)
