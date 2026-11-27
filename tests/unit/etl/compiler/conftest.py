"""Fixtures of the ETL compiler tests."""

from __future__ import annotations

import types
from collections.abc import Callable
from typing import Any

import pytest

from loom.etl import ETLStep, IntoTemp, Sources
from loom.etl.compiler import ETLCompiler, StepPlan
from tests.unit.etl.engines.doubles import NoParams

CompileTarget = Callable[[Any], StepPlan]


@pytest.fixture
def compile_target() -> CompileTarget:
    """Compile a step whose only declaration is the given target."""

    def compile_step(target: Any) -> StepPlan:
        step = types.new_class(
            "TargetProbe",
            (ETLStep[NoParams],),
            exec_body=lambda body: body.update(target=target, execute=lambda self, params: None),
        )
        return ETLCompiler().compile_step(step)

    return compile_step


@pytest.fixture
def compile_source() -> CompileTarget:
    """Compile a step whose only declaration is the given source, named ``orders``."""

    def compile_step(source: Any) -> StepPlan:
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

    return compile_step
