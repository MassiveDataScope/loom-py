"""Fixtures of the ETL compiler tests."""

from __future__ import annotations

import types
from collections.abc import Callable
from typing import Any

import pytest

from loom.etl import ETLStep
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
