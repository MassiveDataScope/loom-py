"""Regression cases for the public ETL compiler validation path."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import (
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    Format,
    FromTable,
    IntoFile,
    IntoTemp,
)
from loom.etl.compiler import ETLCompilationError, ETLCompiler, ETLErrorCode


class Params(ETLParams):
    run_date: date


class StrictA(ETLStep[Params]):
    source = FromTable("raw.orders")
    target = IntoTemp("shared")

    def execute(self, params: Params, *, source: Any) -> Any:
        return source


class StrictB(ETLStep[Params]):
    source = FromTable("raw.orders")
    target = IntoTemp("shared")

    def execute(self, params: Params, *, source: Any) -> Any:
        return source


class AppendA(ETLStep[Params]):
    source = FromTable("raw.orders")
    target = IntoTemp("shared", append=True)

    def execute(self, params: Params, *, source: Any) -> Any:
        return source


class AppendB(ETLStep[Params]):
    source = FromTable("raw.orders")
    target = IntoTemp("shared", append=True)

    def execute(self, params: Params, *, source: Any) -> Any:
        return source


class StrictProcA(ETLProcess[Params]):
    steps = [StrictA]


class StrictProcB(ETLProcess[Params]):
    steps = [StrictB]


class AppendProcA(ETLProcess[Params]):
    steps = [AppendA]


class AppendProcB(ETLProcess[Params]):
    steps = [AppendB]


class NestedProcesses(ETLPipeline[Params]):
    processes = [[[StrictProcA]]]


class NestedStepsProc(ETLProcess[Params]):
    steps = [[[StrictA]]]


class NestedSteps(ETLPipeline[Params]):
    processes = [NestedStepsProc]


class ParallelStrict(ETLPipeline[Params]):
    processes = [[StrictProcA, StrictProcB]]


class ParallelFanIn(ETLPipeline[Params]):
    processes = [[AppendProcA, AppendProcB]]


class ParallelFanInStepsProc(ETLProcess[Params]):
    steps = [[AppendA, AppendB]]


class ParallelFanInSteps(ETLPipeline[Params]):
    processes = [ParallelFanInStepsProc]


class ParallelMixed(ETLPipeline[Params]):
    processes = [[StrictProcA, AppendProcB]]


class PriorThenParallelStepsProc(ETLProcess[Params]):
    steps = [StrictA, [StrictB]]


class PriorThenParallelSteps(ETLPipeline[Params]):
    processes = [PriorThenParallelStepsProc]


class InvalidFileTemplate(ETLStep[Params]):
    target = IntoFile("s3://exports/{unknown}.csv", format=Format.CSV)

    def execute(self, params: Params) -> Any:
        return None


@pytest.mark.parametrize(
    ("pipeline", "code"),
    [
        (NestedProcesses, ETLErrorCode.INVALID_PROCESS_ITEM),
        (NestedSteps, ETLErrorCode.INVALID_STEP_ITEM),
        (ParallelStrict, ETLErrorCode.DUPLICATE_TEMP_NAME),
        (ParallelMixed, ETLErrorCode.INVALID_TEMP_APPEND_MIX),
        (PriorThenParallelSteps, ETLErrorCode.DUPLICATE_TEMP_NAME),
    ],
)
def test_invalid_parallel_declarations_raise_typed_errors(
    pipeline: type[ETLPipeline[Params]], code: ETLErrorCode
) -> None:
    with pytest.raises(ETLCompilationError) as exc_info:
        ETLCompiler().compile(pipeline)
    assert exc_info.value.code is code


@pytest.mark.parametrize("pipeline", [ParallelFanIn, ParallelFanInSteps])
def test_parallel_fan_in_remains_valid(pipeline: type[ETLPipeline[Params]]) -> None:
    ETLCompiler().compile(pipeline)


def test_file_template_is_checked_by_compiler() -> None:
    with pytest.raises(ETLCompilationError) as exc_info:
        ETLCompiler().compile_step(InvalidFileTemplate)
    assert exc_info.value.code is ETLErrorCode.UNKNOWN_TEMPLATE_FIELD
