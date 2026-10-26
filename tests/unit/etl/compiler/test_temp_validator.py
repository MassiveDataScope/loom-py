"""Unit tests for compiler._temp_validator — direct function coverage."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import (
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLStep,
    FromTable,
    FromTemp,
    IntoTable,
    IntoTemp,
)
from loom.etl.compiler import ETLCompilationError, ETLCompiler, ETLErrorCode
from loom.etl.compiler._validators import validate_plan_temps


class P(ETLParams):
    run_date: date


class ProduceStep(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTemp("orders_temp")

    def execute(self, params: P, *, orders: Any) -> Any:
        return orders


class ConsumeStep(ETLStep[P]):
    orders_temp = FromTemp("orders_temp")
    target = IntoTable("staging.out").replace()

    def execute(self, params: P, *, orders_temp: Any) -> Any:
        return orders_temp


class ValidProc(ETLProcess[P]):
    steps = [ProduceStep, ConsumeStep]


class ValidPipeline(ETLPipeline[P]):
    processes = [ValidProc]


# ---------------------------------------------------------------------------
# Valid temp flow passes
# ---------------------------------------------------------------------------


def test_valid_temp_flow_passes() -> None:
    plan = ETLCompiler().compile(ValidPipeline)
    validate_plan_temps(plan)


# ---------------------------------------------------------------------------
# Forward reference (consume before produce) raises
# ---------------------------------------------------------------------------


class ConsumeFirstProc(ETLProcess[P]):
    steps = [ConsumeStep, ProduceStep]


class ConsumeFirstPipeline(ETLPipeline[P]):
    processes = [ConsumeFirstProc]


def test_forward_temp_reference_raises() -> None:
    compiler = ETLCompiler()
    with pytest.raises(ETLCompilationError, match="orders_temp") as exc_info:
        compiler.compile(ConsumeFirstPipeline)

    assert exc_info.value.code is ETLErrorCode.TEMP_NOT_PRODUCED


# ---------------------------------------------------------------------------
# Orphan temp (no IntoTemp anywhere) raises
# ---------------------------------------------------------------------------


class OrphanConsumeProc(ETLProcess[P]):
    steps = [ConsumeStep]


class OrphanConsumePipeline(ETLPipeline[P]):
    processes = [OrphanConsumeProc]


def test_orphan_temp_reference_raises() -> None:
    compiler = ETLCompiler()
    with pytest.raises(ETLCompilationError, match="orders_temp") as exc_info:
        compiler.compile(OrphanConsumePipeline)

    assert exc_info.value.code is ETLErrorCode.TEMP_NOT_PRODUCED


# ---------------------------------------------------------------------------
# Duplicate name (strict) — blocked at compile time
# ---------------------------------------------------------------------------


class ProduceStep2(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTemp("orders_temp")  # same name, no append=True

    def execute(self, params: P, *, orders: Any) -> Any:
        return orders


class DuplicateStrictProc(ETLProcess[P]):
    steps = [ProduceStep, ProduceStep2, ConsumeStep]


class DuplicateStrictPipeline(ETLPipeline[P]):
    processes = [DuplicateStrictProc]


def test_duplicate_strict_temp_raises() -> None:
    compiler = ETLCompiler()
    with pytest.raises(ETLCompilationError, match="already written"):
        compiler.compile(DuplicateStrictPipeline)


def test_duplicate_strict_temp_error_code() -> None:
    from loom.etl.compiler._errors import ETLErrorCode

    compiler = ETLCompiler()
    with pytest.raises(ETLCompilationError) as exc_info:
        compiler.compile(DuplicateStrictPipeline)
    assert exc_info.value.code == ETLErrorCode.DUPLICATE_TEMP_NAME


# ---------------------------------------------------------------------------
# Fan-in (append=True on all writers) — allowed
# ---------------------------------------------------------------------------


class AppendPart1(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTemp("parts", append=True)

    def execute(self, params: P, *, orders: Any) -> Any:
        return orders


class AppendPart2(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTemp("parts", append=True)  # same name, both append=True

    def execute(self, params: P, *, orders: Any) -> Any:
        return orders


class ConsumeParts(ETLStep[P]):
    parts = FromTemp("parts")
    target = IntoTable("staging.out").replace()

    def execute(self, params: P, *, parts: Any) -> Any:
        return parts


class FanInProc(ETLProcess[P]):
    steps = [AppendPart1, AppendPart2, ConsumeParts]


class FanInPipeline(ETLPipeline[P]):
    processes = [FanInProc]


def test_fan_in_append_passes() -> None:
    plan = ETLCompiler().compile(FanInPipeline)
    validate_plan_temps(plan)


# ---------------------------------------------------------------------------
# Mixed modes (one append=True, one append=False) — blocked
# ---------------------------------------------------------------------------


class MixedAppendStep(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTemp("orders_temp", append=True)  # mixes with ProduceStep (append=False)

    def execute(self, params: P, *, orders: Any) -> Any:
        return orders


class MixedModeProc(ETLProcess[P]):
    steps = [ProduceStep, MixedAppendStep, ConsumeStep]


class MixedModePipeline(ETLPipeline[P]):
    processes = [MixedModeProc]


def test_mixed_append_modes_raises() -> None:
    compiler = ETLCompiler()
    with pytest.raises(ETLCompilationError, match="mixes append"):
        compiler.compile(MixedModePipeline)


def test_mixed_append_modes_error_code() -> None:
    from loom.etl.compiler._errors import ETLErrorCode

    compiler = ETLCompiler()
    with pytest.raises(ETLCompilationError) as exc_info:
        compiler.compile(MixedModePipeline)
    assert exc_info.value.code == ETLErrorCode.INVALID_TEMP_APPEND_MIX
