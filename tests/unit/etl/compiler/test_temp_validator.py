"""Unit tests for compiler._temp_validator — direct function coverage."""

from __future__ import annotations

from typing import Any

import pytest

from loom.etl import (
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
from tests.unit.etl.doubles import RunDateETLParams


class OrdersTempProduceStep(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoTemp("orders_temp")

    def execute(self, params: RunDateETLParams, *, orders: Any) -> Any:
        return orders


class OrdersTempConsumeStep(ETLStep[RunDateETLParams]):
    orders_temp = FromTemp("orders_temp")
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateETLParams, *, orders_temp: Any) -> Any:
        return orders_temp


class ValidProc(ETLProcess[RunDateETLParams]):
    steps = [OrdersTempProduceStep, OrdersTempConsumeStep]


class ValidPipeline(ETLPipeline[RunDateETLParams]):
    processes = [ValidProc]


class TestValidTempFlow:
    # ---------------------------------------------------------------------------
    # Valid temp flow passes
    # ---------------------------------------------------------------------------

    def test_valid_temp_flow_passes(self) -> None:
        plan = ETLCompiler().compile(ValidPipeline)
        validate_plan_temps(plan)


# ---------------------------------------------------------------------------
# Forward reference (consume before produce) raises
# ---------------------------------------------------------------------------


class ConsumeFirstProc(ETLProcess[RunDateETLParams]):
    steps = [OrdersTempConsumeStep, OrdersTempProduceStep]


class ConsumeFirstPipeline(ETLPipeline[RunDateETLParams]):
    processes = [ConsumeFirstProc]


class TestForwardTempReference:
    def test_forward_temp_reference_raises(self) -> None:
        compiler = ETLCompiler()
        with pytest.raises(ETLCompilationError, match="orders_temp") as exc_info:
            compiler.compile(ConsumeFirstPipeline)

        assert exc_info.value.code is ETLErrorCode.TEMP_NOT_PRODUCED


# ---------------------------------------------------------------------------
# Orphan temp (no IntoTemp anywhere) raises
# ---------------------------------------------------------------------------


class OrphanConsumeProc(ETLProcess[RunDateETLParams]):
    steps = [OrdersTempConsumeStep]


class OrphanConsumePipeline(ETLPipeline[RunDateETLParams]):
    processes = [OrphanConsumeProc]


class TestOrphanTempReference:
    def test_orphan_temp_reference_raises(self) -> None:
        compiler = ETLCompiler()
        with pytest.raises(ETLCompilationError, match="orders_temp") as exc_info:
            compiler.compile(OrphanConsumePipeline)

        assert exc_info.value.code is ETLErrorCode.TEMP_NOT_PRODUCED


# ---------------------------------------------------------------------------
# Duplicate name (strict) — blocked at compile time
# ---------------------------------------------------------------------------


class ProduceStep2(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoTemp("orders_temp")  # same name, no append=True

    def execute(self, params: RunDateETLParams, *, orders: Any) -> Any:
        return orders


class DuplicateStrictProc(ETLProcess[RunDateETLParams]):
    steps = [OrdersTempProduceStep, ProduceStep2, OrdersTempConsumeStep]


class DuplicateStrictPipeline(ETLPipeline[RunDateETLParams]):
    processes = [DuplicateStrictProc]


class TestDuplicateStrictTemp:
    def test_duplicate_strict_temp_raises(self) -> None:
        compiler = ETLCompiler()
        with pytest.raises(ETLCompilationError, match="already written"):
            compiler.compile(DuplicateStrictPipeline)

    def test_duplicate_strict_temp_error_code(self) -> None:
        from loom.etl.compiler._errors import ETLErrorCode

        compiler = ETLCompiler()
        with pytest.raises(ETLCompilationError) as exc_info:
            compiler.compile(DuplicateStrictPipeline)
        assert exc_info.value.code == ETLErrorCode.DUPLICATE_TEMP_NAME


# ---------------------------------------------------------------------------
# Fan-in (append=True on all writers) — allowed
# ---------------------------------------------------------------------------


class AppendPart1(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoTemp("parts", append=True)

    def execute(self, params: RunDateETLParams, *, orders: Any) -> Any:
        return orders


class AppendPart2(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoTemp("parts", append=True)  # same name, both append=True

    def execute(self, params: RunDateETLParams, *, orders: Any) -> Any:
        return orders


class ConsumeParts(ETLStep[RunDateETLParams]):
    parts = FromTemp("parts")
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateETLParams, *, parts: Any) -> Any:
        return parts


class FanInProc(ETLProcess[RunDateETLParams]):
    steps = [AppendPart1, AppendPart2, ConsumeParts]


class FanInPipeline(ETLPipeline[RunDateETLParams]):
    processes = [FanInProc]


class TestFanInAppend:
    def test_fan_in_append_passes(self) -> None:
        plan = ETLCompiler().compile(FanInPipeline)
        validate_plan_temps(plan)


# ---------------------------------------------------------------------------
# Mixed modes (one append=True, one append=False) — blocked
# ---------------------------------------------------------------------------


class MixedAppendStep(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoTemp("orders_temp", append=True)  # mixes with OrdersTempProduceStep (append=False)

    def execute(self, params: RunDateETLParams, *, orders: Any) -> Any:
        return orders


class MixedModeProc(ETLProcess[RunDateETLParams]):
    steps = [OrdersTempProduceStep, MixedAppendStep, OrdersTempConsumeStep]


class MixedModePipeline(ETLPipeline[RunDateETLParams]):
    processes = [MixedModeProc]


class TestMixedAppendModes:
    def test_mixed_append_modes_raises(self) -> None:
        compiler = ETLCompiler()
        with pytest.raises(ETLCompilationError, match="mixes append"):
            compiler.compile(MixedModePipeline)

    def test_mixed_append_modes_error_code(self) -> None:
        from loom.etl.compiler._errors import ETLErrorCode

        compiler = ETLCompiler()
        with pytest.raises(ETLCompilationError) as exc_info:
            compiler.compile(MixedModePipeline)
        assert exc_info.value.code == ETLErrorCode.INVALID_TEMP_APPEND_MIX
