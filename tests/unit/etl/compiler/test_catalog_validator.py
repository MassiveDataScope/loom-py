"""Unit tests for compiler._catalog_validator — direct function coverage."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, ETLPipeline, ETLProcess, ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLCompiler, ETLErrorCode, validate_plan_catalog
from loom.etl.compiler._plan import PipelinePlan
from loom.etl.declarative.target import SchemaMode
from loom.etl.testing import StubCatalog


class P(ETLParams):  # type: ignore[misc]
    run_date: date


class ReplaceStepA(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.out").replace()

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class OverwriteStepA(ETLStep[P]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.out").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: P, *, orders: Any) -> Any:  # type: ignore[override]
        return orders


class StepB(ETLStep[P]):
    staging = FromTable("staging.out")
    target = IntoTable("mart.summary").replace()

    def execute(self, params: P, *, staging: Any) -> Any:  # type: ignore[override]
        return staging


class ReplaceProcess(ETLProcess[P]):
    steps = [ReplaceStepA, StepB]


class OverwriteProcess(ETLProcess[P]):
    steps = [OverwriteStepA, StepB]


class ReplacePipeline(ETLPipeline[P]):
    processes = [ReplaceProcess]


class OverwritePipeline(ETLPipeline[P]):
    processes = [OverwriteProcess]


def _plan(pipeline_type: type[ETLPipeline[P]]) -> PipelinePlan:
    return ETLCompiler().compile(pipeline_type)


class TestValidatePlanCatalogValid:
    def test_valid_catalog_passes(self) -> None:
        catalog = StubCatalog({"raw.orders": (), "staging.out": (), "mart.summary": ()})
        validate_plan_catalog(_plan(ReplacePipeline), catalog)

    def test_overwrite_target_creates_for_next_step(self) -> None:
        catalog = StubCatalog({"raw.orders": (), "mart.summary": ()})
        validate_plan_catalog(_plan(OverwritePipeline), catalog)


class TestValidatePlanCatalogErrors:
    def test_missing_source_raises(self) -> None:
        catalog = StubCatalog({"staging.out": (), "mart.summary": ()})
        plan = _plan(ReplacePipeline)
        with pytest.raises(ETLCompilationError, match="raw.orders") as exc_info:
            validate_plan_catalog(plan, catalog)

        assert exc_info.value.code is ETLErrorCode.UNKNOWN_SOURCE_TABLE

    def test_missing_non_overwrite_target_raises(self) -> None:
        catalog = StubCatalog({"raw.orders": (), "mart.summary": ()})
        plan = _plan(ReplacePipeline)
        with pytest.raises(ETLCompilationError, match="staging.out") as exc_info:
            validate_plan_catalog(plan, catalog)

        assert exc_info.value.code is ETLErrorCode.UNKNOWN_TARGET_TABLE

    def test_error_message_includes_table_name(self) -> None:
        catalog = StubCatalog({"staging.out": (), "mart.summary": ()})
        plan = _plan(ReplacePipeline)
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_plan_catalog(plan, catalog)
        message = str(exc_info.value)
        assert "source 'orders'" in message
        assert "raw.orders" in message
