"""Tests for ETLCompiler catalog validation (sprint 3)."""

from __future__ import annotations

from typing import Any

import pytest

from loom.etl import ETLStep, Format, FromFile, FromTable, IntoFile, IntoTable
from loom.etl.compiler import ETLCompilationError, ETLCompiler
from loom.etl.testing import InMemoryCatalog
from tests.unit.etl.doubles import RunDateETLParams


class OrdersStep(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.orders").replace()

    def execute(self, params: RunDateETLParams, *, orders: Any) -> Any:
        return orders


class MultiSourceStep(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    customers = FromTable("raw.customers")
    target = IntoTable("staging.out").replace()

    def execute(self, params: RunDateETLParams, *, orders: Any, customers: Any) -> Any:
        return orders


class FileSourceStep(ETLStep[RunDateETLParams]):
    report = FromFile("s3://raw/report.csv", format=Format.CSV)
    target = IntoTable("staging.report").replace()

    def execute(self, params: RunDateETLParams, *, report: Any) -> Any:
        return report


class FileTargetStep(ETLStep[RunDateETLParams]):
    orders = FromTable("raw.orders")
    target = IntoFile("s3://exports/out.csv", format=Format.CSV)

    def execute(self, params: RunDateETLParams, *, orders: Any) -> Any:
        return orders


class TestCompilerCatalog:
    def test_compile_without_catalog_skips_table_validation(self) -> None:
        plan = ETLCompiler().compile_step(OrdersStep)
        assert plan.step_type is OrdersStep

    def test_compile_with_valid_catalog_succeeds(self) -> None:
        catalog = InMemoryCatalog({"raw.orders": ("id",), "staging.orders": ()})
        plan = ETLCompiler(catalog=catalog).compile_step(OrdersStep)
        assert plan.step_type is OrdersStep

    def test_compile_multi_source_all_registered(self) -> None:
        catalog = InMemoryCatalog(
            {
                "raw.orders": ("id",),
                "raw.customers": ("id",),
                "staging.out": (),
            }
        )
        plan = ETLCompiler(catalog=catalog).compile_step(MultiSourceStep)
        assert len(plan.source_bindings) == 2

    def test_file_source_not_validated_against_catalog(self) -> None:
        # Catalog has the target but NOT the file path — files are physical, not catalogued.
        catalog = InMemoryCatalog({"staging.report": ()})
        plan = ETLCompiler(catalog=catalog).compile_step(FileSourceStep)
        assert plan.step_type is FileSourceStep

    def test_file_target_not_validated_against_catalog(self) -> None:
        # Catalog has the TABLE source but not the file target — file targets have no TableRef.
        catalog = InMemoryCatalog({"raw.orders": ("id",)})
        plan = ETLCompiler(catalog=catalog).compile_step(FileTargetStep)
        assert plan.step_type is FileTargetStep

    def test_missing_source_table_raises(self) -> None:
        catalog = InMemoryCatalog({"staging.orders": ()})  # raw.orders missing
        compiler = ETLCompiler(catalog=catalog)
        with pytest.raises(ETLCompilationError, match="unknown table 'raw.orders'"):
            compiler.compile_step(OrdersStep)

    def test_missing_target_table_raises(self) -> None:
        catalog = InMemoryCatalog({"raw.orders": ("id",)})  # staging.orders missing
        compiler = ETLCompiler(catalog=catalog)
        with pytest.raises(ETLCompilationError, match="unknown table 'staging.orders'"):
            compiler.compile_step(OrdersStep)

    def test_missing_one_of_multiple_sources_raises(self) -> None:
        catalog = InMemoryCatalog(
            {
                "raw.orders": ("id",),
                # raw.customers intentionally missing
                "staging.out": (),
            }
        )
        compiler = ETLCompiler(catalog=catalog)
        with pytest.raises(ETLCompilationError, match="unknown table 'raw.customers'"):
            compiler.compile_step(MultiSourceStep)

    def test_error_message_includes_step_name(self) -> None:
        catalog = InMemoryCatalog({"staging.orders": ()})
        compiler = ETLCompiler(catalog=catalog)
        with pytest.raises(ETLCompilationError, match="OrdersStep"):
            compiler.compile_step(OrdersStep)

    def test_compiler_reuses_catalog_across_compile_calls(self) -> None:
        catalog = InMemoryCatalog(
            {
                "raw.orders": ("id",),
                "staging.orders": (),
                "raw.customers": ("id",),
                "staging.out": (),
            }
        )
        compiler = ETLCompiler(catalog=catalog)
        plan1 = compiler.compile_step(OrdersStep)
        plan2 = compiler.compile_step(MultiSourceStep)
        assert plan1.step_type is OrdersStep
        assert plan2.step_type is MultiSourceStep
