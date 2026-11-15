from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("polars")
pytest.importorskip("deltalake")

import polars as pl
from deltalake import write_deltalake

from loom.etl import (
    ETLParams,
    ETLPipeline,
    ETLProcess,
    ETLRunner,
    ETLStep,
    Format,
    FromFile,
    FromTable,
    IntoFile,
    IntoTable,
    SchemaMode,
)


def _table_path(root: Path, ref: str) -> Path:
    return root.joinpath(*ref.split("."))


def _seed_table(root: Path, ref: str, frame: pl.DataFrame) -> None:
    path = _table_path(root, ref)
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), frame, mode="overwrite")


def _read_table(root: Path, ref: str) -> pl.DataFrame:
    return pl.scan_delta(str(_table_path(root, ref))).collect()


def _fresh_runner_cls() -> type[ETLRunner]:
    """Return ETLRunner class from the currently imported runtime snapshot."""
    return ETLRunner


class RunParams(ETLParams, frozen=True):
    run_date: date


class FilterOrdersStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.orders").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return orders.filter(pl.col("year") == params.run_date.year).select("id", "year", "amount")


class SummarizeOrdersStep(ETLStep[RunParams]):
    orders = FromTable("staging.orders")
    target = IntoTable("mart.daily_orders").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return orders.select(
            pl.len().alias("row_count"),
            pl.col("amount").sum().alias("total_amount"),
        )


class DailyProcess(ETLProcess[RunParams]):
    steps = [FilterOrdersStep, SummarizeOrdersStep]


class DailyPipeline(ETLPipeline[RunParams]):
    processes = [DailyProcess]


class CopyOrdersStep(ETLStep[RunParams]):
    orders = FromTable("raw.orders")
    target = IntoTable("staging.orders_only").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return orders


class CopyCustomersStep(ETLStep[RunParams]):
    customers = FromTable("raw.customers")
    target = IntoTable("staging.customers_only").replace(schema=SchemaMode.OVERWRITE)

    def execute(self, params: RunParams, *, customers: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return customers


class IncludeProcess(ETLProcess[RunParams]):
    steps = [CopyOrdersStep, CopyCustomersStep]


class IncludePipeline(ETLPipeline[RunParams]):
    processes = [IncludeProcess]


class TestRunnerIntegration:
    def test_runner_from_dict_executes_end_to_end_pipeline(self, tmp_path: Path) -> None:
        root = tmp_path / "lake"
        _seed_table(
            root,
            "raw.orders",
            pl.DataFrame(
                {
                    "id": [1, 2, 3],
                    "year": [2024, 2025, 2024],
                    "amount": [10.0, 20.0, 5.5],
                }
            ),
        )

        runner = _fresh_runner_cls().from_dict(
            storage={"defaults": {"table_path": {"uri": str(root)}}},
            observability={"log": {"enabled": False}},
        )
        runner.run(DailyPipeline, RunParams(run_date=date(2024, 1, 5)))

        staging = _read_table(root, "staging.orders").sort("id")
        summary = _read_table(root, "mart.daily_orders")

        assert staging["id"].to_list() == [1, 3]
        assert staging["year"].to_list() == [2024, 2024]
        assert staging["amount"].to_list() == pytest.approx([10.0, 5.5])
        assert summary["row_count"].to_list() == [2]
        assert summary["total_amount"].to_list() == pytest.approx([15.5])

    def test_runner_from_yaml_applies_include_filter(self, tmp_path: Path) -> None:
        root = tmp_path / "lake"
        _seed_table(root, "raw.orders", pl.DataFrame({"id": [1, 2], "amount": [1.0, 2.0]}))
        _seed_table(root, "raw.customers", pl.DataFrame({"id": [7], "name": ["alice"]}))

        config_path = tmp_path / "etl.yaml"
        config_path.write_text(
            f"""
storage:
  defaults:
    table_path:
      uri: {root}
observability:
  log:
    enabled: false
""".strip(),
            encoding="utf-8",
        )

        runner = _fresh_runner_cls().from_yaml(str(config_path))
        runner.run(
            IncludePipeline, RunParams(run_date=date(2024, 1, 5)), include=["CopyOrdersStep"]
        )

        orders_target = _table_path(root, "staging.orders_only")
        customers_target = _table_path(root, "staging.customers_only")

        assert (orders_target / "_delta_log").exists()
        assert not (customers_target / "_delta_log").exists()
        assert _read_table(root, "staging.orders_only")["id"].to_list() == [1, 2]

    def test_runner_handles_json_string_source_and_final_csv_reporting(
        self, tmp_path: Path
    ) -> None:
        root = tmp_path / "lake"
        source_path = tmp_path / "events.csv"
        report_path = tmp_path / "daily_report.csv"
        pl.DataFrame(
            {
                "event_id": [1, 2, 3],
                "payload": [
                    json.dumps({"store": "es", "amount": 10.0, "items": 2}),
                    json.dumps({"store": "es", "amount": 5.5, "items": 1}),
                    json.dumps({"store": "uk", "amount": 7.0, "items": 3}),
                ],
            }
        ).write_csv(source_path)

        class PayloadContract:
            store: str
            amount: float
            items: int

        class LoadEvents(ETLStep[RunParams]):
            events = FromFile(str(source_path), format=Format.CSV).parse_json(
                "payload", PayloadContract
            )
            target = IntoTable("staging.events").replace(schema=SchemaMode.OVERWRITE)

            def execute(self, params: RunParams, *, events: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
                _ = params
                return events.select(
                    pl.col("event_id").cast(pl.Int64).alias("event_id"),
                    pl.col("payload").struct.field("store").alias("store"),
                    pl.col("payload").struct.field("amount").cast(pl.Float64).alias("amount"),
                    pl.col("payload").struct.field("items").cast(pl.Int64).alias("items"),
                )

        class BuildPrepared(ETLStep[RunParams]):
            events = FromTable("staging.events")
            target = IntoTable("prepared.store_daily").replace(schema=SchemaMode.OVERWRITE)

            def execute(self, params: RunParams, *, events: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
                _ = params
                return (
                    events.group_by("store")
                    .agg(
                        pl.col("amount").sum().alias("gross_amount"),
                        pl.col("items").sum().alias("item_count"),
                    )
                    .sort("store")
                )

        class EmitReport(ETLStep[RunParams]):
            store_daily = FromTable("prepared.store_daily")
            target = IntoFile(str(report_path), format=Format.CSV)

            def execute(self, params: RunParams, *, store_daily: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
                _ = params
                return store_daily.select("store", "gross_amount", "item_count").sort("store")

        class ReportingProcess(ETLProcess[RunParams]):
            steps = [LoadEvents, BuildPrepared, EmitReport]

        class ReportingPipeline(ETLPipeline[RunParams]):
            processes = [ReportingProcess]

        runner = _fresh_runner_cls().from_dict(
            storage={"defaults": {"table_path": {"uri": str(root)}}},
            observability={"log": {"enabled": False}},
        )
        runner.run(ReportingPipeline, RunParams(run_date=date(2024, 1, 5)))

        prepared = _read_table(root, "prepared.store_daily").sort("store")
        report = pl.read_csv(report_path).sort("store")

        assert prepared["store"].to_list() == ["es", "uk"]
        assert prepared["gross_amount"].to_list() == pytest.approx([15.5, 7.0])
        assert prepared["item_count"].to_list() == [3, 3]
        assert report["store"].to_list() == ["es", "uk"]
        assert report["gross_amount"].to_list() == pytest.approx([15.5, 7.0])
        assert report["item_count"].to_list() == [3, 3]
