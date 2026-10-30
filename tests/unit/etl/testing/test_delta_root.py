"""PolarsStepRunner with ``delta_root=``: real Delta reads and writes on a local root."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("deltalake")

from deltalake import DeltaTable  # noqa: E402 — import after importorskip guard

from loom.etl import (  # noqa: E402
    ETLParams,
    ETLStep,
    FromTable,
    IntoTable,
    Sources,
    col,
    params,
)
from loom.etl.schema._schema import LoomDtype, SchemaError  # noqa: E402
from loom.etl.testing import ETLScenario, MergeMetrics, PolarsStepRunner  # noqa: E402


class P(ETLParams):  # type: ignore[misc]
    year: int = 2024


class UpsertOrders(ETLStep[P]):
    sources = Sources(cdc=FromTable("raw.cdc"))
    target = IntoTable("silver.orders").upsert(keys=("id",), delete_flag="_deleted")

    def execute(self, params: P, *, cdc: Any) -> Any:  # type: ignore[override]
        return cdc


class BuildSales(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.sales"))
    target = IntoTable("silver.sales").replace_partitions("year")

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw


class FixYear(ETLStep[P]):
    sources = Sources(fix=FromTable("raw.sales_fix").where(col("year") == params.year))
    target = IntoTable("silver.sales").replace_where(col("year") == params.year)

    def execute(self, params: P, *, fix: Any) -> Any:  # type: ignore[override]
        return fix


class ReplaceSales(ETLStep[P]):
    sources = Sources(fix=FromTable("raw.sales_fix"))
    target = IntoTable("silver.sales").replace()

    def execute(self, params: P, *, fix: Any) -> Any:  # type: ignore[override]
        return fix


class UpdateOrders(ETLStep[P]):
    sources = Sources(cdc=FromTable("raw.cdc"))
    target = IntoTable("silver.orders").update(keys=("id",))

    def execute(self, params: P, *, cdc: Any) -> Any:  # type: ignore[override]
        return cdc.drop("_deleted")


class EagerReplace(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.sales"))
    target = IntoTable("silver.sales").replace()

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw.collect()


class AppendEvent(ETLStep[P]):
    sources = Sources(raw=FromTable("raw.events"))
    target = IntoTable("silver.events").append()

    def execute(self, params: P, *, raw: Any) -> Any:  # type: ignore[override]
        return raw


_ORDER_SCHEMA = {"id": LoomDtype.INT64, "status": LoomDtype.UTF8}
SILVER_ORDERS = ETLScenario().with_table(
    "silver.orders", [(1, "open"), (2, "open")], ["id", "status"], schema=_ORDER_SCHEMA
)
_CDC_SCHEMA = {**_ORDER_SCHEMA, "_deleted": LoomDtype.BOOLEAN}
RAW_CDC = ETLScenario().with_table(
    "raw.cdc", [(2, "closed", True)], ["id", "status", "_deleted"], schema=_CDC_SCHEMA
)
_SALE_SCHEMA = {"year": LoomDtype.INT64, "amount": LoomDtype.FLOAT64}
SALES = ETLScenario().with_table(
    "raw.sales", [(2023, 1.0), (2024, 2.0)], ["year", "amount"], schema=_SALE_SCHEMA
)
SALES_FIX = ETLScenario().with_table(
    "raw.sales_fix", [(2023, 10.0), (2024, 20.0)], ["year", "amount"], schema=_SALE_SCHEMA
)


@pytest.fixture
def delta_runner(tmp_path: Path) -> PolarsStepRunner:
    return PolarsStepRunner(SILVER_ORDERS + RAW_CDC, delta_root=tmp_path)


# 10 --------------------------------------------------------------------------
class TestUpsertWithDeleteFlag:
    def test_tombstone_deletes_the_row_and_a_second_run_is_idempotent(
        self, delta_runner: PolarsStepRunner
    ) -> None:
        first = delta_runner.run(UpsertOrders, P())
        after_first = delta_runner.table("silver.orders").to_polars().sort("id")

        second = delta_runner.run(UpsertOrders, P())
        after_second = delta_runner.table("silver.orders").to_polars().sort("id")

        assert first.deletes == 1
        assert first.merged.deleted == 1
        assert second.merged == MergeMetrics(inserted=0, updated=0, deleted=0)
        assert after_first.to_dict(as_series=False) == {"id": [1], "status": ["open"]}
        assert "_deleted" not in after_first.columns
        assert after_second.equals(after_first)

    def test_merged_is_unavailable_for_a_replace(self, tmp_path: Path) -> None:
        runner = PolarsStepRunner(SALES, delta_root=tmp_path)

        result = runner.run(BuildSales, P())

        with pytest.raises(RuntimeError, match=r"table\(ref\)\.assert_count"):
            _ = result.merged

    def test_merged_counts_inserted_and_updated_rows(self, tmp_path: Path) -> None:
        cdc = ETLScenario().with_table(
            "raw.cdc",
            [(1, "closed", False), (3, "new", None)],
            ["id", "status", "_deleted"],
            schema=_CDC_SCHEMA,
        )
        runner = PolarsStepRunner(SILVER_ORDERS + cdc, delta_root=tmp_path)

        result = runner.run(UpsertOrders, P())

        assert result.merged == MergeMetrics(inserted=1, updated=1, deleted=0)
        assert result.deletes == 0

    def test_merged_reports_an_update_target(self, tmp_path: Path) -> None:
        cdc = ETLScenario().with_table(
            "raw.cdc",
            [(1, "closed", False), (3, "new", False)],
            ["id", "status", "_deleted"],
            schema=_CDC_SCHEMA,
        )
        runner = PolarsStepRunner(SILVER_ORDERS + cdc, delta_root=tmp_path)

        result = runner.run(UpdateOrders, P())

        assert result.merged == MergeMetrics(inserted=0, updated=1, deleted=0)

    def test_merged_is_unavailable_when_the_upsert_creates_the_table(self, tmp_path: Path) -> None:
        runner = PolarsStepRunner(RAW_CDC, delta_root=tmp_path)

        result = runner.run(UpsertOrders, P())

        with pytest.raises(RuntimeError, match="created by this write"):
            _ = result.merged

    def test_untyped_seed_column_asks_for_schema(self, tmp_path: Path) -> None:
        runner = PolarsStepRunner(delta_root=tmp_path)

        with pytest.raises(SchemaError, match="schema="):
            runner.seed("raw.cdc", [(1, None)], ["id", "status"])
        with pytest.raises(SchemaError, match="schema="):
            runner.seed("raw.cdc", [], ["id"])


# 11 --------------------------------------------------------------------------
class TestReplaceOnPartitionedTable:
    def test_replace_where_rewrites_one_partition_and_replace_rewrites_all(
        self, tmp_path: Path
    ) -> None:
        runner = PolarsStepRunner(SALES + SALES_FIX, delta_root=tmp_path)
        runner.run(BuildSales, P())
        table_uri = tmp_path / "silver" / "sales"
        assert DeltaTable(str(table_uri)).metadata().partition_columns == ["year"]

        runner.run(FixYear, P(year=2024))
        fixed = runner.table("silver.sales").to_polars().sort("year")

        runner.run(ReplaceSales, P())
        replaced = runner.table("silver.sales").to_polars().sort("year")

        assert fixed.to_dict(as_series=False) == {"year": [2023, 2024], "amount": [1.0, 20.0]}
        assert replaced.to_dict(as_series=False) == {
            "year": [2023, 2024],
            "amount": [10.0, 20.0],
        }
        assert runner.resolved_predicate is None


# 12 --------------------------------------------------------------------------
class TestRealWriterContract:
    def test_eager_frame_into_a_table_fails_like_production(self, tmp_path: Path) -> None:
        runner = PolarsStepRunner(SALES, delta_root=tmp_path)

        with pytest.raises(AttributeError, match="collect"):
            runner.run(EagerReplace, P())


class TestIsolation:
    @pytest.mark.parametrize("event_id", [1, 2])
    def test_function_scoped_roots_do_not_share_writes(self, tmp_path: Path, event_id: int) -> None:
        runner = PolarsStepRunner(delta_root=tmp_path).seed(
            "raw.events", [(event_id,)], ["id"], schema={"id": LoomDtype.INT64}
        )

        runner.run(AppendEvent, P())

        frame = runner.table("silver.events").to_polars()
        assert frame["id"].to_list() == [event_id]
