"""Integration tests for ETLExecutor.run_step with real Polars + Delta I/O.

All tests use temporary Delta directories created by the ``delta_root``
fixture and cleaned up automatically by pytest's ``tmp_path`` teardown.

Requires ``polars>=1.0`` and ``deltalake>=0.25``.  The module is skipped
automatically when either package is absent (enforced by ``conftest.py``).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from loom.core.errors import InvalidValue
from loom.core.observability.observability import Observability
from loom.etl import ETLStep, FromTable, IntoTable
from loom.etl.compiler import ETLCompiler
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.engines.polars import PolarsSourceReader
from loom.etl.executor import ETLExecutor
from loom.etl.lineage._records import EventName, RunStatus
from loom.etl.schema._schema import LoomDtype
from loom.etl.testing import RecordingRunObserver
from tests.unit.etl.engines.doubles import NoParams
from tests.unit.etl.engines.frame_types import Buyer

from .conftest import MinimalPolarsSourceReader, MinimalPolarsTargetWriter, table_path


def _read_delta(root: Path, ref: str) -> pl.DataFrame:
    """Read a Delta table through Polars native Delta scan."""
    path = table_path(root, TableRef(ref))
    return pl.scan_delta(str(path)).collect()


class DoubleAmountStep(ETLStep[NoParams]):
    """Reads raw.orders and writes staging.orders with doubled amounts."""

    orders: FromTable = FromTable("raw.orders")  # type: ignore[assignment]
    target = IntoTable("staging.orders").replace()

    def execute(self, params: NoParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return orders.with_columns((pl.col("amount") * 2).alias("amount"))


class PassThroughStep(ETLStep[NoParams]):
    """Copies raw.events to staging.events unchanged."""

    events: FromTable = FromTable("raw.events")  # type: ignore[assignment]
    target = IntoTable("staging.events").replace()

    def execute(self, params: NoParams, *, events: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return events


class DecodeBuyerStep(ETLStep[NoParams]):
    """Reads the JSON column raw.customers.buyer into a Buyer struct."""

    customers: FromTable = FromTable("raw.customers").parse_json("buyer", Buyer)  # type: ignore[assignment]
    target = IntoTable("staging.customers").replace()

    def execute(self, params: NoParams, *, customers: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return customers


class AppendStep(ETLStep[NoParams]):
    """Appends raw.deltas into staging.ledger."""

    deltas: FromTable = FromTable("raw.deltas")  # type: ignore[assignment]
    target = IntoTable("staging.ledger").append()

    def execute(self, params: NoParams, *, deltas: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return deltas


class TestRunStep:
    def test_run_step_writes_transformed_data(
        self,
        seed_table,
        polars_reader: MinimalPolarsSourceReader,
        polars_writer: MinimalPolarsTargetWriter,
        delta_root,
    ) -> None:
        """execute() result lands in the target Delta table."""
        seed_table("raw.orders", pl.DataFrame({"id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]}))

        plan = ETLCompiler().compile_step(DoubleAmountStep)
        ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

        result = _read_delta(delta_root, "staging.orders")
        assert result["amount"].to_list() == pytest.approx([20.0, 40.0, 60.0])
        assert result["id"].to_list() == [1, 2, 3]

    def test_run_step_row_count_preserved(
        self,
        seed_table,
        polars_reader: MinimalPolarsSourceReader,
        polars_writer: MinimalPolarsTargetWriter,
        delta_root,
    ) -> None:
        """Target table has the same number of rows as the source."""
        seed_table("raw.events", pl.DataFrame({"ts": [1, 2, 3, 4, 5], "val": [0] * 5}))

        plan = ETLCompiler().compile_step(PassThroughStep)
        ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

        result = _read_delta(delta_root, "staging.events")
        assert len(result) == 5

    def test_run_step_replace_overwrites_existing_target(
        self,
        seed_table,
        polars_reader: MinimalPolarsSourceReader,
        polars_writer: MinimalPolarsTargetWriter,
        delta_root,
    ) -> None:
        """REPLACE mode overwrites any prior content in the target table."""
        seed_table("raw.orders", pl.DataFrame({"id": [1], "amount": [5.0]}))
        # Pre-populate the target with stale data
        seed_table("staging.orders", pl.DataFrame({"id": [99], "amount": [999.0]}))

        plan = ETLCompiler().compile_step(DoubleAmountStep)
        ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

        result = _read_delta(delta_root, "staging.orders")
        assert result["id"].to_list() == [1]
        assert result["amount"].to_list() == pytest.approx([10.0])

    def test_run_step_append_adds_rows(
        self,
        seed_table,
        polars_reader: MinimalPolarsSourceReader,
        polars_writer: MinimalPolarsTargetWriter,
        delta_root,
    ) -> None:
        """APPEND mode grows the target table on each write."""
        seed_table("raw.deltas", pl.DataFrame({"id": [1, 2], "v": [10, 20]}))
        # Pre-populate the ledger with existing rows
        seed_table("staging.ledger", pl.DataFrame({"id": [0], "v": [0]}))

        plan = ETLCompiler().compile_step(AppendStep)
        ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

        result = _read_delta(delta_root, "staging.ledger")
        assert len(result) == 3  # 1 existing + 2 appended

    def test_run_step_emits_start_and_end_events(
        self,
        seed_table,
        polars_reader: MinimalPolarsSourceReader,
        polars_writer: MinimalPolarsTargetWriter,
    ) -> None:
        """Observer receives step_start then step_end(success) on a clean run."""
        seed_table("raw.orders", pl.DataFrame({"id": [1], "amount": [1.0]}))
        observer = RecordingRunObserver()

        plan = ETLCompiler().compile_step(DoubleAmountStep)
        ETLExecutor(
            polars_reader,
            polars_writer,
            observability=Observability([observer]),
        ).run_step(plan, NoParams())

        assert observer.event_names == [EventName.STEP_START, EventName.STEP_END]
        assert observer.step_statuses == ["success"]

    def test_run_step_emits_error_event_on_failure(
        self,
        delta_root,
        polars_writer: MinimalPolarsTargetWriter,
    ) -> None:
        """Observer receives step_error + step_end(failed) when a step raises."""

        # Reader that always raises
        class FailingReader:
            def read(self, spec, params_instance):  # type: ignore[override]
                raise RuntimeError("read failure")

        observer = RecordingRunObserver()
        plan = ETLCompiler().compile_step(DoubleAmountStep)

        executor = ETLExecutor(
            FailingReader(),
            polars_writer,
            observability=Observability([observer]),
        )
        params = NoParams()
        with pytest.raises(RuntimeError, match="read failure"):
            executor.run_step(plan, params)

        assert EventName.STEP_ERROR in observer.event_names
        assert observer.step_statuses == [RunStatus.FAILED]


class TestJsonDecodeFailure:
    @pytest.mark.parametrize(
        "document", ["not json", "[1, 2]"], ids=["malformed", "shape-mismatch"]
    )
    def test_a_document_that_does_not_decode_names_the_declared_column(
        self,
        seed_table,
        delta_root,
        polars_writer: MinimalPolarsTargetWriter,
        document: str,
    ) -> None:
        seed_table("raw.customers", pl.DataFrame({"buyer": [document]}))
        plan = ETLCompiler().compile_step(DecodeBuyerStep)

        with pytest.raises(InvalidValue, match=r"customers\.buyer as StructType"):
            ETLExecutor(PolarsSourceReader(delta_root), polars_writer).run_step(plan, NoParams())


class StreamingReplaceStep(ETLStep[NoParams]):
    """Copies raw.stream_src to staging.stream_dst with streaming enabled."""

    streaming = True

    src: FromTable = FromTable("raw.stream_src")  # type: ignore[assignment]
    target = IntoTable("staging.stream_dst").replace()

    def execute(self, params: NoParams, *, src: pl.LazyFrame) -> pl.LazyFrame:  # type: ignore[override]
        return src


class TestStreamingStep:
    def test_streaming_step_writes_correct_data(
        self,
        seed_table,
        polars_reader: MinimalPolarsSourceReader,
        polars_writer: MinimalPolarsTargetWriter,
        delta_root,
    ) -> None:
        """A step with streaming=True produces the same result as a non-streaming step."""
        seed_table("raw.stream_src", pl.DataFrame({"id": [1, 2, 3], "v": [10, 20, 30]}))

        plan = ETLCompiler().compile_step(StreamingReplaceStep)
        assert plan.streaming is True

        ETLExecutor(polars_reader, polars_writer).run_step(plan, NoParams())

        result = _read_delta(delta_root, "staging.stream_dst")
        assert result["id"].to_list() == [1, 2, 3]
        assert result["v"].to_list() == [10, 20, 30]

    def test_streaming_flag_is_false_by_default(self) -> None:
        """ETLStep.streaming defaults to False; plan carries it correctly."""
        plan = ETLCompiler().compile_step(DoubleAmountStep)
        assert plan.streaming is False


class TestSeedTableFixture:
    def test_seed_table_registers_schema_in_catalog(
        self,
        seed_table,
        delta_catalog,
    ) -> None:
        """seed_table() registers the correct LoomDtype schema in the catalog."""
        seed_table("raw.orders", pl.DataFrame({"id": [1], "amount": [1.0]}))
        schema = delta_catalog.schema(TableRef("raw.orders"))

        assert schema is not None
        assert schema[0].name == "id"
        assert schema[0].dtype is LoomDtype.INT64
        assert schema[1].name == "amount"
        assert schema[1].dtype is LoomDtype.FLOAT64
