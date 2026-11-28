from __future__ import annotations

from dataclasses import fields
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from loom.etl.declarative.expr import col, params
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import (
    AppendSpec,
    IntoTable,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpsertSpec,
)
from loom.etl.engines.polars import _writer as writer_mod
from loom.etl.engines.polars._writer import PolarsTargetWriter
from loom.etl.storage._config import MissingTablePolicy
from tests.unit.etl.engines.doubles import table_path


class TestStreamingSpecField:
    @pytest.mark.parametrize(
        "spec_cls, kwargs",
        [
            (AppendSpec, {"table_ref": TableRef("t.a")}),
            (ReplaceSpec, {"table_ref": TableRef("t.a")}),
            (
                ReplacePartitionsSpec,
                {"table_ref": TableRef("t.a"), "partition_cols": ("year",)},
            ),
        ],
    )
    def test_defaults_streaming_false(self, spec_cls: type, kwargs: dict[str, Any]) -> None:
        spec = spec_cls(**kwargs)
        assert spec.streaming is False

    def test_replace_where_defaults_streaming_false(self) -> None:
        spec = ReplaceWhereSpec(
            table_ref=TableRef("t.a"),
            replace_predicate=(col("year") == params.year),
        )
        assert spec.streaming is False

    @pytest.mark.parametrize(
        "spec_cls, kwargs",
        [
            (AppendSpec, {"table_ref": TableRef("t.a"), "streaming": True}),
            (ReplaceSpec, {"table_ref": TableRef("t.a"), "streaming": True}),
            (
                ReplacePartitionsSpec,
                {
                    "table_ref": TableRef("t.a"),
                    "partition_cols": ("year",),
                    "streaming": True,
                },
            ),
        ],
    )
    def test_accepts_streaming_true(self, spec_cls: type, kwargs: dict[str, Any]) -> None:
        spec = spec_cls(**kwargs)
        assert spec.streaming is True

    def test_upsert_spec_has_no_streaming_field(self) -> None:
        field_names = {f.name for f in fields(UpsertSpec)}
        assert "streaming" not in field_names


class TestStreamingBuilders:
    def test_append_propagates_streaming(self) -> None:
        spec = IntoTable("t.orders").append(streaming=True)._to_spec()
        assert isinstance(spec, AppendSpec)
        assert spec.streaming is True

    def test_replace_propagates_streaming(self) -> None:
        spec = IntoTable("t.orders").replace(streaming=True)._to_spec()
        assert isinstance(spec, ReplaceSpec)
        assert spec.streaming is True

    def test_replace_partitions_propagates_streaming(self) -> None:
        spec = IntoTable("t.orders").replace_partitions("year", "month", streaming=True)._to_spec()
        assert isinstance(spec, ReplacePartitionsSpec)
        assert spec.streaming is True
        assert spec.partition_cols == ("year", "month")

    def test_replace_where_propagates_streaming(self) -> None:
        spec = (
            IntoTable("t.orders")
            .replace_where(col("year") == params.year, streaming=True)
            ._to_spec()
        )
        assert isinstance(spec, ReplaceWhereSpec)
        assert spec.streaming is True

    def test_defaults_streaming_false_across_modes(self) -> None:
        for spec in (
            IntoTable("t.x").append()._to_spec(),
            IntoTable("t.x").replace()._to_spec(),
            IntoTable("t.x").replace_partitions("y")._to_spec(),
            IntoTable("t.x").replace_where(col("y") == params.y)._to_spec(),
        ):
            assert spec.streaming is False


class TestPolarsStreamingWrite:
    TABLE_REF = TableRef("staging.cdc_changes")

    @pytest.fixture
    def writer(self, delta_root: Path) -> PolarsTargetWriter:
        return PolarsTargetWriter(str(delta_root), missing_table_policy=MissingTablePolicy.CREATE)

    @pytest.fixture
    def initial_frame(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "id": [0],
                "year": [2025],
                "month": [12],
                "value": [0.0],
            }
        )

    @pytest.fixture
    def incoming_frame(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            {
                "id": [1, 2, 3, 4],
                "year": [2026, 2026, 2026, 2026],
                "month": [1, 1, 2, 2],
                "value": [10.0, 20.0, 30.0, 40.0],
            }
        )

    def test_streaming_passes_arrow_stream_to_write_deltalake(
        self,
        writer: PolarsTargetWriter,
        seed_table,
        initial_frame: pl.DataFrame,
        incoming_frame: pl.LazyFrame,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seed_table(self.TABLE_REF, initial_frame)

        captured: dict[str, Any] = {}
        original = writer_mod.write_deltalake

        def _spy(uri: Any, data: Any, **kwargs: Any) -> None:
            captured["data"] = data
            captured["kwargs"] = kwargs
            original(uri, data, **kwargs)

        monkeypatch.setattr(writer_mod, "write_deltalake", _spy)

        spec = ReplacePartitionsSpec(
            table_ref=self.TABLE_REF,
            partition_cols=("year", "month"),
            streaming=True,
        )

        writer.write(incoming_frame, spec, None, streaming=False)

        data = captured["data"]
        assert not isinstance(data, pl.DataFrame)
        assert isinstance(data, pa.RecordBatchReader) or hasattr(data, "__arrow_c_stream__")

    def test_streaming_replace_partitions_lands_rows_in_delta(
        self,
        writer: PolarsTargetWriter,
        delta_root: Path,
        seed_table,
        initial_frame: pl.DataFrame,
        incoming_frame: pl.LazyFrame,
    ) -> None:
        seed_table(self.TABLE_REF, initial_frame)

        spec = ReplacePartitionsSpec(
            table_ref=self.TABLE_REF,
            partition_cols=("year", "month"),
            streaming=True,
        )

        writer.write(incoming_frame, spec, None, streaming=False)

        out = pl.scan_delta(str(table_path(delta_root, self.TABLE_REF))).sort("id").collect()
        assert out["id"].to_list() == [0, 1, 2, 3, 4]
        assert out["year"].to_list() == [2025, 2026, 2026, 2026, 2026]
        assert out["value"].to_list() == [0.0, 10.0, 20.0, 30.0, 40.0]

    def test_non_streaming_preserves_dataframe_path(
        self,
        writer: PolarsTargetWriter,
        seed_table,
        initial_frame: pl.DataFrame,
        incoming_frame: pl.LazyFrame,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seed_table(self.TABLE_REF, initial_frame)

        captured: dict[str, Any] = {}
        original = writer_mod.write_deltalake

        def _spy(uri: Any, data: Any, **kwargs: Any) -> None:
            captured["data"] = data
            original(uri, data, **kwargs)

        monkeypatch.setattr(writer_mod, "write_deltalake", _spy)

        spec = ReplacePartitionsSpec(
            table_ref=self.TABLE_REF,
            partition_cols=("year", "month"),
            streaming=False,
        )

        writer.write(incoming_frame, spec, None, streaming=False)

        assert isinstance(captured["data"], pl.DataFrame)

    def test_streaming_replace_partitions_predicate_covers_partitions(
        self,
        writer: PolarsTargetWriter,
        seed_table,
        initial_frame: pl.DataFrame,
        incoming_frame: pl.LazyFrame,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seed_table(self.TABLE_REF, initial_frame)

        captured: dict[str, Any] = {}
        original = writer_mod.write_deltalake

        def _spy(uri: Any, data: Any, **kwargs: Any) -> None:
            captured["predicate"] = kwargs.get("predicate")
            captured["mode"] = kwargs.get("mode")
            original(uri, data, **kwargs)

        monkeypatch.setattr(writer_mod, "write_deltalake", _spy)

        spec = ReplacePartitionsSpec(
            table_ref=self.TABLE_REF,
            partition_cols=("year", "month"),
            streaming=True,
        )

        writer.write(incoming_frame, spec, None, streaming=False)

        predicate = captured["predicate"]
        assert predicate is not None
        assert "2026" in predicate
        assert "1" in predicate
        assert "2" in predicate
        assert captured["mode"] == "overwrite"
