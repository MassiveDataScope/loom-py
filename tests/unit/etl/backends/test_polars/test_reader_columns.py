"""Integration tests for PolarsSourceReader column projection.

Verifies that .columns() on FromTable and FromFile pushes column selection
down to the Parquet scanner — only declared columns are materialised.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from deltalake import write_deltalake

from loom.etl.backends.polars import PolarsSourceReader
from loom.etl.declarative._format import Format
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import FileSourceSpec, FromFile, FromTable, TableSourceSpec

from .conftest import table_path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _seed_delta(root: Path, ref: str, data: pl.DataFrame) -> Path:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data, mode="overwrite")
    return path


def _reader(root: Path) -> PolarsSourceReader:
    return PolarsSourceReader(str(root))


def _table_spec(ref: str, columns: tuple[str, ...] = ()) -> TableSourceSpec:
    return TableSourceSpec(alias="data", table_ref=TableRef(ref), columns=columns)


class TestReaderColumnProjection:
    # ---------------------------------------------------------------------------
    # FromTable.columns() — column projection on Delta table
    # ---------------------------------------------------------------------------

    def test_reader_no_columns_returns_all(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "amount": [10.0, 20.0]})
        _seed_delta(tmp_path, "raw.orders", data)

        spec = _table_spec("raw.orders")
        result = _reader(tmp_path).read(spec, None).collect()

        assert set(result.columns) == {"id", "name", "amount"}

    def test_reader_columns_projects_subset(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "amount": [10.0, 20.0]})
        _seed_delta(tmp_path, "raw.orders", data)

        spec = _table_spec("raw.orders", columns=("id", "amount"))
        result = _reader(tmp_path).read(spec, None).collect()

        assert set(result.columns) == {"id", "amount"}
        assert "name" not in result.columns

    def test_reader_columns_single_column(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"id": [1, 2], "value": [100, 200]})
        _seed_delta(tmp_path, "raw.metrics", data)

        spec = _table_spec("raw.metrics", columns=("id",))
        result = _reader(tmp_path).read(spec, None).collect()

        assert result.columns == ["id"]

    def test_reader_columns_preserves_row_count(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"id": [1, 2, 3], "x": [10, 20, 30], "y": [1, 2, 3]})
        _seed_delta(tmp_path, "raw.events", data)

        spec = _table_spec("raw.events", columns=("id", "x"))
        result = _reader(tmp_path).read(spec, None).collect()

        assert len(result) == 3
        assert result["id"].to_list() == [1, 2, 3]

    def test_reader_columns_combined_with_predicate(self, tmp_path: Path) -> None:
        from loom.etl.declarative.expr._predicate import EqPred
        from loom.etl.declarative.expr._refs import UnboundColumnRef

        data = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "year": [2023, 2024, 2024],
                "amount": [10.0, 20.0, 30.0],
            }
        )
        _seed_delta(tmp_path, "raw.orders", data)

        pred = EqPred(left=UnboundColumnRef("year"), right=2024)
        spec = TableSourceSpec(
            alias="orders",
            table_ref=TableRef("raw.orders"),
            predicates=(pred,),
            columns=("id", "amount"),
        )
        result = _reader(tmp_path).read(spec, None).collect()

        assert set(result.columns) == {"id", "amount"}
        assert result["id"].to_list() == [2, 3]


class TestFromTableColumns:
    # ---------------------------------------------------------------------------
    # FromTable.columns() builder — spec propagation
    # ---------------------------------------------------------------------------

    def test_from_table_columns_spec_propagation(self) -> None:
        spec = FromTable("raw.orders").columns("id", "status")._to_spec("orders")
        assert spec.columns == ("id", "status")

    def test_from_table_columns_chained_with_where(self) -> None:
        from loom.etl.declarative.expr._refs import col

        spec = (
            FromTable("raw.orders")
            .where(col("year") == 2024)
            .columns("id", "amount")
            ._to_spec("orders")
        )
        assert spec.columns == ("id", "amount")
        assert len(spec.predicates) == 1


class TestFromFileColumns:
    # ---------------------------------------------------------------------------
    # FromFile.columns() — column projection on file source
    # ---------------------------------------------------------------------------

    def test_from_file_columns_spec_stored(self, tmp_path: Path) -> None:
        spec = (
            FromFile(str(tmp_path / "data.parquet"), format=Format.PARQUET)
            .columns("order_id", "amount")
            ._to_spec("data")
        )
        assert spec.columns == ("order_id", "amount")

    def test_reader_file_columns_projects_parquet(self, tmp_path: Path) -> None:
        data = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "value": [10, 20]})
        path = tmp_path / "data.parquet"
        data.write_parquet(str(path))

        spec = FileSourceSpec(
            alias="data", path=str(path), format=Format.PARQUET, columns=("id", "value")
        )
        result = _reader(tmp_path).read(spec, None).collect()

        assert set(result.columns) == {"id", "value"}
        assert "name" not in result.columns
