"""Integration tests for PolarsTargetWriter UPSERT/MERGE.

Requires polars and deltalake.  The module is skipped automatically when
either is absent (see conftest.py).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import SchemaMode
from loom.etl.declarative.target._table import UpsertSpec
from loom.etl.storage._config import MissingTablePolicy

from .doubles import make_polars_writer, read_delta_table, seed_delta_table

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _upsert_spec(
    ref: str,
    keys: tuple[str, ...],
    partition_cols: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    include: tuple[str, ...] = (),
    schema_mode: SchemaMode = SchemaMode.STRICT,
) -> UpsertSpec:
    return UpsertSpec(
        table_ref=TableRef(ref),
        declared_keys=keys,
        partition_cols=partition_cols,
        upsert_exclude=exclude,
        upsert_include=include,
        schema_mode=schema_mode,
    )


class TestUpsertCreatesTheTable:
    # ---------------------------------------------------------------------------
    # First run: table does not exist → overwrite creates it
    # ---------------------------------------------------------------------------

    def test_upsert_first_run_creates_table(self, tmp_path: Path) -> None:
        writer = make_polars_writer(tmp_path, missing_table_policy=MissingTablePolicy.CREATE)
        frame = pl.LazyFrame({"id": [1, 2], "name": ["alice", "bob"]})
        spec = _upsert_spec("test.orders", keys=("id",))

        writer.write(frame, spec, None)

        result = read_delta_table(tmp_path, "test.orders")
        assert sorted(result["id"].to_list()) == [1, 2]


class TestUpsertUpdatesMatchedRows:
    # ---------------------------------------------------------------------------
    # Subsequent run: existing rows updated, new rows inserted
    # ---------------------------------------------------------------------------

    def test_upsert_updates_matched_rows(self, tmp_path: Path) -> None:
        initial = pl.DataFrame({"id": [1, 2], "status": ["pending", "pending"]})
        seed_delta_table(tmp_path, "test.orders", initial)
        writer = make_polars_writer(tmp_path)

        batch = pl.LazyFrame({"id": [2, 3], "status": ["done", "new"]})
        spec = _upsert_spec("test.orders", keys=("id",))
        writer.write(batch, spec, None)

        result = read_delta_table(tmp_path, "test.orders").sort("id")
        assert result["id"].to_list() == [1, 2, 3]
        assert result["status"].to_list() == ["pending", "done", "new"]


class TestUpsertExclude:
    # ---------------------------------------------------------------------------
    # exclude= keeps specified columns unchanged on match
    # ---------------------------------------------------------------------------

    def test_upsert_exclude_preserves_column(self, tmp_path: Path) -> None:
        initial = pl.DataFrame(
            {
                "id": [1],
                "status": ["pending"],
                "created_at": ["2024-01-01"],
            }
        )
        seed_delta_table(tmp_path, "test.orders", initial)
        writer = make_polars_writer(tmp_path)

        batch = pl.LazyFrame(
            {
                "id": [1],
                "status": ["done"],
                "created_at": ["9999-99-99"],
            }
        )
        spec = _upsert_spec("test.orders", keys=("id",), exclude=("created_at",))
        writer.write(batch, spec, None)

        result = read_delta_table(tmp_path, "test.orders")
        assert result["status"].to_list() == ["done"]
        assert result["created_at"].to_list() == ["2024-01-01"]


class TestUpsertInclude:
    # ---------------------------------------------------------------------------
    # include= updates only the listed columns on match
    # ---------------------------------------------------------------------------

    def test_upsert_include_updates_only_listed_columns(self, tmp_path: Path) -> None:
        initial = pl.DataFrame(
            {
                "id": [1],
                "status": ["pending"],
                "notes": ["original"],
            }
        )
        seed_delta_table(tmp_path, "test.orders", initial)
        writer = make_polars_writer(tmp_path)

        batch = pl.LazyFrame(
            {
                "id": [1],
                "status": ["done"],
                "notes": ["should not change"],
            }
        )
        spec = _upsert_spec("test.orders", keys=("id",), include=("status",))
        writer.write(batch, spec, None)

        result = read_delta_table(tmp_path, "test.orders")
        assert result["status"].to_list() == ["done"]
        assert result["notes"].to_list() == ["original"]


class TestUpsertAcrossPartitions:
    # ---------------------------------------------------------------------------
    # Multi-partition batch: only touched partitions are merged
    # ---------------------------------------------------------------------------

    def test_upsert_multi_partition_merges_correctly(self, tmp_path: Path) -> None:
        initial = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "year": [2023, 2023, 2024],
                "value": [10, 20, 30],
            }
        )
        seed_delta_table(tmp_path, "test.events", initial)
        writer = make_polars_writer(tmp_path)

        # Batch touches year=2023 (update id=1) and year=2025 (insert id=4)
        batch = pl.LazyFrame(
            {
                "id": [1, 4],
                "year": [2023, 2025],
                "value": [99, 40],
            }
        )
        spec = _upsert_spec("test.events", keys=("id",), partition_cols=("year",))
        writer.write(batch, spec, None)

        result = read_delta_table(tmp_path, "test.events").sort("id")
        assert result["id"].to_list() == [1, 2, 3, 4]
        assert result["value"].to_list() == [99, 20, 30, 40]


class TestUpsertInsertsNewRows:
    # ---------------------------------------------------------------------------
    # Upsert inserts new rows without affecting existing ones
    # ---------------------------------------------------------------------------

    def test_upsert_inserts_new_rows_without_affecting_existing(self, tmp_path: Path) -> None:
        initial = pl.DataFrame({"id": [1], "name": ["alice"]})
        seed_delta_table(tmp_path, "test.users", initial)
        writer = make_polars_writer(tmp_path)

        batch = pl.LazyFrame({"id": [2], "name": ["bob"]})
        spec = _upsert_spec("test.users", keys=("id",))
        writer.write(batch, spec, None)

        result = read_delta_table(tmp_path, "test.users")
        assert sorted(result["id"].to_list()) == [1, 2]
        assert set(result.columns) == {"id", "name"}
