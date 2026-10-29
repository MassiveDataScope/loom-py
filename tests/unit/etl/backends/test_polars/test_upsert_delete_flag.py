"""Polars Delta UPSERT with an optional delete flag.

Requires polars and deltalake. The module is skipped when either is absent
(see conftest.py).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.table import TableMerger

from loom.etl.backends.polars import PolarsTargetWriter
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import SchemaMode, UpsertDeleteFlagError
from loom.etl.declarative.target._table import UpsertSpec
from loom.etl.schema._schema import SchemaNotFoundError
from loom.etl.storage._config import MissingTablePolicy

from .conftest import table_path

_FLAG = "_deleted"
_REF = "test.orders"


def _spec(
    ref: str = _REF,
    *,
    keys: tuple[str, ...] = ("id",),
    partition_cols: tuple[str, ...] = (),
    exclude: tuple[str, ...] = (),
    include: tuple[str, ...] = (),
    schema_mode: SchemaMode = SchemaMode.STRICT,
    delete_flag: str | None = _FLAG,
) -> UpsertSpec:
    return UpsertSpec(
        table_ref=TableRef(ref),
        upsert_keys=keys,
        partition_cols=partition_cols,
        upsert_exclude=exclude,
        upsert_include=include,
        schema_mode=schema_mode,
        delete_flag=delete_flag,
    )


def _read_table(root: Path, ref: str = _REF) -> pl.DataFrame:
    path = table_path(root, TableRef(ref))
    return pl.scan_delta(str(path)).collect()


def _seed_table(
    root: Path,
    data: pl.DataFrame | pa.Table,
    ref: str = _REF,
    *,
    partition_by: list[str] | None = None,
) -> None:
    path = table_path(root, TableRef(ref))
    path.mkdir(parents=True, exist_ok=True)
    write_deltalake(str(path), data, mode="overwrite", partition_by=partition_by)


def _partition_columns(root: Path, ref: str = _REF) -> list[str]:
    return list(DeltaTable(str(table_path(root, TableRef(ref)))).metadata().partition_columns)


def _writer(
    root: Path,
    *,
    missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
) -> PolarsTargetWriter:
    return PolarsTargetWriter(str(root), missing_table_policy=missing_table_policy)


def _frame(
    ids: list[int],
    status: list[str],
    deleted: list[bool | None],
) -> pl.LazyFrame:
    return pl.DataFrame(
        {"id": ids, "status": status, _FLAG: deleted},
        schema={"id": pl.Int64, "status": pl.String, _FLAG: pl.Boolean},
    ).lazy()


def _sorted(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.sort("id")


def test_upsert_without_delete_flag_keeps_merge_clauses_and_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The MERGE calls and the table match the upsert that has no delete flag."""
    calls: list[tuple[str, dict[str, Any]]] = []

    def _wrap(name: str, original: Any) -> Any:
        def _recorded(self: Any, *args: Any, **kwargs: Any) -> Any:
            calls.append((name, kwargs))
            return original(self, *args, **kwargs)

        return _recorded

    monkeypatch.setattr(
        TableMerger,
        "when_matched_update",
        _wrap("when_matched_update", TableMerger.when_matched_update),
    )
    monkeypatch.setattr(
        TableMerger,
        "when_matched_delete",
        _wrap("when_matched_delete", TableMerger.when_matched_delete),
    )
    monkeypatch.setattr(
        TableMerger,
        "when_not_matched_insert",
        _wrap("when_not_matched_insert", TableMerger.when_not_matched_insert),
    )
    original_merge = DeltaTable.merge

    def _recorded_merge(self: DeltaTable, *args: Any, **kwargs: Any) -> Any:
        calls.append(("merge", kwargs))
        return original_merge(self, *args, **kwargs)

    monkeypatch.setattr(DeltaTable, "merge", _recorded_merge)

    _seed_table(tmp_path, pl.DataFrame({"id": [1, 2], "status": ["pending", "pending"]}))
    writer = _writer(tmp_path)
    batch = pl.LazyFrame({"id": [2, 3], "status": ["done", "new"]})
    writer.write(batch, _spec(delete_flag=None), None)

    result = _sorted(_read_table(tmp_path))
    assert result["id"].to_list() == [1, 2, 3]
    assert result["status"].to_list() == ["pending", "done", "new"]
    assert [name for name, _ in calls] == [
        "merge",
        "when_matched_update",
        "when_not_matched_insert",
    ]
    assert calls[0][1]["predicate"] == "t.id = s.id"
    assert calls[0][1]["source_alias"] == "s"
    assert calls[0][1]["target_alias"] == "t"
    assert calls[1][1] == {"updates": {"status": "s.status"}}
    assert calls[2][1] == {"updates": {"id": "s.id", "status": "s.status"}}


def test_upsert_delete_flag_inserts_an_unmatched_row(tmp_path: Path) -> None:
    _seed_table(tmp_path, pl.DataFrame({"id": [1], "status": ["keep"]}))
    _writer(tmp_path).write(
        _frame([2], ["fresh"], [False]),
        _spec(),
        None,
    )

    result = _sorted(_read_table(tmp_path))
    assert result["id"].to_list() == [1, 2]
    assert result["status"].to_list() == ["keep", "fresh"]
    assert result.columns == ["id", "status"]


def test_upsert_delete_flag_updates_a_matched_row(tmp_path: Path) -> None:
    _seed_table(tmp_path, pl.DataFrame({"id": [1], "status": ["old"]}))
    _writer(tmp_path).write(
        _frame([1], ["new"], [False]),
        _spec(),
        None,
    )

    result = _read_table(tmp_path)
    assert result["id"].to_list() == [1]
    assert result["status"].to_list() == ["new"]
    assert result.columns == ["id", "status"]


def test_upsert_delete_flag_deletes_a_matched_flagged_row(tmp_path: Path) -> None:
    _seed_table(tmp_path, pl.DataFrame({"id": [1, 2], "status": ["keep", "gone"]}))
    _writer(tmp_path).write(
        _frame([2], ["ignored"], [True]),
        _spec(),
        None,
    )

    result = _read_table(tmp_path)
    assert result["id"].to_list() == [1]
    assert result["status"].to_list() == ["keep"]


def test_upsert_delete_flag_does_not_insert_an_unmatched_flagged_row(tmp_path: Path) -> None:
    _seed_table(tmp_path, pl.DataFrame({"id": [1], "status": ["keep"]}))
    _writer(tmp_path).write(
        _frame([9], ["nope"], [True]),
        _spec(),
        None,
    )

    result = _read_table(tmp_path)
    assert result["id"].to_list() == [1]
    assert result["status"].to_list() == ["keep"]
    assert _FLAG not in result.columns


def test_upsert_delete_flag_treats_null_as_false(tmp_path: Path) -> None:
    """A null flag updates a match and inserts a non-match."""
    _seed_table(tmp_path, pl.DataFrame({"id": [1], "status": ["old"]}))
    _writer(tmp_path).write(
        _frame([1, 2], ["edited", "created"], [None, None]),
        _spec(),
        None,
    )

    result = _sorted(_read_table(tmp_path))
    assert result["id"].to_list() == [1, 2]
    assert result["status"].to_list() == ["edited", "created"]


def test_upsert_delete_flag_mixed_batch_applies_insert_update_and_delete(tmp_path: Path) -> None:
    _seed_table(
        tmp_path,
        pl.DataFrame({"id": [1, 2, 3], "status": ["old", "gone", "untouched"]}),
    )
    _writer(tmp_path).write(
        _frame(
            [1, 2, 4, 5],
            ["edited", "ignored", "created", "skipped"],
            [False, True, False, True],
        ),
        _spec(),
        None,
    )

    result = _sorted(_read_table(tmp_path))
    assert result["id"].to_list() == [1, 3, 4]
    assert result["status"].to_list() == ["edited", "untouched", "created"]
    assert result.columns == ["id", "status"]


def test_upsert_delete_flag_is_idempotent(tmp_path: Path) -> None:
    _seed_table(
        tmp_path,
        pl.DataFrame({"id": [1, 2], "status": ["old", "gone"]}),
    )
    batch = _frame([1, 2, 4], ["edited", "ignored", "created"], [False, True, False])
    spec = _spec()
    writer = _writer(tmp_path)
    writer.write(batch, spec, None)
    first = _sorted(_read_table(tmp_path))
    writer.write(batch, spec, None)
    second = _sorted(_read_table(tmp_path))

    assert first.equals(second)
    assert second["id"].to_list() == [1, 4]
    assert second["status"].to_list() == ["edited", "created"]


def test_upsert_delete_flag_first_write_omits_flagged_rows_and_the_column(tmp_path: Path) -> None:
    writer = _writer(tmp_path, missing_table_policy=MissingTablePolicy.CREATE)
    writer.write(
        _frame([1, 2], ["kept", "skipped"], [False, True]),
        _spec(),
        None,
    )

    result = _read_table(tmp_path)
    assert result["id"].to_list() == [1]
    assert result["status"].to_list() == ["kept"]
    assert result.columns == ["id", "status"]


def test_upsert_delete_flag_is_not_written_under_evolve(tmp_path: Path) -> None:
    _seed_table(tmp_path, pl.DataFrame({"id": [1], "status": ["old"]}))
    _writer(tmp_path).write(
        _frame([1], ["new"], [False]),
        _spec(schema_mode=SchemaMode.EVOLVE),
        None,
    )

    result = _read_table(tmp_path)
    assert result.columns == ["id", "status"]
    assert result["status"].to_list() == ["new"]


def test_upsert_delete_flag_first_write_with_every_row_flagged_creates_an_empty_table(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    writer = _writer(tmp_path, missing_table_policy=MissingTablePolicy.CREATE)
    with caplog.at_level(logging.WARNING):
        writer.write(_frame([1, 2], ["a", "b"], [True, True]), _spec(), None)

    result = _read_table(tmp_path)
    assert result.is_empty()
    assert result.columns == ["id", "status"]
    assert "write_produced_no_rows" in " ".join(r.getMessage() for r in caplog.records)


def test_upsert_delete_flag_first_write_keeps_rows_whose_flag_is_null(tmp_path: Path) -> None:
    writer = _writer(tmp_path, missing_table_policy=MissingTablePolicy.CREATE)
    writer.write(_frame([1, 2], ["a", "b"], [None, None]), _spec(), None)

    result = _sorted(_read_table(tmp_path))
    assert result["id"].to_list() == [1, 2]
    assert result.columns == ["id", "status"]


def test_upsert_delete_flag_first_write_partitions_the_created_table(tmp_path: Path) -> None:
    writer = _writer(tmp_path, missing_table_policy=MissingTablePolicy.CREATE)
    frame = pl.LazyFrame(
        {
            "p": ["a", "b", "b"],
            "id": [1, 2, 3],
            "status": ["x", "y", "z"],
            _FLAG: [False, True, False],
        }
    )
    writer.write(frame, _spec(keys=("p", "id"), partition_cols=("p",)), None)

    result = _sorted(_read_table(tmp_path))
    assert result["id"].to_list() == [1, 3]
    assert _FLAG not in result.columns
    assert _partition_columns(tmp_path) == ["p"]


def test_upsert_delete_flag_first_write_under_strict_refuses_to_create(tmp_path: Path) -> None:
    writer = _writer(tmp_path, missing_table_policy=MissingTablePolicy.SCHEMA_MODE)
    with pytest.raises(SchemaNotFoundError):
        writer.write(_frame([1], ["a"], [False]), _spec(schema_mode=SchemaMode.STRICT), None)

    assert not table_path(tmp_path, TableRef(_REF)).exists()


def test_upsert_delete_flag_merges_a_partitioned_table(tmp_path: Path) -> None:
    _seed_table(
        tmp_path,
        pl.DataFrame({"p": ["a", "a", "b"], "id": [1, 2, 3], "status": ["x", "y", "z"]}),
        partition_by=["p"],
    )
    frame = pl.LazyFrame(
        {
            "p": ["a", "a", "b", "b"],
            "id": [1, 2, 3, 4],
            "status": ["X", "gone", "Z", "new"],
            _FLAG: [False, True, None, False],
        }
    )
    _writer(tmp_path).write(frame, _spec(keys=("p", "id"), partition_cols=("p",)), None)

    result = _sorted(_read_table(tmp_path))
    assert result.select("p", "id", "status").rows() == [
        ("a", 1, "X"),
        ("b", 3, "Z"),
        ("b", 4, "new"),
    ]
    assert _partition_columns(tmp_path) == ["p"]


def test_upsert_delete_flag_keeps_each_flag_on_its_row_after_a_reordering_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plan whose row order changes between runs deletes exactly the flagged keys.

    Common-subplan elimination is disabled, so two evaluations of the same
    plan would come back in different orders.
    """
    original_collect = pl.LazyFrame.collect

    def _collect_without_cse(self: pl.LazyFrame, *args: Any, **kwargs: Any) -> Any:
        kwargs["optimizations"] = pl.QueryOptFlags(comm_subplan_elim=False)
        return original_collect(self, *args, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "collect", _collect_without_cse)
    ids = list(range(600))
    _seed_table(tmp_path, pl.DataFrame({"id": ids, "status": ["old"] * len(ids)}))
    names = pl.LazyFrame({"id": ids, "status": ["new"] * len(ids)})
    frame = (
        pl.LazyFrame({"id": ids})
        .map_batches(lambda batch: batch.sample(fraction=1.0, shuffle=True))
        .join(names, on="id")
        .group_by("id")
        .agg(pl.col("status").first())
        .with_columns((pl.col("id") % 3 == 0).alias(_FLAG))
        .with_columns(pl.col("id").cast(pl.Int32))
    )
    _writer(tmp_path).write(frame, _spec(), None)

    result = _sorted(_read_table(tmp_path))
    assert result["id"].to_list() == [i for i in ids if i % 3 != 0]
    assert set(result["status"].to_list()) == {"new"}


def test_upsert_delete_flag_named_like_a_target_column_is_refused(tmp_path: Path) -> None:
    initial = pl.DataFrame({"id": [1], _FLAG: [False]})
    _seed_table(tmp_path, initial)
    frame = pl.LazyFrame({"id": [1], _FLAG: [True]})
    with pytest.raises(UpsertDeleteFlagError, match="is also a column of the target table"):
        _writer(tmp_path).write(frame, _spec(), None)

    assert _read_table(tmp_path).equals(initial)


def test_upsert_delete_flag_with_a_repeated_source_key_fails_the_merge(tmp_path: Path) -> None:
    """Delta refuses a target row matched by two source rows, flagged or not."""
    initial = pl.DataFrame({"id": [1], "status": ["keep"]})
    _seed_table(tmp_path, initial)
    with pytest.raises(DeltaError, match="multiple source rows"):
        _writer(tmp_path).write(_frame([1, 1], ["x", "y"], [True, False]), _spec(), None)

    assert _read_table(tmp_path).equals(initial)


def test_upsert_delete_flag_tombstone_may_carry_nulls_in_not_null_columns(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("status", pa.string(), nullable=False),
        ]
    )
    _seed_table(tmp_path, pa.table({"id": [1, 2], "status": ["a", "b"]}, schema=schema))
    tombstone = pl.LazyFrame(
        {"id": [1, 3], "status": [None, "c"], _FLAG: [True, False]},
        schema={"id": pl.Int64, "status": pl.String, _FLAG: pl.Boolean},
    )
    _writer(tmp_path).write(tombstone, _spec(), None)

    result = _sorted(_read_table(tmp_path))
    assert result.rows() == [(2, "b"), (3, "c")]


@pytest.mark.parametrize("flag", ["", "   "])
def test_upsert_delete_flag_blank_is_refused(tmp_path: Path, flag: str) -> None:
    initial = pl.DataFrame({"id": [1], "status": ["keep"]})
    _seed_table(tmp_path, initial)
    with pytest.raises(UpsertDeleteFlagError, match="must name a source column"):
        _writer(tmp_path).write(_frame([1], ["x"], [True]), _spec(delete_flag=flag), None)

    assert _read_table(tmp_path).equals(initial)


@pytest.mark.parametrize(
    ("frame", "spec", "match"),
    [
        (
            pl.LazyFrame({"id": [1], "status": ["x"]}),
            _spec(),
            "missing from the source frame",
        ),
        (
            pl.LazyFrame({"id": [1], "status": ["x"], _FLAG: ["yes"]}),
            _spec(),
            "must be a boolean column",
        ),
        (
            pl.LazyFrame({"id": [1], "status": ["x"]}),
            _spec(keys=("id",), delete_flag="id"),
            "conflicts with keys",
        ),
        (
            _frame([1], ["x"], [False]),
            _spec(exclude=(_FLAG,)),
            "conflicts with exclude",
        ),
        (
            _frame([1], ["x"], [False]),
            _spec(include=(_FLAG, "status")),
            "conflicts with include",
        ),
        (
            _frame([1], ["x"], [True]),
            _spec(partition_cols=(_FLAG,)),
            "conflicts with partition_cols",
        ),
    ],
)
def test_upsert_delete_flag_validation_leaves_the_table_unchanged(
    tmp_path: Path,
    frame: pl.LazyFrame,
    spec: UpsertSpec,
    match: str,
) -> None:
    initial = pl.DataFrame({"id": [1], "status": ["keep"]})
    _seed_table(tmp_path, initial)
    with pytest.raises(UpsertDeleteFlagError, match=match):
        _writer(tmp_path).write(frame, spec, None)

    assert _read_table(tmp_path).equals(initial)


def test_upsert_delete_flag_aligns_a_null_typed_source_column(tmp_path: Path) -> None:
    """A source column of dtype Null is cast to the table type before the write check."""
    _seed_table(tmp_path, pl.DataFrame({"id": [1, 2], "status": ["a", "b"]}))
    frame = pl.LazyFrame({"id": [1, 3], _FLAG: [True, False]}).with_columns(status=pl.lit(None))
    _writer(tmp_path).write(frame, _spec(), None)

    result = _sorted(_read_table(tmp_path))
    assert result.rows() == [(2, "b"), (3, None)]
