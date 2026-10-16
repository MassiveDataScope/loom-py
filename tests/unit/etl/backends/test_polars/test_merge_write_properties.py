"""Delta MERGE writes (upsert, update) honour the route's writer and commit settings.

Requires polars and deltalake.  The module is skipped automatically when
either is absent (see conftest.py).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake

from loom.etl.backends.polars import PolarsTargetWriter
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._table import UpdateSpec, UpsertSpec
from loom.etl.storage import PrefixLocator

from .conftest import table_path

_REF = "test.orders"
_MAX_ROW_GROUP = 2
_COMMIT_METADATA = {"loom.pipeline": "orders", "loom.run_id": "run-1"}


def _seed(root: Path) -> Path:
    path = table_path(root, TableRef(_REF))
    path.mkdir(parents=True, exist_ok=True)
    initial = pl.DataFrame({"id": [1, 2, 3, 4, 5], "status": ["pending"] * 5})
    write_deltalake(str(path), initial, mode="overwrite")
    return path


def _writer(root: Path) -> PolarsTargetWriter:
    locator = PrefixLocator(
        str(root),
        writer={"compression": "ZSTD", "max_row_group_size": _MAX_ROW_GROUP},
        commit={"custom_metadata": _COMMIT_METADATA},
    )
    return PolarsTargetWriter(locator)


def _files_added_by_last_commit(path: Path, before: set[str]) -> list[Path]:
    after = set(DeltaTable(str(path)).file_uris())
    added = sorted(after - before)
    assert added, "the MERGE rewrote no file"
    return [Path(uri.removeprefix("file://")) for uri in added]


def _assert_written_with_route_settings(files: list[Path]) -> None:
    for file in files:
        metadata = pq.ParquetFile(file).metadata
        for index in range(metadata.num_row_groups):
            row_group = metadata.row_group(index)
            assert row_group.num_rows <= _MAX_ROW_GROUP
            for column in range(row_group.num_columns):
                assert row_group.column(column).compression == "ZSTD"


def _assert_commit_carries_metadata(path: Path) -> None:
    last_commit = DeltaTable(str(path)).history(1)[0]
    assert last_commit["operation"] == "MERGE"
    for key, value in _COMMIT_METADATA.items():
        assert last_commit[key] == value


@pytest.mark.parametrize(
    "spec",
    [
        pytest.param(UpsertSpec(table_ref=TableRef(_REF), upsert_keys=("id",)), id="upsert"),
        pytest.param(UpdateSpec(table_ref=TableRef(_REF), keys=("id",)), id="update"),
    ],
)
def test_merge_writes_parquet_and_commit_with_route_settings(
    tmp_path: Path, spec: UpsertSpec | UpdateSpec
) -> None:
    path = _seed(tmp_path)
    before = set(DeltaTable(str(path)).file_uris())
    batch = pl.LazyFrame({"id": [2, 6], "status": ["done", "new"]})

    _writer(tmp_path).write(batch, spec, None)

    _assert_written_with_route_settings(_files_added_by_last_commit(path, before))
    _assert_commit_carries_metadata(path)
