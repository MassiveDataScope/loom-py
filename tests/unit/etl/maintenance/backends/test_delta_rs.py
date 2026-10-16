"""Integration tests for DeltaRsMaintainer against real local Delta tables."""

from __future__ import annotations

import os
import time
from pathlib import Path

import polars as pl
import pytest
from deltalake import write_deltalake
from deltalake.exceptions import DeltaError

from loom.etl.maintenance._ops import CompactSpec, VacuumSpec, ZOrderSpec
from loom.etl.maintenance.backends._delta_rs import DeltaRsMaintainer
from loom.etl.storage._locator import TableLocation

_RETENTION_HOURS = 168


@pytest.fixture
def delta_table_uri(tmp_path: object) -> str:
    """Create a small Delta table and return its URI."""
    uri = str(tmp_path) + "/test_table"
    df = pl.DataFrame({"id": [1, 2, 3], "date": ["2024-01-01", "2024-01-02", "2024-01-03"]})
    write_deltalake(uri, df.to_arrow(), mode="overwrite")
    return uri


@pytest.fixture
def delta_location(delta_table_uri: str) -> TableLocation:
    return TableLocation(uri=delta_table_uri)


def _orphan_older_than_retention(table_uri: str) -> Path:
    orphan = Path(table_uri) / "part-00000-orphan-from-aborted-write.parquet"
    pl.DataFrame({"id": [99]}).write_parquet(orphan)
    aged = time.time() - (_RETENTION_HOURS + 24) * 3600
    os.utime(orphan, (aged, aged))
    return orphan


class TestDeltaRsMaintainerVacuum:
    def test_vacuum_dry_run_returns_file_list(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = VacuumSpec(dry_run=True)
        result = maintainer.vacuum(delta_table_uri, spec, delta_location)
        assert isinstance(result.files_deleted, list)

    def test_vacuum_dry_run_true_does_not_delete(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = VacuumSpec(dry_run=True)
        before = set(os.listdir(delta_table_uri))
        maintainer.vacuum(delta_table_uri, spec, delta_location)
        after = set(os.listdir(delta_table_uri))
        assert before == after


class TestDeltaRsMaintainerVacuumOrphans:
    def test_full_vacuum_deletes_an_orphan_older_than_the_retention(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        orphan = _orphan_older_than_retention(delta_table_uri)
        spec = VacuumSpec(retention_hours=_RETENTION_HOURS, dry_run=False, full=True)

        result = DeltaRsMaintainer().vacuum(delta_table_uri, spec, delta_location)

        assert not orphan.exists()
        assert [Path(f).name for f in result.files_deleted] == [orphan.name]
        assert pl.read_delta(delta_table_uri).height == 3

    def test_lite_vacuum_keeps_an_orphan_older_than_the_retention(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        orphan = _orphan_older_than_retention(delta_table_uri)
        spec = VacuumSpec(retention_hours=_RETENTION_HOURS, dry_run=False)

        result = DeltaRsMaintainer().vacuum(delta_table_uri, spec, delta_location)

        assert orphan.exists()
        assert result.files_deleted == []

    def test_full_vacuum_dry_run_lists_the_orphan_without_deleting_it(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        orphan = _orphan_older_than_retention(delta_table_uri)
        spec = VacuumSpec(retention_hours=_RETENTION_HOURS, dry_run=True, full=True)

        result = DeltaRsMaintainer().vacuum(delta_table_uri, spec, delta_location)

        assert orphan.exists()
        assert [Path(f).name for f in result.files_deleted] == [orphan.name]


class TestDeltaRsMaintainerCompact:
    def test_compact_returns_optimize_result(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = CompactSpec()
        result = maintainer.compact(delta_table_uri, spec, delta_location)
        assert result.num_files_added >= 0
        assert result.num_files_removed >= 0


class TestDeltaRsMaintainerZOrder:
    def test_z_order_returns_optimize_result(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = ZOrderSpec(columns=["id"])
        result = maintainer.z_order(delta_table_uri, spec, delta_location)
        assert result.num_files_added >= 0
        assert result.num_files_removed >= 0

    def test_z_order_nonexistent_column_raises(
        self, delta_table_uri: str, delta_location: TableLocation
    ) -> None:
        maintainer = DeltaRsMaintainer()
        spec = ZOrderSpec(columns=["nonexistent_col"])
        with pytest.raises(DeltaError):
            maintainer.z_order(delta_table_uri, spec, delta_location)
