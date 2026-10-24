"""Shared fixtures for maintenance unit tests."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.model.redaction import Dsn
from loom.etl.maintenance._ops import CompactSpec, VacuumSpec, ZOrderSpec
from loom.etl.maintenance._protocol import (
    DeltaTableMaintainer,
    OptimizeResult,
    VacuumResult,
)
from loom.etl.storage._config import (
    MaintenanceConfig,
    MaintenanceVacuumConfig,
    StorageConfig,
    StorageDefaults,
    TablePathConfig,
    TableRoute,
)
from loom.etl.storage._locator import TableLocation


class StubMaintainer:
    """Records calls to each operation; optionally raises on specific table URIs."""

    def __init__(self, raise_on: set[str] | None = None) -> None:
        self.raise_on: set[str] = raise_on or set()
        self.vacuum_calls: list[dict[str, Any]] = []
        self.compact_calls: list[dict[str, Any]] = []
        self.z_order_calls: list[dict[str, Any]] = []

    def vacuum(self, uri: str, spec: VacuumSpec, location: TableLocation) -> VacuumResult:
        self.vacuum_calls.append({"uri": uri, "spec": spec, "location": location})
        if uri in self.raise_on:
            raise RuntimeError(f"stub error on vacuum uri={uri}")
        return VacuumResult(files_deleted=["old_file.parquet"])

    def compact(self, uri: str, spec: CompactSpec, location: TableLocation) -> OptimizeResult:
        self.compact_calls.append({"uri": uri, "spec": spec, "location": location})
        if uri in self.raise_on:
            raise RuntimeError(f"stub error on compact uri={uri}")
        return OptimizeResult(num_files_added=1, num_files_removed=3)

    def z_order(self, uri: str, spec: ZOrderSpec, location: TableLocation) -> OptimizeResult:
        self.z_order_calls.append({"uri": uri, "spec": spec, "location": location})
        if uri in self.raise_on:
            raise RuntimeError(f"stub error on z_order uri={uri}")
        return OptimizeResult(num_files_added=2, num_files_removed=5)


assert isinstance(StubMaintainer(), DeltaTableMaintainer), (
    "StubMaintainer must satisfy DeltaTableMaintainer protocol"
)


@pytest.fixture
def stub_maintainer() -> StubMaintainer:
    return StubMaintainer()


@pytest.fixture
def simple_config(tmp_path: Any) -> StorageConfig:
    """StorageConfig with a PrefixLocator rooted at a tmp dir."""
    return StorageConfig(
        defaults=StorageDefaults(table_path=TablePathConfig(uri=Dsn(str(tmp_path) + "/")))
    )


@pytest.fixture
def routed_config(tmp_path: Any) -> StorageConfig:
    """StorageConfig with explicit table routes under raw.* and staging.*."""
    return StorageConfig(
        defaults=StorageDefaults(table_path=TablePathConfig(uri=Dsn(str(tmp_path) + "/"))),
        tables=(
            TableRoute(
                name="raw.events", path=TablePathConfig(uri=Dsn(str(tmp_path) + "/raw/events"))
            ),
            TableRoute(
                name="raw.snapshots",
                path=TablePathConfig(uri=Dsn(str(tmp_path) + "/raw/snapshots")),
            ),
            TableRoute(
                name="staging.events",
                path=TablePathConfig(uri=Dsn(str(tmp_path) + "/staging/events")),
            ),
        ),
    )


@pytest.fixture
def maintenance_config(tmp_path: Any) -> StorageConfig:
    """StorageConfig with a maintenance block."""
    return StorageConfig(
        defaults=StorageDefaults(table_path=TablePathConfig(uri=Dsn(str(tmp_path) + "/"))),
        tables=(
            TableRoute(
                name="raw.events", path=TablePathConfig(uri=Dsn(str(tmp_path) + "/raw/events"))
            ),
            TableRoute(
                name="raw.snapshots",
                path=TablePathConfig(uri=Dsn(str(tmp_path) + "/raw/snapshots")),
            ),
            TableRoute(
                name="staging.events",
                path=TablePathConfig(uri=Dsn(str(tmp_path) + "/staging/events")),
            ),
        ),
        maintenance=MaintenanceConfig(
            schemas=("raw",),
            vacuum=MaintenanceVacuumConfig(retention_hours=168, dry_run=True),
            compact=True,
        ),
    )
