"""A custom maintenance op runs through the runner with public imports only."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import ClassVar

from loom.etl.maintenance import (
    DeltaTableMaintainer,
    MaintainSchema,
    MaintainTable,
    MaintenanceRunner,
    MaintenanceSpec,
    MaintenanceStep,
    OperationDeclaration,
    OpSpec,
    OptimizeResult,
    VacuumSpec,
)
from loom.etl.storage import StorageConfig, TableLocation
from tests.unit.etl.maintenance.conftest import StubMaintainer


@dataclass
class _CheckpointResult(OptimizeResult):
    uri: str = ""


@dataclass(frozen=True)
class _CheckpointSpec:
    name: ClassVar[str] = "checkpoint"

    def execute(
        self,
        maintainer: DeltaTableMaintainer,
        uri: str,
        location: TableLocation,
    ) -> _CheckpointResult:
        _ = maintainer, location
        return _CheckpointResult(uri=uri)


class _Checkpointed:
    def __init__(self, inner: OperationDeclaration) -> None:
        self._inner = inner

    def resolve(self, config: StorageConfig) -> list[MaintenanceSpec]:
        return [
            replace(spec, ops=(*spec.ops, _CheckpointSpec()))
            for spec in self._inner.resolve(config)
        ]


def _runner(config: StorageConfig, maintainer: StubMaintainer) -> MaintenanceRunner:
    return MaintenanceRunner(maintainer, config.to_path_locator(), config)


def test_the_custom_op_runs_after_the_declared_ones(routed_config: StorageConfig) -> None:
    stub = StubMaintainer()

    class Nightly(MaintenanceStep[None]):
        operations = [_Checkpointed(MaintainTable("raw.events").vacuum(dry_run=False))]

    report = _runner(routed_config, stub).run(Nightly)

    (result,) = report.results
    assert list(result.op_results) == ["vacuum", "checkpoint"]
    checkpoint = result.op_results["checkpoint"]
    assert isinstance(checkpoint, _CheckpointResult)
    assert checkpoint.uri.endswith("raw/events")
    assert stub.vacuum_calls[0]["spec"] == VacuumSpec(dry_run=False)


def test_the_custom_op_reaches_every_table_of_a_schema(routed_config: StorageConfig) -> None:
    class Nightly(MaintenanceStep[None]):
        operations = [_Checkpointed(MaintainSchema("raw").compact())]

    report = _runner(routed_config, StubMaintainer()).run(Nightly)

    assert sorted(r.table_ref for r in report.results) == ["raw.events", "raw.snapshots"]
    assert all("checkpoint" in r.op_results for r in report.results)


def test_the_compiled_specs_satisfy_the_public_protocols(routed_config: StorageConfig) -> None:
    (spec,) = MaintainTable("raw.events").vacuum().resolve(routed_config)

    assert spec == MaintenanceSpec(table_ref="raw.events", ops=(VacuumSpec(),))
    assert isinstance(_Checkpointed(MaintainTable("raw.events")), OperationDeclaration)
    assert all(isinstance(op, OpSpec) for op in (*spec.ops, _CheckpointSpec()))
