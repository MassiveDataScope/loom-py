"""Delta table maintenance — backend-agnostic vacuum, compact, and Z-Order.

Public API
----------

Authoring::

    from loom.etl.maintenance import MaintenanceStep, MaintainTable, MaintainSchema

Running::

    from loom.etl.maintenance import MaintenanceRunner, MaintenanceReport

Result types::

    from loom.etl.maintenance import (
        VacuumResult,
        OptimizeResult,
        TableMaintenanceResult,
        MaintenanceError,
    )

Backend protocol (for custom implementations)::

    from loom.etl.maintenance import DeltaTableMaintainer

Operation specs (for custom operations and backends)::

    from loom.etl.maintenance import (
        MaintenanceSpec,
        OpSpec,
        OperationDeclaration,
        VacuumSpec,
        CompactSpec,
        ZOrderSpec,
    )

Example::

    class NightlyMaintenance(MaintenanceStep[None]):
        operations = [
            MaintainSchema("raw").vacuum(retention_hours=168, dry_run=False).compact(),
            MaintainTable("staging.orders")
                .vacuum(dry_run=False)
                .z_order_by(["run_date", "_id"]),
        ]

    runner = MaintenanceRunner.from_config(config)
    report = runner.run(NightlyMaintenance)
    report.raise_if_errors()

Custom operations
-----------------

An operation is any class implementing :class:`OpSpec`: a ``name`` class
variable, which keys its result in :attr:`TableMaintenanceResult.op_results`,
and an ``execute(maintainer, uri, location)`` method returning a
:class:`VacuumResult` or an :class:`OptimizeResult` (or a subclass). To run
it, wrap a declaration in an :class:`OperationDeclaration` whose ``resolve``
appends the op to each :class:`MaintenanceSpec`::

    from dataclasses import dataclass, replace
    from typing import ClassVar

    from deltalake import DeltaTable

    @dataclass(frozen=True)
    class CheckpointSpec:
        name: ClassVar[str] = "checkpoint"

        def execute(self, maintainer, uri, location) -> OptimizeResult:
            DeltaTable(uri, storage_options=location.storage_options).create_checkpoint()
            return OptimizeResult()

    class Checkpointed:
        def __init__(self, inner: OperationDeclaration) -> None:
            self._inner = inner

        def resolve(self, config: StorageConfig) -> list[MaintenanceSpec]:
            return [
                replace(spec, ops=(*spec.ops, CheckpointSpec()))
                for spec in self._inner.resolve(config)
            ]

    class NightlyMaintenance(MaintenanceStep[None]):
        operations = [Checkpointed(MaintainSchema("raw").vacuum(dry_run=False))]
"""

from loom.etl.maintenance._builder import MaintainSchema, MaintainTable
from loom.etl.maintenance._ops import CompactSpec, MaintenanceSpec, VacuumSpec, ZOrderSpec
from loom.etl.maintenance._protocol import (
    DeltaTableMaintainer,
    OperationDeclaration,
    OpSpec,
    OptimizeResult,
    TableMaintenanceResult,
    VacuumResult,
)
from loom.etl.maintenance._runner import MaintenanceError, MaintenanceReport, MaintenanceRunner
from loom.etl.maintenance._step import MaintenanceStep

__all__ = [
    # authoring
    "MaintenanceStep",
    "MaintainTable",
    "MaintainSchema",
    # runner
    "MaintenanceRunner",
    "MaintenanceReport",
    "MaintenanceError",
    # result types
    "VacuumResult",
    "OptimizeResult",
    "TableMaintenanceResult",
    # protocols
    "DeltaTableMaintainer",
    "OperationDeclaration",
    "OpSpec",
    # operation specs
    "MaintenanceSpec",
    "VacuumSpec",
    "CompactSpec",
    "ZOrderSpec",
]
