"""MaintenanceStep — base class for declarative Delta table maintenance.

Mirrors the ``ETLStep`` authoring pattern (class-level declarations validated
at definition time) but is entirely separate from the data-transformation
machinery — maintenance steps have no sources, no target, and no execute().
"""

from __future__ import annotations

from typing import Any, ClassVar, Generic, TypeVar

from loom.core.errors import InvalidType
from loom.etl.maintenance._protocol import OperationDeclaration
from loom.etl.pipeline._generics import _extract_generic_arg

ParamsT = TypeVar("ParamsT")


class MaintenanceStep(Generic[ParamsT]):
    """Base class for Delta table maintenance steps.

    Subclass and declare :attr:`operations` as a list of
    :class:`~loom.etl.maintenance.MaintainTable` and/or
    :class:`~loom.etl.maintenance.MaintainSchema` instances.

    Example::

        class NightlyMaintenance(MaintenanceStep[None]):
            operations = [
                MaintainSchema("raw").vacuum(retention_hours=168, dry_run=False).compact(),
                MaintainTable("staging.orders")
                    .vacuum(dry_run=False)
                    .z_order_by(["run_date", "_id"]),
            ]

    The step is executed via :class:`~loom.etl.maintenance.MaintenanceRunner`::

        runner = MaintenanceRunner.from_config(config)
        report = runner.run(NightlyMaintenance)
        report.raise_if_errors()
    """

    operations: ClassVar[list[OperationDeclaration]] = []
    _params_type: ClassVar[type[Any] | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._params_type = _extract_params_type(cls)
        _validate_operations(cls)

    def operations_for(self, params: ParamsT) -> list[OperationDeclaration]:
        """Return the list of operations to execute for this run.

        Override to build the list dynamically from *params* — useful when
        operations are driven by Prefect deployment parameters instead of
        being hardcoded at class-definition time.

        The default implementation returns the class-level :attr:`operations`
        list (static behaviour, backwards-compatible).
        """
        _ = params
        return type(self).operations


def _extract_params_type(cls: type[Any]) -> type[Any] | None:
    """Extract the ParamsT generic arg from MaintenanceStep[ParamsT]."""
    return _extract_generic_arg(cls, MaintenanceStep)


def _validate_operations(cls: type[Any]) -> None:
    """Ensure ``operations`` is a list of OperationDeclaration implementors."""
    ops = cls.__dict__.get("operations", cls.operations)
    if not isinstance(ops, list):
        raise InvalidType(
            f"{cls.__qualname__}: 'operations' must be a list, got {type(ops).__name__}"
        )
    for idx, op in enumerate(ops):
        if not isinstance(op, OperationDeclaration):
            raise InvalidType(
                f"{cls.__qualname__}: operations[{idx}] must implement OperationDeclaration "
                f"(MaintainTable or MaintainSchema), got {type(op).__name__}"
            )
