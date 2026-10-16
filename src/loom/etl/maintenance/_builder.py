"""Fluent builder API for declaring Delta table maintenance operations.

Users subclass :class:`MaintenanceStep` and populate ``operations`` with
instances of :class:`MaintainTable` (explicit single table) or
:class:`MaintainSchema` (autodiscovery by schema prefix).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from loom.etl.maintenance._ops import CompactSpec, MaintenanceSpec, VacuumSpec, ZOrderSpec

if TYPE_CHECKING:
    from loom.etl.storage._config import StorageConfig, TableRoute


def _assert_no_conflicting_ops(
    ops: list[VacuumSpec | CompactSpec | ZOrderSpec], owner: str
) -> None:
    """Raise TypeError when both compact and z_order ops are declared."""
    has_compact = any(isinstance(op, CompactSpec) for op in ops)
    has_z_order = any(isinstance(op, ZOrderSpec) for op in ops)
    if has_compact and has_z_order:
        raise TypeError(f"{owner}: compact() and z_order_by() are mutually exclusive")


def _expand_for_schemas(
    routes: Iterable[TableRoute],
    schemas: Iterable[str],
    ops: tuple[VacuumSpec | CompactSpec | ZOrderSpec, ...],
) -> list[MaintenanceSpec]:
    """Build one MaintenanceSpec per route whose name matches any schema prefix.

    An empty *schemas* iterable matches all routes (no prefix filtering).
    Prefix matching uses ``name.startswith(f"{schema}.")`` so ``"raw"``
    matches ``"raw.events"`` but not ``"raw_backup.events"``.
    """
    prefixes = tuple(f"{s}." for s in schemas)
    return [
        MaintenanceSpec(table_ref=r.name, ops=ops)
        for r in routes
        if not prefixes or any(r.name.startswith(p) for p in prefixes)
    ]


class MaintainTable:
    """Declare maintenance operations for one explicitly named Delta table.

    Example::

        MaintainTable("raw.events")
            .vacuum(retention_hours=168, dry_run=False)
            .compact()

    Args:
        table_ref: Logical table reference (e.g. ``"raw.events"``).
    """

    def __init__(self, table_ref: str) -> None:
        if not table_ref.strip():
            raise ValueError("MaintainTable: table_ref must be a non-empty string")
        self._table_ref = table_ref
        self._ops: list[VacuumSpec | CompactSpec | ZOrderSpec] = []

    # ------------------------------------------------------------------
    # Fluent setters
    # ------------------------------------------------------------------

    def vacuum(
        self,
        retention_hours: int | None = None,
        *,
        dry_run: bool = True,
        full: bool = False,
    ) -> MaintainTable:
        """Schedule a VACUUM step.

        Args:
            retention_hours: Files older than this are eligible.  ``None``
                uses the table's ``delta.deletedFileRetentionDuration``
                (default 168 h / 7 days).
            dry_run: When ``True`` (default), lists files without deleting.
                Set ``dry_run=False`` explicitly for production runs.
            full: When ``True``, also deletes every file under the table
                directory that the Delta log does not reference and that is
                older than the retention, whoever wrote it.  See
                :class:`~loom.etl.maintenance.VacuumSpec`.
        """
        self._ops.append(VacuumSpec(retention_hours=retention_hours, dry_run=dry_run, full=full))
        return self

    def compact(self, target_size: int | None = None) -> MaintainTable:
        """Schedule a bin-packing compaction step.

        Mutually exclusive with :meth:`z_order_by`.
        """
        self._ops.append(CompactSpec(target_size=target_size))
        return self

    def z_order_by(
        self,
        columns: list[str],
        target_size: int | None = None,
    ) -> MaintainTable:
        """Schedule a Z-Order clustering step.

        Mutually exclusive with :meth:`compact`.

        Args:
            columns: Columns to Z-Order by.  Must be non-empty.
        """
        if not columns:
            raise ValueError(
                f"MaintainTable({self._table_ref!r}): z_order_by requires at least one column"
            )
        self._ops.append(ZOrderSpec(columns=list(columns), target_size=target_size))
        return self

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _to_spec(self) -> MaintenanceSpec:
        """Compile to an immutable :class:`MaintenanceSpec`."""
        _assert_no_conflicting_ops(self._ops, f"MaintainTable({self._table_ref!r})")
        return MaintenanceSpec(table_ref=self._table_ref, ops=tuple(self._ops))

    def resolve(self, config: StorageConfig) -> list[MaintenanceSpec]:
        """Implement :class:`~loom.etl.maintenance._protocol.OperationDeclaration`."""
        _ = config  # MaintainTable is explicit — no config needed
        return [self._to_spec()]

    def __repr__(self) -> str:
        return f"MaintainTable({self._table_ref!r})"


class MaintainSchema:
    """Declare maintenance operations for **all** tables under a schema prefix.

    Discovery is config-driven: :meth:`_expand` filters
    ``StorageConfig.tables`` for routes whose ``name`` starts with
    ``{schema_prefix}.``.  No filesystem scanning is performed.

    Example::

        MaintainSchema("raw").vacuum(retention_hours=168, dry_run=False).compact()

    Args:
        schema_prefix: Schema prefix to match (e.g. ``"raw"`` matches
            ``"raw.events"``, ``"raw.orders"``).
    """

    def __init__(self, schema_prefix: str) -> None:
        if not schema_prefix.strip():
            raise ValueError("MaintainSchema: schema_prefix must be a non-empty string")
        self._schema_prefix = schema_prefix.strip()
        self._ops: list[VacuumSpec | CompactSpec | ZOrderSpec] = []

    # ------------------------------------------------------------------
    # Fluent setters (mirror MaintainTable)
    # ------------------------------------------------------------------

    def vacuum(
        self,
        retention_hours: int | None = None,
        *,
        dry_run: bool = True,
        full: bool = False,
    ) -> MaintainSchema:
        """Schedule a VACUUM step for all discovered tables.

        Takes the same arguments as :meth:`MaintainTable.vacuum`.
        """
        self._ops.append(VacuumSpec(retention_hours=retention_hours, dry_run=dry_run, full=full))
        return self

    def compact(self, target_size: int | None = None) -> MaintainSchema:
        """Schedule a compaction step for all discovered tables.

        Mutually exclusive with :meth:`z_order_by`.
        """
        self._ops.append(CompactSpec(target_size=target_size))
        return self

    def z_order_by(
        self,
        columns: list[str],
        target_size: int | None = None,
    ) -> MaintainSchema:
        """Schedule a Z-Order step for all discovered tables.

        Mutually exclusive with :meth:`compact`.
        """
        if not columns:
            raise ValueError(
                f"MaintainSchema({self._schema_prefix!r}): z_order_by requires at least one column"
            )
        self._ops.append(ZOrderSpec(columns=list(columns), target_size=target_size))
        return self

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _expand(self, config: StorageConfig) -> list[MaintenanceSpec]:
        """Expand to one :class:`MaintenanceSpec` per matching table route.

        Filters ``config.tables`` for routes whose ``name`` starts with
        ``{schema_prefix}.``.
        """
        _assert_no_conflicting_ops(self._ops, f"MaintainSchema({self._schema_prefix!r})")
        return _expand_for_schemas(config.tables, (self._schema_prefix,), tuple(self._ops))

    def resolve(self, config: StorageConfig) -> list[MaintenanceSpec]:
        """Implement :class:`~loom.etl.maintenance._protocol.OperationDeclaration`."""
        return self._expand(config)

    def __repr__(self) -> str:
        return f"MaintainSchema({self._schema_prefix!r})"
