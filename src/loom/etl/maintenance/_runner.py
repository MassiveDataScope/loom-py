"""MaintenanceRunner — wires StorageConfig → DeltaRsMaintainer → MaintenanceStep."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from loom.core.logger import get_logger

if TYPE_CHECKING:
    from loom.etl.maintenance._protocol import DeltaTableMaintainer, OptimizeResult, VacuumResult
    from loom.etl.maintenance._step import MaintenanceStep
    from loom.etl.storage._config import StorageConfig
    from loom.etl.storage._locator import TableLocation, TableLocator

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.maintenance._builder import _expand_for_schemas
from loom.etl.maintenance._ops import CompactSpec, MaintenanceSpec, VacuumSpec, ZOrderSpec
from loom.etl.maintenance._protocol import TableMaintenanceResult

_log = get_logger(__name__)


class MaintenanceError(Exception):
    """Raised by :meth:`MaintenanceReport.raise_if_errors` when any table failed."""

    def __init__(self, failed: list[TableMaintenanceResult]) -> None:
        self.failed = failed
        refs = [r.table_ref for r in failed]
        super().__init__(f"Maintenance failed for {len(failed)} table(s): {refs}")


@dataclass
class MaintenanceReport:
    """Aggregate result of a maintenance run.

    Args:
        results: Per-table outcomes (one entry per table processed).
    """

    results: list[TableMaintenanceResult] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """``True`` when at least one table reported an error."""
        return any(not r.ok for r in self.results)

    def raise_if_errors(self) -> None:
        """Raise :class:`MaintenanceError` if any table failed.

        Useful as a Prefect flow gate::

            report = runner.run(NightlyMaintenance)
            report.raise_if_errors()
        """
        failed = [r for r in self.results if not r.ok]
        if failed:
            raise MaintenanceError(failed)


class MaintenanceRunner:
    """Execute a :class:`~loom.etl.maintenance.MaintenanceStep` against Delta tables.

    Construct via :meth:`from_config` for the standard delta-rs path::

        runner = MaintenanceRunner.from_config(config)
        report = runner.run(NightlyMaintenance)
        report.raise_if_errors()

    Args:
        maintainer: Backend implementation (default: :class:`DeltaRsMaintainer`).
        locator: Resolves logical table refs to physical ``TableLocation``.
        config: Full storage config (needed by :class:`MaintainSchema` expansion
            and :meth:`run_from_config`).
        missing_table_policy: ``"skip"`` silently omits tables not present in
            *config* (or not yet initialized as Delta tables); ``"error"``
            raises immediately.
    """

    def __init__(
        self,
        maintainer: DeltaTableMaintainer,
        locator: TableLocator,
        config: StorageConfig,
        *,
        missing_table_policy: Literal["skip", "error"] = "skip",
    ) -> None:
        self._maintainer = maintainer
        self._locator = locator
        self._config = config
        self._missing_table_policy: Literal["skip", "error"] = missing_table_policy

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        config: StorageConfig,
        *,
        missing_table_policy: Literal["skip", "error"] = "skip",
    ) -> MaintenanceRunner:
        """Build a :class:`MaintenanceRunner` from a :class:`StorageConfig`.

        Uses delta-rs (``deltalake`` package) as the only backend.
        Calls :meth:`~loom.etl.storage._config.StorageConfig.to_path_locator`
        which reuses the same ``PrefixLocator`` / ``MappingLocator`` logic as
        the Polars ETL backend — no private helpers crossed.

        Raises:
            ValueError: If *config* has no path routes configured
                (only catalog/UC refs, which delta-rs cannot resolve directly).
        """
        # DeltaRsMaintainer is imported lazily here so that importing
        # MaintenanceRunner does not force the [delta] extra for users who only
        # use type annotations or subclass MaintenanceStep without running it.
        from loom.etl.maintenance.backends._delta_rs import DeltaRsMaintainer  # noqa: PLC0415

        locator = config.to_path_locator()
        return cls(
            DeltaRsMaintainer(),
            locator,
            config,
            missing_table_policy=missing_table_policy,
        )

    # ------------------------------------------------------------------
    # Run from a MaintenanceStep subclass
    # ------------------------------------------------------------------

    def run(
        self,
        step_cls: type[MaintenanceStep[Any]],
        params: Any = None,
    ) -> MaintenanceReport:
        """Execute all operations declared on *step_cls*.

        :class:`MaintainSchema` entries are expanded against the config at
        run time; :class:`MaintainTable` entries are compiled directly.

        Args:
            step_cls: A :class:`~loom.etl.maintenance.MaintenanceStep` subclass.
            params: Reserved for future parameterisation (currently unused).

        Returns:
            :class:`MaintenanceReport` with per-table results.
        """
        specs = self._resolve_specs(step_cls, params)
        results = [self._run_one(spec) for spec in specs]
        return MaintenanceReport(results=results)

    # ------------------------------------------------------------------
    # Run from config (no Python class needed)
    # ------------------------------------------------------------------

    def run_from_config(self) -> MaintenanceReport:
        """Execute maintenance using the ``maintenance`` block in StorageConfig.

        Discovers all tables listed under the configured schema prefixes and
        applies the vacuum / compact / z-order settings from
        ``StorageConfig.maintenance``.

        Returns an empty :class:`MaintenanceReport` when no ``maintenance``
        config is defined or no matching tables are found.
        """
        mc = self._config.maintenance
        if not mc.schemas and not mc.vacuum and not mc.compact and not mc.z_order_by:
            _log.debug("run_from_config: no maintenance config — skipping")
            return MaintenanceReport()

        vacuum_spec = (
            VacuumSpec(
                retention_hours=mc.vacuum.retention_hours,
                dry_run=mc.vacuum.dry_run,
                full=mc.vacuum.full,
            )
            if mc.vacuum
            else None
        )
        compact_spec: CompactSpec | None = CompactSpec() if mc.compact else None
        z_order_spec: ZOrderSpec | None = (
            ZOrderSpec(columns=list(mc.z_order_by)) if mc.z_order_by else None
        )

        if compact_spec is not None and z_order_spec is not None:
            raise ValueError(
                "StorageConfig.maintenance: compact and z_order_by are mutually exclusive"
            )

        ops: tuple[VacuumSpec | CompactSpec | ZOrderSpec, ...] = tuple(
            s for s in [vacuum_spec, compact_spec, z_order_spec] if s is not None
        )
        specs = _expand_for_schemas(self._config.tables, mc.schemas, ops)
        return MaintenanceReport(results=[self._run_one(spec) for spec in specs])

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_specs(
        self, step_cls: type[MaintenanceStep[Any]], params: Any
    ) -> list[MaintenanceSpec]:
        return [
            spec for op in step_cls().operations_for(params) for spec in op.resolve(self._config)
        ]

    def _resolve_location(self, table_ref: str) -> TableLocation | None:
        """Resolve *table_ref* to a TableLocation, applying missing_table_policy.

        Returns ``None`` when the table is absent and policy is ``"skip"``.
        Raises the underlying exception when policy is ``"error"``.
        """
        try:
            return self._locator.locate(TableRef(table_ref))
        except Exception as exc:
            if self._missing_table_policy == "skip":
                _log.warning("maintenance skip", table=table_ref, reason=repr(exc))
                return None
            raise

    def _run_one(self, spec: MaintenanceSpec) -> TableMaintenanceResult:
        """Execute all ops for one table, catching errors per table."""
        location = self._resolve_location(spec.table_ref)
        if location is None:
            return TableMaintenanceResult(table_ref=spec.table_ref)

        t0 = time.monotonic()
        op_results: dict[str, VacuumResult | OptimizeResult] = {}
        error: Exception | None = None

        try:
            for op in spec.ops:
                op_results[op.name] = op.execute(self._maintainer, location.uri, location)
        except Exception as exc:
            error = exc
            _log.exception("maintenance failed", table=spec.table_ref)

        return TableMaintenanceResult(
            table_ref=spec.table_ref,
            op_results=op_results,
            error=error,
            duration_seconds=time.monotonic() - t0,
        )
