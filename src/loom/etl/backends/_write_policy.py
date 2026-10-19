"""Shared write policy via Template Method pattern.

This module provides the base class for all backend-specific target writers.
It implements the common "check-exists → create OR align+write" policy,
while delegating backend-specific operations to abstract hooks.
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from loom.core.logger import get_logger
from loom.etl.backends._historify import HistorifyBackend, scd2_transform
from loom.etl.declarative.target import (
    AppendSpec,
    ClientSpec,
    EmptyReplaceError,
    FileSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    SchemaMode,
    TargetSpec,
    TempFanInSpec,
    TempSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._history import HistorifyRepairReport, HistorifySpec
from loom.etl.runtime.contracts import TargetWriter
from loom.etl.schema._schema import SchemaNotFoundError
from loom.etl.storage._config import AuditConfig, MissingTablePolicy
from loom.etl.storage.routing import ResolvedTarget, TableRouteResolver

if TYPE_CHECKING:
    from loom.etl.lineage._records import WriteContext

_log = get_logger(__name__)

InputFrameT = TypeVar("InputFrameT")
WriteFrameT = TypeVar("WriteFrameT")
PhysicalSchemaT = TypeVar("PhysicalSchemaT")


@dataclass(frozen=True, slots=True)
class _PreparedWrite(Generic[WriteFrameT, PhysicalSchemaT]):
    """A write-ready frame aligned to the schema of an already existing table.

    Attributes:
        frame:           Materialised frame, aligned to ``existing_schema``.
        existing_schema: Physical schema read from the destination table.
    """

    frame: WriteFrameT
    existing_schema: PhysicalSchemaT


def _ensure_can_create_missing_table(
    *,
    target: ResolvedTarget,
    schema_mode: SchemaMode,
    missing_table_policy: MissingTablePolicy,
) -> None:
    """Validate whether the write path may create a missing destination table."""
    if _can_create_missing_table(
        schema_mode=schema_mode, missing_table_policy=missing_table_policy
    ):
        return
    raise SchemaNotFoundError(
        f"Destination table does not yet exist: {target}. "
        "Use SchemaMode.OVERWRITE or set storage.missing_table_policy='create'."
    )


def _can_create_missing_table(
    *,
    schema_mode: SchemaMode,
    missing_table_policy: MissingTablePolicy,
) -> bool:
    """Return ``True`` when table creation is allowed for missing destination."""
    if missing_table_policy is MissingTablePolicy.ERROR:
        return False
    return missing_table_policy is MissingTablePolicy.CREATE or schema_mode is SchemaMode.OVERWRITE


class _WritePolicy(TargetWriter, Generic[InputFrameT, WriteFrameT, PhysicalSchemaT]):
    """Base class for backend-specific target writers using Template Method pattern.

    Implements the shared write policy:
    1. Check if target exists
    2. If not exists → validate can create → create
    3. If exists → align schema → materialize → write

    Backend subclasses implement the hooks (_physical_schema, _append, etc.).
    """

    def __init__(
        self,
        *,
        resolver: TableRouteResolver,
        missing_table_policy: MissingTablePolicy,
        audit_config: AuditConfig | None = None,
    ) -> None:
        self._resolver = resolver
        self._missing_table_policy = missing_table_policy
        self._audit_config: AuditConfig = audit_config or AuditConfig()

    # ========================================================================
    # Public API (from TargetWriter)
    # ========================================================================

    def write(
        self,
        frame: InputFrameT,
        spec: TargetSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool = False,
        write_ctx: WriteContext | None = None,
    ) -> None:
        """Write frame according to spec.

        Args:
            frame:           Input frame from the step execute() result.
            spec:            Compiled target specification.
            params_instance: Concrete params for the current run.
            streaming:       Hint for lazy backends to use streaming collect.
            write_ctx:       Execution context for audit-column injection.
                             ``None`` disables audit columns for this write.
        """
        if isinstance(spec, FileSpec):
            self._write_file(frame, spec, params_instance, streaming=streaming)
            return
        if isinstance(spec, (TempSpec, TempFanInSpec)):
            raise TypeError(
                f"{type(self).__name__} does not support temp targets; "
                "TEMP writes are handled by CheckpointStore in ETLExecutor."
            )
        if isinstance(spec, ClientSpec):
            raise TypeError(
                f"{type(self).__name__} does not support client targets. "
                "ClientStep execution is handled by ETLExecutor before write() is called. "
                "This is an internal error — ClientSpec should never reach write()."
            )

        frame = self._apply_audit_columns(frame, write_ctx, params_instance, self._audit_config)
        target = self._resolver.resolve(spec.table_ref)
        self._dispatch_table_write(frame, target, spec, params_instance, streaming)

    def _dispatch_table_write(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: TargetSpec,
        params_instance: Any,
        streaming: bool,
    ) -> None:
        """Route a table write to the policy method of the spec's write mode."""
        match spec:
            case AppendSpec():
                self._do_append(frame, target, spec, streaming)
            case ReplaceSpec():
                self._do_replace(frame, target, spec, streaming)
            case ReplacePartitionsSpec():
                self._do_replace_partitions(frame, target, spec, streaming)
            case ReplaceWhereSpec():
                self._do_replace_where(frame, target, spec, params_instance, streaming)
            case UpsertSpec():
                self._do_upsert(frame, target, spec, streaming)
            case UpdateSpec():
                self._do_update(frame, target, spec, streaming)
            case HistorifySpec():
                self._do_historify(frame, target, spec, params_instance)
            case _:
                raise TypeError(f"Unsupported target spec: {type(spec)!r}")

    # ========================================================================
    # Template Methods (shared policy)
    # ========================================================================

    def _prepare_write(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        streaming: bool,
        create_partition_cols: tuple[str, ...] = (),
        require_physical_partitions: tuple[str, ...] | None = None,
        allow_empty: bool = True,
    ) -> _PreparedWrite[WriteFrameT, PhysicalSchemaT] | None:
        """Run the shared policy up to the point where the write mode differs.

        The policy is the same for every mode that may create its destination:
        read the physical schema; when the table is absent, check that creating
        it is allowed and create it from *frame*; when it is present, align
        *frame* to it and materialise the result.

        Args:
            frame:                       Input frame for the write.
            target:                      Resolved destination.
            schema_mode:                 Schema mode declared by the spec.
            streaming:                   Hint for lazy backends to stream the
                                         materialisation.
            create_partition_cols:       Partition columns for the created
                                         table; empty for unpartitioned modes.
            require_physical_partitions: Partition columns the existing table
                                         must physically carry, or ``None`` to
                                         skip that check.
            allow_empty:                 When ``False``, raise
                                         :class:`EmptyReplaceError` for a
                                         frame with no rows, before the table
                                         is created or written.

        Returns:
            The prepared write when the table already exists and the caller
            must still issue its mode-specific write, or ``None`` when the
            table was absent and has just been created from *frame*. A caller
            passing ``allow_empty=False`` must ``_release_pinned(prepared.frame)``.
        """
        existing = self._physical_schema(target)
        if existing is None:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
            materialized = self._materialize_checked(
                frame, target, streaming, allow_empty=allow_empty
            )
            try:
                self._create(
                    materialized,
                    target,
                    schema_mode=schema_mode,
                    partition_cols=create_partition_cols,
                )
            finally:
                if not allow_empty:
                    self._release_pinned(materialized)
            return None
        if require_physical_partitions is not None:
            self._require_physical_partitions(target, require_physical_partitions)
        aligned = self._align(frame, existing, schema_mode)
        return _PreparedWrite(
            frame=self._materialize_checked(aligned, target, streaming, allow_empty=allow_empty),
            existing_schema=existing,
        )

    def _do_append(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: AppendSpec,
        streaming: bool,
    ) -> None:
        """Append policy: check exists → create OR align → write."""
        prepared = self._prepare_write(
            frame, target, schema_mode=spec.schema_mode, streaming=streaming
        )
        if prepared is None:
            return
        self._append(prepared.frame, target, schema_mode=spec.schema_mode)

    def _do_replace(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: ReplaceSpec,
        streaming: bool,
    ) -> None:
        """Replace policy: check exists → create OR align → write."""
        prepared = self._prepare_write(
            frame,
            target,
            schema_mode=spec.schema_mode,
            streaming=streaming,
            allow_empty=spec.allow_empty,
        )
        if prepared is None:
            return
        try:
            self._replace(prepared.frame, target, schema_mode=spec.schema_mode)
        finally:
            if not spec.allow_empty:
                self._release_pinned(prepared.frame)

    def _do_replace_partitions(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: ReplacePartitionsSpec,
        streaming: bool,
    ) -> None:
        """Replace partitions policy: check exists → create OR align → write."""
        prepared = self._prepare_write(
            frame,
            target,
            schema_mode=spec.schema_mode,
            streaming=streaming,
            create_partition_cols=spec.partition_cols,
            require_physical_partitions=spec.partition_cols if spec.require_physical else None,
        )
        if prepared is None:
            return
        self._replace_partitions(
            prepared.frame,
            target,
            partition_cols=spec.partition_cols,
            schema_mode=spec.schema_mode,
        )

    def _physical_partition_columns(self, target: ResolvedTarget) -> tuple[str, ...] | None:
        """The table's physical partition columns, or ``None`` when the backend
        cannot tell — which makes ``require_physical`` refuse rather than guess."""
        return None

    def _require_physical_partitions(
        self, target: ResolvedTarget, partition_cols: tuple[str, ...]
    ) -> None:
        physical = self._physical_partition_columns(target)
        if physical is None:
            raise ValueError(
                f"replace_physical_partitions: backend cannot verify the physical "
                f"partitioning of {target}. Use replace_partitions/replace_matching, "
                f"or a backend that exposes partition metadata."
            )
        missing = [col for col in partition_cols if col not in physical]
        if missing:
            raise ValueError(
                f"replace_physical_partitions: table {target} is partitioned by "
                f"{list(physical) or 'nothing'} but the write asked for {list(partition_cols)} "
                f"(missing: {missing}). For value-based replacement on these columns "
                f"use replace_matching instead."
            )

    def _do_replace_where(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: ReplaceWhereSpec,
        params_instance: Any,
        streaming: bool,
    ) -> None:
        """Replace where policy: compile predicate → check exists → create OR align → write."""
        predicate = self._predicate_to_sql(spec.replace_predicate, params_instance)
        prepared = self._prepare_write(
            frame, target, schema_mode=spec.schema_mode, streaming=streaming
        )
        if prepared is None:
            return
        self._replace_where(
            prepared.frame,
            target,
            predicate=predicate,
            schema_mode=spec.schema_mode,
        )

    def _do_upsert(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: UpsertSpec,
        streaming: bool,
    ) -> None:
        """Upsert policy: check exists → create OR align+write."""
        prepared = self._prepare_write(
            frame,
            target,
            schema_mode=spec.schema_mode,
            streaming=False,
            create_partition_cols=spec.partition_cols,
        )
        if prepared is None:
            return
        self._upsert(
            prepared.frame,
            target,
            spec=spec,
            existing_schema=prepared.existing_schema,
        )

    def _do_update(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: UpdateSpec,
        streaming: bool,
    ) -> None:
        """Update policy: require exists → align+write.

        Unlike :meth:`_do_upsert` there is no creation path: an update-only
        MERGE against a missing table has nothing to update, and creating the
        table from the frame would insert every row — exactly what this mode
        exists to prevent.
        """
        existing = self._physical_schema(target)
        if existing is None:
            raise SchemaNotFoundError(
                f"Destination table does not exist: {target}. "
                "update() never inserts, so it cannot create the table — "
                "create it first with append/replace/upsert."
            )
        aligned = self._align(frame, existing, spec.schema_mode)
        materialized = self._materialize_checked(aligned, target, streaming=False)
        self._update(
            materialized,
            target,
            spec=spec,
            existing_schema=existing,
        )

    def _do_historify(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        spec: HistorifySpec,
        params_instance: Any,
    ) -> HistorifyRepairReport | None:
        """SCD Type 2 historify policy: read existing, validate, transform, write.

        Reads existing target data, validates creation rights when absent, runs
        :func:`~loom.etl.backends._historify.scd2_transform` with the engine's
        :meth:`_historify_backend`, then writes the rebuilt history. The existing
        frame is dropped before the write, so eager backends release the table
        snapshot instead of holding it alongside the rebuilt history.

        Args:
            frame:           Incoming input frame.
            target:          Resolved Delta target.
            spec:            Compiled historify spec.
            params_instance: Runtime params; forwarded to the engine for
                             ``effective_date`` resolution.

        Returns:
            A :class:`~loom.etl.HistorifyRepairReport` when a re-weave was
            performed, or ``None`` for a normal forward-only run.

        Raises:
            HistorifyDeleteMarkerError:     ``delete_marker`` missing or not boolean.
            HistorifyKeyConflictError:      Duplicate entity state vectors.
            HistorifyDateCollisionError:    Same-date ties in LOG mode.
            HistorifyTemporalConflictError: Future-open records detected.
        """
        existing = self._read_existing_data(target, frame, spec)
        create = existing is None
        if create:
            _ensure_can_create_missing_table(
                target=target,
                schema_mode=spec.schema_mode,
                missing_table_policy=self._missing_table_policy,
            )
        materialized = self._materialize_checked(frame, target, streaming=False)
        result, report = scd2_transform(
            self._historify_backend(), materialized, existing, spec, params_instance
        )
        del existing
        if report is not None:
            _log.warning(
                "historify_temporal_rerun",
                target=target.logical_ref.ref,
                affected_key_count=len(report.affected_keys),
                dates_requiring_rerun=[str(d) for d in report.dates_requiring_rerun],
                warnings=list(report.warnings),
            )
        self._write_history(result, target, spec=spec, create=create)
        return report

    def _write_history(
        self,
        result: WriteFrameT,
        target: ResolvedTarget,
        *,
        spec: HistorifySpec,
        create: bool,
    ) -> None:
        """Write rebuilt history: create the table, or replace its scoped content."""
        if create:
            self._create(
                result,
                target,
                schema_mode=spec.schema_mode,
                partition_cols=spec.partition_scope or (),
            )
        elif spec.partition_scope:
            self._replace_partitions(
                result, target, partition_cols=spec.partition_scope, schema_mode=spec.schema_mode
            )
        else:
            self._replace(result, target, schema_mode=spec.schema_mode)

    # ========================================================================
    # Audit Hook (overrideable — default is a no-op)
    # ========================================================================

    def _apply_audit_columns(
        self,
        frame: InputFrameT,
        write_ctx: WriteContext | None,
        params_instance: Any,
        audit: AuditConfig,
    ) -> InputFrameT:
        """Inject audit columns into *frame* before the write.

        The default implementation is a no-op so that non-Polars backends
        (Spark) are not broken.  Override in backend subclasses to apply
        engine-specific column injection.

        Args:
            frame:           Input frame before write.
            write_ctx:       Step execution context carrying run identifiers.
            params_instance: Concrete params; used to resolve ``from_param``.
            audit:           Audit config from the storage configuration.

        Returns:
            Frame with audit columns added (or the original frame unchanged).
        """
        return frame

    # ========================================================================
    # Abstract Hooks (backend-specific implementations)
    # ========================================================================

    @abstractmethod
    def _physical_schema(self, target: ResolvedTarget) -> PhysicalSchemaT | None:
        """Read physical schema for target, or None if not exists."""

    @abstractmethod
    def _read_existing_data(
        self,
        target: ResolvedTarget,
        frame: InputFrameT,
        spec: HistorifySpec,
    ) -> WriteFrameT | None:
        """Read existing target data for SCD Type 2, pruned to relevant partitions.

        Called before the incoming frame is fully materialized so that backends
        can extract partition-column values cheaply (e.g. from a LazyFrame) and
        use them to push down a partition filter — reading only the files that
        could be affected by the incoming delta, not the entire history table.

        When ``spec.partition_scope`` is set the implementation SHOULD restrict
        the read to the partitions present in ``frame``.  When it is ``None``
        the full table must be returned.

        Args:
            target: Resolved Delta target.
            frame:  Incoming input frame, not yet fully materialized.  Use it
                    only to extract distinct partition-column values.
            spec:   Compiled historify spec; ``spec.partition_scope`` carries
                    the partition column names.

        Returns:
            Backend frame with the (optionally pruned) current state, or
            ``None`` when the target table does not yet exist.
        """

    @abstractmethod
    def _align(
        self,
        frame: InputFrameT,
        existing_schema: PhysicalSchemaT | None,
        mode: SchemaMode,
    ) -> InputFrameT:
        """Align frame schema with existing."""

    def _row_count_if_cheap(self, frame: WriteFrameT) -> int | None:
        """Rows in *frame*, or ``None`` when the backend cannot tell without a scan."""
        _ = frame
        return None

    @abstractmethod
    def _pin_rows(self, frame: WriteFrameT) -> tuple[WriteFrameT, bool]:
        """Return *frame* computed once, and whether it has no rows.

        The write uses the returned frame, so it writes the rows that were
        counted.

        Raises:
            ValueError: When the backend cannot count the rows of *frame*
                before the write, such as a Spark streaming DataFrame.
        """

    def _release_pinned(self, frame: WriteFrameT) -> None:
        """Free what :meth:`_pin_rows` kept for *frame*, once nothing reads it.

        Called after the write, whether it succeeded or failed, and before
        :class:`EmptyReplaceError` is raised. An override must not raise, so
        the outcome of the write is the one the caller sees. Holds nothing by
        default.
        """
        _ = frame

    def _materialize_checked(
        self,
        frame: InputFrameT,
        target: ResolvedTarget,
        streaming: bool,
        *,
        allow_empty: bool = True,
    ) -> WriteFrameT:
        """Materialise *frame*, warning when the write carries no rows.

        When *allow_empty* is ``False`` the returned frame is the one whose
        rows were counted.

        Raises:
            EmptyReplaceError: When *allow_empty* is ``False`` and the
                materialised frame has no rows.
            ValueError: When *allow_empty* is ``False`` and the backend
                cannot count the rows of the materialised frame.
        """
        materialized = self._materialize_for_write(frame, streaming)
        if allow_empty:
            if self._row_count_if_cheap(materialized) == 0:
                _log.warning("write_produced_no_rows", target=target.logical_ref.ref)
            return materialized
        pinned, empty = self._pin_rows(materialized)
        if empty:
            self._release_pinned(pinned)
            raise EmptyReplaceError(target.logical_ref.ref)
        return pinned

    @abstractmethod
    def _materialize_for_write(self, frame: InputFrameT, streaming: bool) -> WriteFrameT:
        """Convert input frame into write-ready frame for backend sinks."""

    @abstractmethod
    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convert predicate to SQL string."""

    @abstractmethod
    def _create(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new table."""

    @abstractmethod
    def _append(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to existing table."""

    @abstractmethod
    def _replace(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace existing table."""

    @abstractmethod
    def _replace_partitions(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Replace partitions in existing table."""

    @abstractmethod
    def _replace_where(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Replace where predicate matches."""

    @abstractmethod
    def _upsert(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        spec: UpsertSpec,
        existing_schema: PhysicalSchemaT,
    ) -> None:
        """Upsert/merge into existing table."""

    @abstractmethod
    def _update(
        self,
        frame: WriteFrameT,
        target: ResolvedTarget,
        *,
        spec: UpdateSpec,
        existing_schema: PhysicalSchemaT,
    ) -> None:
        """Matched-only merge into existing table — never inserts."""

    @abstractmethod
    def _historify_backend(self) -> HistorifyBackend[WriteFrameT]:
        """Return the SCD2 primitives of this engine."""

    @abstractmethod
    def _write_file(
        self,
        frame: InputFrameT,
        spec: FileSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet), resolving path templates from params."""


__all__ = ["_WritePolicy"]
