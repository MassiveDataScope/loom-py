"""Polars target writer implementing _WritePolicy hooks."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Literal

import polars as pl
from deltalake import CommitProperties, DeltaTable, WriterProperties, write_deltalake
from deltalake.table import TableMerger

from loom.core.logger import get_logger
from loom.etl.backends._merge import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_merge_plan,
    _build_partition_predicate,
    _log_partition_combos,
    _MergePlan,
    _warn_no_partition_cols,
)
from loom.etl.backends._path_template import resolve_file_uri
from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.backends.polars._historify import PolarsHistorifyBackend
from loom.etl.backends.polars._schema import (
    PolarsPhysicalSchema,
    apply_schema_polars,
    read_delta_physical_schema,
)
from loom.etl.declarative.target import SchemaMode
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._history import HistorifySpec
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.lineage._records import LineageRecord, WriteContext, get_lineage_schema
from loom.etl.schema._schema import SchemaError
from loom.etl.storage import (
    MissingTablePolicy,
    PathRouteResolver,
    PathTarget,
    ResolvedTarget,
    TableLocation,
    TableRouteResolver,
)
from loom.etl.storage._config import AuditConfig
from loom.etl.storage._file_locator import FileLocator
from loom.etl.storage._locator import TableLocator, _as_locator

from ._dtype import loom_type_to_polars
from ._file_writer import PolarsFileWriter
from ._streaming import partition_rows_from_spool, spool_lazy_to_arrow_reader

_log = get_logger(__name__)


def _log_write_start(
    mode: str,
    location: TableLocation,
    frame: pl.DataFrame,
    *,
    partition_cols: tuple[str, ...] = (),
    predicate: str | None = None,
) -> None:
    _log.info(
        "delta write start",
        mode=mode,
        uri=location.uri,
        rows=frame.height,
        cols=len(frame.columns),
        partition_cols=list(partition_cols) or None,
        predicate=predicate,
    )


def _log_write_commit(mode: str, location: TableLocation) -> None:
    try:
        dt = DeltaTable(location.uri, storage_options=location.storage_options or {})
        history = dt.history(1)
        entry = history[0] if history else {}
        metrics = entry.get("operationMetrics") or {}
        _log.info(
            "delta write commit",
            mode=mode,
            uri=location.uri,
            version=dt.version(),
            rows=int(metrics.get("numOutputRows", 0)) or None,
            bytes=int(metrics.get("numOutputBytes", 0)) or None,
            files=int(metrics.get("numAddedFiles", 0)) or None,
        )
    except Exception:  # noqa: BLE001
        _log.info("delta write commit", mode=mode, uri=location.uri, metrics="unavailable")


def _check_null_dtype_columns_lazy(frame: pl.LazyFrame) -> None:
    """Lazy variant of :func:`_check_null_dtype_columns`.

    Raises :exc:`~loom.etl.schema.SchemaError` if any column in the lazy
    schema has dtype ``Null``.  Operates on ``collect_schema()`` (metadata
    only), so it is safe to call on a frame about to be streamed.
    """
    schema = frame.collect_schema()
    null_cols = [name for name, dtype in schema.items() if isinstance(dtype, pl.Null)]
    if not null_cols:
        return
    schema_repr = "\n".join(f"  {n}: {d}" for n, d in schema.items())
    raise SchemaError(
        f"Delta Lake does not support the Null dtype. "
        f"Column(s) with Null dtype: {null_cols}.\n"
        f"Cast each column to its intended type before writing "
        f"(use an explicit schema declaration or .cast() in the pipeline step).\n"
        f"Full frame schema:\n{schema_repr}"
    )


def _check_null_dtype_columns(frame: pl.DataFrame) -> None:
    """Raise :exc:`~loom.etl.schema.SchemaError` if any column has dtype ``Null``.

    Delta Lake rejects ``Null``-typed columns.  This check surfaces the
    offending column names before the write attempt so the error is
    actionable rather than an opaque external exception.

    Args:
        frame: Collected DataFrame about to be written to Delta.

    Raises:
        SchemaError: When one or more columns carry ``pl.Null`` dtype.
    """
    null_cols = [name for name, dtype in frame.schema.items() if isinstance(dtype, pl.Null)]
    if not null_cols:
        return
    schema_repr = "\n".join(f"  {n}: {d}" for n, d in frame.schema.items())
    raise SchemaError(
        f"Delta Lake does not support the Null dtype. "
        f"Column(s) with Null dtype: {null_cols}.\n"
        f"Cast each column to its intended type before writing "
        f"(use an explicit schema declaration or .cast() in the pipeline step).\n"
        f"Full frame schema:\n{schema_repr}"
    )


class PolarsTargetWriter(_WritePolicy[pl.LazyFrame, pl.DataFrame, PolarsPhysicalSchema]):
    """Polars target writer using delta-rs for Delta tables."""

    def __init__(
        self,
        locator: str | TableLocator,
        *,
        route_resolver: TableRouteResolver | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
        file_locator: FileLocator | None = None,
        audit_config: AuditConfig | None = None,
    ) -> None:
        self._locator = _as_locator(locator)
        super().__init__(
            resolver=route_resolver or PathRouteResolver(self._locator),
            missing_table_policy=missing_table_policy,
            audit_config=audit_config,
        )
        self._file_writer = PolarsFileWriter()
        self._file_locator = file_locator

    def append(
        self,
        frame: pl.LazyFrame,
        table_ref: Any,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Append frame to table (legacy API, creates table on first write)."""
        spec = AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE)
        self.write(frame, spec, params_instance, streaming=streaming)

    def to_frame(self, records: Sequence[LineageRecord], /) -> pl.LazyFrame:
        """Convert lineage records into a Polars LazyFrame."""
        if not records:
            raise ValueError("PolarsTargetWriter.to_frame requires at least one record.")
        first = records[0]
        record_type = type(first)
        if any(type(record) is not record_type for record in records):
            raise TypeError(
                "PolarsTargetWriter.to_frame requires homogeneous record types per batch."
            )
        rows = [record.to_row() for record in records]
        return pl.from_dicts(rows, schema=_polars_lineage_schema(first)).lazy()

    # ====================================================================
    # Schema Hooks
    # ====================================================================

    def _physical_schema(self, target: ResolvedTarget) -> PolarsPhysicalSchema | None:
        """Read physical schema from Delta log."""
        path_target = self._as_path_target(target)
        physical = read_delta_physical_schema(
            path_target.location.uri,
            path_target.location.storage_options,
        )
        return physical

    def _align(
        self,
        frame: pl.LazyFrame,
        existing_schema: PolarsPhysicalSchema | None,
        mode: SchemaMode,
    ) -> pl.LazyFrame:
        """Align frame schema with existing."""
        schema = existing_schema.schema if existing_schema is not None else None
        return apply_schema_polars(frame, schema, mode)

    def _row_count_if_cheap(self, frame: pl.DataFrame) -> int | None:
        """Rows in the collected frame."""
        return frame.height

    def _pin_rows(self, frame: pl.DataFrame) -> tuple[pl.DataFrame, bool]:
        """Return the collected frame, which already holds its rows, and whether it has none."""
        return frame, frame.is_empty()

    def _materialize_for_write(self, frame: pl.LazyFrame, streaming: bool) -> pl.DataFrame:
        """Collect lazy frame to DataFrame before Delta write."""
        df = frame.collect(engine="streaming" if streaming else "auto")
        _check_null_dtype_columns(df)
        return df

    def _do_replace_partitions(
        self,
        frame: pl.LazyFrame,
        target: ResolvedTarget,
        spec: ReplacePartitionsSpec,
        streaming: bool,
    ) -> None:
        if not spec.streaming:
            super()._do_replace_partitions(frame, target, spec, streaming)
            return

        existing = self._physical_schema(target)
        if existing is None:
            super()._do_replace_partitions(frame, target, spec, streaming)
            return

        if spec.require_physical:
            self._require_physical_partitions(target, spec.partition_cols)
        aligned = self._align(frame, existing, spec.schema_mode)
        _check_null_dtype_columns_lazy(aligned)
        self._streaming_replace_partitions(aligned, target, spec)

    def _streaming_replace_partitions(
        self,
        frame: pl.LazyFrame,
        target: ResolvedTarget,
        spec: ReplacePartitionsSpec,
    ) -> None:
        path_target = self._as_path_target(target)
        location = path_target.location
        with spool_lazy_to_arrow_reader(frame) as spool:
            partition_rows = partition_rows_from_spool(spool.spool_path, spec.partition_cols)
            if not partition_rows:
                _log.warning("replace_partitions empty frame", table=location.uri)
                return
            null_rows = [r for r in partition_rows if any(v is None for v in r.values())]
            if null_rows:
                sample = null_rows[:5]
                more = (
                    f" (+{len(null_rows) - len(sample)} more)"
                    if len(null_rows) > len(sample)
                    else ""
                )
                raise ValueError(
                    f"replace_partitions: null values in partition columns "
                    f"{spec.partition_cols} for table {path_target}. Null partition "
                    f"values cannot be used as a replace predicate. Ensure all "
                    f"partition columns are non-null before writing. "
                    f"Offending rows: {sample}{more}"
                )
            predicate = _build_partition_predicate(iter(partition_rows), spec.partition_cols)
            _log.info(
                "delta write start",
                mode="replace_partitions",
                uri=location.uri,
                rows="streaming",
                cols=None,
                partition_cols=list(spec.partition_cols),
                predicate=predicate,
            )
            write_deltalake(
                location.uri,
                spool.reader,
                mode="overwrite",
                predicate=predicate,
                schema_mode=self._delta_schema_mode(spec.schema_mode),
                **self._write_kwargs(location),
            )
        _log_write_commit("replace_partitions", location)

    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convert predicate to SQL (Polars uses internal SQL representation)."""
        return predicate_to_sql(predicate, params)

    # ====================================================================
    # Write Hooks
    # ====================================================================

    def _create(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new Delta table."""
        path_target = self._as_path_target(target)
        _ = schema_mode
        location = path_target.location
        if partition_cols:
            missing = tuple(c for c in partition_cols if c not in frame.columns)
            if missing:
                # Empty frame with no partition cols is a legitimate no-op
                # (e.g. decomposition fallback when the source field is absent
                # from the input schema). Mirrors _replace_partitions on the
                # update path.
                if frame.is_empty():
                    _log.warning(
                        "create skipped: empty frame missing partition columns",
                        table=location.uri,
                        missing=list(missing),
                    )
                    return
                raise ValueError(
                    f"_create: partition columns not found in frame schema: "
                    f"{list(missing)}. Frame columns: {frame.columns}. Cannot "
                    f"create partitioned Delta table at {location.uri!r}."
                )
        self._warn_uc_first_create(path_target)
        kwargs = self._write_kwargs(location)

        _log_write_start("create", location, frame, partition_cols=partition_cols)
        if partition_cols:
            write_deltalake(
                location.uri,
                frame,
                mode="overwrite",
                partition_by=list(partition_cols),
                schema_mode="overwrite",
                **kwargs,
            )
        else:
            write_deltalake(
                location.uri,
                frame,
                mode="overwrite",
                schema_mode="overwrite",
                **kwargs,
            )
        _log_write_commit("create", location)

    @staticmethod
    def _delta_schema_mode(
        schema_mode: SchemaMode,
    ) -> Literal["merge", "overwrite"] | None:
        """Map Loom SchemaMode to the delta-rs schema_mode string.

        Returns None for STRICT so delta-rs applies its default behaviour
        (reject writes that change the schema).
        """
        if schema_mode is SchemaMode.OVERWRITE:
            return "overwrite"
        if schema_mode is SchemaMode.EVOLVE:
            return "merge"
        return None  # STRICT — let delta-rs reject schema changes

    def _append(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to existing Delta table."""
        path_target = self._as_path_target(target)
        location = path_target.location
        _log_write_start("append", location, frame)
        write_deltalake(
            location.uri,
            frame,
            mode="append",
            schema_mode=self._delta_schema_mode(schema_mode),
            **self._write_kwargs(location),
        )
        _log_write_commit("append", location)

    def _replace(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite existing Delta table."""
        path_target = self._as_path_target(target)
        location = path_target.location
        _log_write_start("replace", location, frame)
        write_deltalake(
            location.uri,
            frame,
            mode="overwrite",
            schema_mode=self._delta_schema_mode(schema_mode),
            **self._write_kwargs(location),
        )
        _log_write_commit("replace", location)

    def _physical_partition_columns(self, target: ResolvedTarget) -> tuple[str, ...] | None:
        location = self._as_path_target(target).location
        dt = DeltaTable(location.uri, storage_options=location.storage_options or {})
        return tuple(dt.metadata().partition_columns)

    def _replace_partitions(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite partitions present in frame."""
        path_target = self._as_path_target(target)
        if frame.is_empty():
            _log.warning(
                "replace_partitions empty frame",
                table=path_target.location.uri,
            )
            return

        location = path_target.location
        partition_rows: list[dict[str, Any]] = list(
            frame.select(list(partition_cols)).unique().iter_rows(named=True)
        )
        null_rows = [r for r in partition_rows if any(v is None for v in r.values())]
        if null_rows:
            raise ValueError(
                f"replace_partitions: null values in partition columns {partition_cols} for "
                f"table {path_target}. Null partition values cannot be used as a replace "
                f"predicate. Ensure all partition columns are non-null before writing. "
                f"Offending rows: {null_rows}"
            )
        predicate = _build_partition_predicate(
            iter(partition_rows),
            partition_cols,
        )
        _log_write_start(
            "replace_partitions",
            location,
            frame,
            partition_cols=partition_cols,
            predicate=predicate,
        )
        write_deltalake(
            location.uri,
            frame,
            mode="overwrite",
            predicate=predicate,
            schema_mode=self._delta_schema_mode(schema_mode),
            **self._write_kwargs(location),
        )
        _log_write_commit("replace_partitions", location)

    def _replace_where(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite rows matching SQL predicate."""
        path_target = self._as_path_target(target)
        location = path_target.location
        _log_write_start("replace_where", location, frame, predicate=predicate)
        write_deltalake(
            location.uri,
            frame,
            mode="overwrite",
            predicate=predicate,
            schema_mode=self._delta_schema_mode(schema_mode),
            **self._write_kwargs(location),
        )
        _log_write_commit("replace_where", location)

    def _upsert(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        spec: UpsertSpec,
        existing_schema: PolarsPhysicalSchema,
    ) -> None:
        """Merge frame into target using Delta MERGE (update + insert)."""
        _ = existing_schema
        merger, plan, location = self._matched_update_merger(frame, target, spec, mode="upsert")
        merger.when_not_matched_insert(updates=plan.insert_values).execute()
        _log_write_commit("upsert", location)

    def _update(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        *,
        spec: UpdateSpec,
        existing_schema: PolarsPhysicalSchema,
    ) -> None:
        """Merge frame into target using a matched-only Delta MERGE.

        No ``when_not_matched_insert`` clause is emitted, so unmatched source
        rows are ignored and the MERGE inserts zero rows by construction.
        """
        _ = existing_schema
        merger, _plan, location = self._matched_update_merger(frame, target, spec, mode="update")
        merger.execute()
        _log_write_commit("update", location)

    def _matched_update_merger(
        self,
        frame: pl.DataFrame,
        target: ResolvedTarget,
        spec: UpsertSpec | UpdateSpec,
        *,
        mode: str,
    ) -> tuple[TableMerger, _MergePlan, TableLocation]:
        """Build the Delta MERGE up to the shared ``when_matched_update`` clause.

        Both :meth:`_upsert` and :meth:`_update` start from the same merge:
        partition-combo pre-filter, ON predicate, and the matched-update
        column map.  The caller chains its remaining clauses and executes.

        Args:
            frame:  Materialised source frame.
            target: Resolved path target.
            spec:   Upsert or update spec (identical merge fields).
            mode:   Write-mode label for logging.

        Returns:
            Tuple of the prepared merger, the merge plan, and the target
            location (for commit logging).
        """
        path_target = self._as_path_target(target)
        location = path_target.location

        # Collect partition combinations for pre-filter
        combos = self._collect_partition_combos(
            frame,
            spec.partition_cols,
            path_target.logical_ref.ref,
        )

        merge_plan = _build_merge_plan(
            combos=combos,
            spec=spec,
            df_columns=tuple(frame.columns),
            target_alias=TARGET_ALIAS,
            source_alias=SOURCE_ALIAS,
        )

        dt = DeltaTable(location.uri, storage_options=location.storage_options or {})
        _log_write_start(
            mode,
            location,
            frame,
            partition_cols=spec.partition_cols,
            predicate=merge_plan.predicate,
        )
        merger = dt.merge(
            source=frame,
            predicate=merge_plan.predicate,
            source_alias=SOURCE_ALIAS,
            target_alias=TARGET_ALIAS,
            **self._delta_properties(location),
        ).when_matched_update(updates=merge_plan.update_set)
        return merger, merge_plan, location

    def _read_existing_data(
        self,
        target: ResolvedTarget,
        frame: pl.LazyFrame,
        spec: HistorifySpec,
    ) -> pl.DataFrame | None:
        """Read existing Delta data pruned to the partitions present in ``frame``.

        When ``spec.partition_scope`` is set, distinct partition-column values
        are extracted from ``frame`` (still lazy, one cheap ``collect``) and
        pushed down as a filter before the Delta scan — so only the affected
        Parquet files are opened.  Without a partition scope the full table is
        returned.

        Args:
            target: Resolved path target.
            frame:  Incoming LazyFrame; only partition columns are touched here.
            spec:   Historify spec; ``partition_scope`` carries column names.

        Returns:
            Collected DataFrame of existing rows, or ``None`` if the table has
            not been created yet.
        """
        from deltalake.exceptions import TableNotFoundError

        path_target = self._as_path_target(target)
        location = path_target.location
        storage_options = location.storage_options or {}
        try:
            if not spec.partition_scope:
                return pl.read_delta(location.uri, storage_options=storage_options or None)
            partition_vals = frame.select(list(spec.partition_scope)).unique().collect()
            scan = pl.scan_delta(location.uri, storage_options=storage_options or None)
            filter_expr = _partition_filter(partition_vals, spec.partition_scope)
            return scan.filter(filter_expr).collect()
        except (TableNotFoundError, FileNotFoundError):
            return None

    def _historify_backend(self) -> PolarsHistorifyBackend:
        return PolarsHistorifyBackend()

    def _write_file(
        self,
        frame: pl.LazyFrame,
        spec: FileSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet), resolving alias and template."""
        resolved = self._resolve_file_spec(spec, params_instance)
        self._file_writer.write(frame, resolved, streaming=streaming)

    def _resolve_file_spec(self, spec: FileSpec, params_instance: Any) -> FileSpec:
        """Return a FileSpec with a physical URI, resolving alias and template."""
        return FileSpec(
            path=resolve_file_uri(
                spec.path,
                is_alias=spec.is_alias,
                locator=self._file_locator,
                params_instance=params_instance,
                role="IntoFile",
            ),
            format=spec.format,
            is_alias=False,
            write_options=spec.write_options,
        )

    # ====================================================================
    # Audit Hook
    # ====================================================================

    def _apply_audit_columns(
        self,
        frame: pl.LazyFrame,
        write_ctx: WriteContext | None,
        params_instance: Any,
        audit: AuditConfig,
    ) -> pl.LazyFrame:
        """Inject audit columns into a Polars LazyFrame.

        Skipped when ``audit.enabled`` is ``False`` or ``write_ctx`` is ``None``.
        ``process_run_id`` is only added when it is not ``None`` to avoid
        nullable-column schema drift in Delta tables.

        Args:
            frame:           Incoming lazy frame.
            write_ctx:       Step execution context.
            params_instance: Concrete params; used to resolve ``from_param``.
            audit:           Audit configuration from ``StorageConfig``.

        Returns:
            Frame with audit columns appended, or the original frame unchanged.
        """
        if not audit.enabled or write_ctx is None:
            return frame
        prefix = audit.prefix
        cols: list[pl.Expr] = [
            pl.lit(write_ctx.run_id).alias(f"{prefix}run_id"),
            pl.lit(write_ctx.step).alias(f"{prefix}step"),
            pl.lit(write_ctx.attempt).cast(pl.Int32).alias(f"{prefix}attempt"),
        ]
        if write_ctx.process_run_id is not None:
            cols.append(pl.lit(write_ctx.process_run_id).alias(f"{prefix}process_run_id"))
        for col_def in audit.custom:
            cols.append(_resolve_custom_column(col_def, params_instance))
        return frame.with_columns(cols)

    # ====================================================================
    # Helpers
    # ====================================================================

    @staticmethod
    def _warn_uc_first_create(target: PathTarget) -> None:
        """Warn about UC table creation limitations."""
        if not target.location.uri.lower().startswith("uc://"):
            return
        _log.warning(
            "Polars write first-create for Unity Catalog. "
            "delta-rs writes Delta log data, but catalog registration is not guaranteed; "
            "pre-create the table in UC (Spark SQL/Databricks) before this write.",
            ref=target.logical_ref.ref,
            uri=target.location.uri,
        )

    @staticmethod
    def _as_path_target(target: ResolvedTarget) -> PathTarget:
        """Validate and narrow resolved target to path target for Polars writes."""
        if isinstance(target, PathTarget):
            return target
        raise TypeError(
            "PolarsTargetWriter requires path-resolved targets. "
            "Configure routing so catalog refs resolve to PathTarget "
            "(e.g. uc://catalog.schema.table)."
        )

    @staticmethod
    def _delta_properties(loc: TableLocation) -> dict[str, Any]:
        """Build ``writer_properties`` and ``commit_properties`` from TableLocation."""
        return {
            "writer_properties": WriterProperties(**loc.writer) if loc.writer else None,
            "commit_properties": CommitProperties(**loc.commit) if loc.commit else None,
        }

    @classmethod
    def _write_kwargs(cls, loc: TableLocation) -> dict[str, Any]:
        """Build write kwargs from TableLocation."""
        return {
            "storage_options": loc.storage_options or None,
            "configuration": loc.delta_config or None,
            "target_file_size": loc.target_file_size,
            **cls._delta_properties(loc),
        }

    @staticmethod
    def _collect_partition_combos(
        frame: pl.DataFrame,
        partition_cols: tuple[str, ...],
        table_ref: str,
    ) -> list[dict[str, Any]]:
        """Collect unique partition value combinations."""
        if not partition_cols:
            _warn_no_partition_cols(table_ref)
            return []
        combos = frame.unique(subset=list(partition_cols)).select(list(partition_cols)).to_dicts()
        _log_partition_combos(combos, table_ref)
        return combos


__all__ = ["PolarsTargetWriter"]


def _resolve_custom_column(col_def: Any, params_instance: Any) -> pl.Expr:
    """Build a Polars literal expression for one custom audit column definition.

    Args:
        col_def:         :class:`~loom.etl.storage._config.CustomColumnDef` instance.
        params_instance: Concrete params object; used when ``from_param`` is set.

    Returns:
        A ``pl.lit(value).alias(name)`` expression ready for ``with_columns``.

    Raises:
        AttributeError: When ``from_param`` is set but the attribute is missing
                        from ``params_instance``.
    """
    if col_def.value is not None:
        return pl.lit(str(col_def.value)).alias(col_def.name)
    val = str(getattr(params_instance, col_def.from_param))
    return pl.lit(val).alias(col_def.name)


def _partition_filter(
    partition_vals: pl.DataFrame,
    partition_scope: tuple[str, ...],
) -> pl.Expr:
    """Build a Polars filter expression that matches exact partition combinations.

    Instead of a loose column-wise ``is_in`` (which forms a bounding hyper-
    rectangle and may read unwanted partition combinations), this builds an
    ``OR`` of ``AND`` equalities — one per distinct partition combo present in
    ``partition_vals``.  Delta-rs pushes these predicates to the file-scan level,
    opening only the Parquet files that belong to the exact combinations needed.

    Args:
        partition_vals: DataFrame with one column per partition key and one row
                        per distinct combination present in the incoming frame.
        partition_scope: Ordered column names that form the partition key.

    Returns:
        A Polars expression suitable for use in ``.filter()``.
    """
    combos = partition_vals.select(list(partition_scope)).unique().iter_rows(named=True)
    exprs: list[pl.Expr] = []
    for combo in combos:
        and_expr: pl.Expr = pl.col(partition_scope[0]) == combo[partition_scope[0]]
        for col in partition_scope[1:]:
            and_expr = and_expr & (pl.col(col) == combo[col])
        exprs.append(and_expr)
    if not exprs:
        return pl.lit(False)
    result = exprs[0]
    for expr in exprs[1:]:
        result = result | expr
    return result


def _polars_lineage_schema(record: LineageRecord) -> pl.Schema:
    cols = get_lineage_schema(type(record))
    return pl.Schema({c.name: loom_type_to_polars(c.dtype) for c in cols})
