"""Spark target writer implementing _WritePolicy hooks."""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Callable, Sequence
from typing import Any, cast

from delta.tables import DeltaMergeBuilder, DeltaTable
from pyspark.errors.exceptions.base import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T
from pyspark.sql.column import Column
from pyspark.storagelevel import StorageLevel

from loom.etl.backends._format_registry import resolve_format_handler, write_options_or_default
from loom.etl.backends._historify._transform import scd2_transform
from loom.etl.backends._merge import (
    SOURCE_ALIAS,
    TARGET_ALIAS,
    _build_merge_plan,
    _log_partition_combos,
    _MergePlan,
    _warn_no_partition_cols,
)
from loom.etl.backends._path_template import resolve_file_uri
from loom.etl.backends._predicate import predicate_to_sql
from loom.etl.backends._write_policy import _WritePolicy
from loom.etl.backends.spark._dtype import loom_type_to_spark
from loom.etl.backends.spark._historify import SparkHistorifyBackend
from loom.etl.backends.spark._schema import SparkPhysicalSchema, apply_schema_spark
from loom.etl.declarative._format import Format
from loom.etl.declarative._write_options import (
    CsvWriteOptions,
    JsonWriteOptions,
    ParquetWriteOptions,
)
from loom.etl.declarative.target import SchemaMode
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._history import HistorifyRepairReport, HistorifySpec
from loom.etl.declarative.target._table import AppendSpec, UpdateSpec, UpsertSpec
from loom.etl.lineage._records import LineageRecord, get_lineage_schema
from loom.etl.storage._config import MissingTablePolicy
from loom.etl.storage._file_locator import FileLocator
from loom.etl.storage._locator import TableLocator, _as_locator
from loom.etl.storage.routing import (
    CatalogRouteResolver,
    CatalogTarget,
    PathRouteResolver,
    ResolvedTarget,
    TableRouteResolver,
)

_log = logging.getLogger(__name__)


class SparkTargetWriter(_WritePolicy[DataFrame, DataFrame, SparkPhysicalSchema]):
    """Spark target writer using Delta Lake."""

    def __init__(
        self,
        spark: SparkSession,
        locator: str | TableLocator | None = None,
        *,
        route_resolver: TableRouteResolver | None = None,
        missing_table_policy: MissingTablePolicy = MissingTablePolicy.SCHEMA_MODE,
        file_locator: FileLocator | None = None,
    ) -> None:
        self._spark = spark

        if route_resolver is None:
            if locator is None:
                resolver: TableRouteResolver = CatalogRouteResolver()
            else:
                resolver = PathRouteResolver(_as_locator(locator))
        else:
            resolver = route_resolver

        super().__init__(
            resolver=resolver,
            missing_table_policy=missing_table_policy,
        )
        self._file_locator = file_locator

    def append(
        self,
        frame: DataFrame,
        table_ref: Any,
        params_instance: Any,
        *,
        streaming: bool = False,
    ) -> None:
        """Append frame to table (legacy API, creates table on first write)."""
        spec = AppendSpec(table_ref=table_ref, schema_mode=SchemaMode.EVOLVE)
        self.write(frame, spec, params_instance, streaming=streaming)

    def to_frame(self, records: Sequence[LineageRecord], /) -> DataFrame:
        """Convert lineage records into a Spark DataFrame."""
        if not records:
            raise ValueError("SparkTargetWriter.to_frame requires at least one record.")
        first = records[0]
        record_type = type(first)
        if any(type(record) is not record_type for record in records):
            raise TypeError(
                "SparkTargetWriter.to_frame requires homogeneous record types per batch."
            )
        rows = [record.to_row() for record in records]
        return self._spark.createDataFrame(rows, schema=_spark_record_schema(first))

    # ====================================================================
    # Schema Hooks
    # ====================================================================

    def _physical_schema(self, target: ResolvedTarget) -> SparkPhysicalSchema | None:
        """Read physical schema from Spark/Delta."""
        if isinstance(target, CatalogTarget):
            if not self._spark.catalog.tableExists(target.catalog_ref.ref):
                return None
            fields = self._spark.table(target.catalog_ref.ref).schema.fields
            return SparkPhysicalSchema(schema=T.StructType(list(fields)))

        # Path target
        try:
            dt = DeltaTable.forPath(self._spark, target.location.uri)
            return SparkPhysicalSchema(schema=dt.toDF().schema)
        except AnalysisException:
            return None

    def _align(
        self,
        frame: DataFrame,
        existing_schema: SparkPhysicalSchema | None,
        mode: SchemaMode,
    ) -> DataFrame:
        """Align frame schema with existing."""
        if existing_schema is None or mode is SchemaMode.OVERWRITE:
            return frame

        return apply_schema_spark(frame, existing_schema.schema, mode)

    def _materialize_for_write(self, frame: DataFrame, streaming: bool) -> DataFrame:
        """Return Spark frame unchanged (already materialized/eager)."""
        _ = streaming
        return frame

    def _pin_rows(self, frame: DataFrame) -> tuple[DataFrame, bool]:
        """Return *frame* computed once and kept, and whether it has no rows.

        A classic DataFrame is computed with ``localCheckpoint(eager=True)``:
        the check and the write both read the rows kept on the executors, and
        losing an executor before the write ends fails the write instead of
        recomputing the frame. A DataFrame without ``localCheckpoint()``,
        such as a pyspark 3.5 Spark Connect DataFrame, is persisted with
        ``MEMORY_AND_DISK`` and counted; the write reads the cached rows, but
        Spark recomputes a cached block it loses, so that write may read rows
        other than the ones counted.

        Raises:
            ValueError: When *frame* is a streaming DataFrame, whose rows
                cannot be counted before the write.
        """
        if frame.isStreaming:
            raise ValueError(
                "replace(allow_empty=False) needs a batch DataFrame to check for rows, "
                "but the frame to write is a streaming DataFrame. Read the source as a "
                "batch DataFrame or declare replace() without allow_empty=False."
            )
        if _checkpoints_locally(frame):
            pinned = frame.localCheckpoint(eager=True)
        else:
            pinned = frame.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            return pinned, _has_no_rows(pinned)
        except BaseException:
            self._release_pinned(pinned)
            raise

    def _release_pinned(self, frame: DataFrame) -> None:
        """Free the blocks :meth:`_pin_rows` kept for *frame*.

        ``DataFrame.unpersist()`` does not free the blocks of a local
        checkpoint: they belong to the RDD under the checkpointed frame's
        logical plan, which is unpersisted instead.
        """
        if _checkpoints_locally(frame):
            frame._jdf.queryExecution().logical().rdd().unpersist(False)
        else:
            frame.unpersist()

    def _predicate_to_sql(self, predicate: Any, params: Any) -> str:
        """Convert predicate to SQL."""
        return predicate_to_sql(predicate, params)

    # ====================================================================
    # Write Hooks
    # ====================================================================

    def _create(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
        partition_cols: tuple[str, ...] = (),
    ) -> None:
        """Create new Delta table."""
        writer = (
            frame.write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        self._sink(writer, target)

    def _append(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Append to existing Delta table."""
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("append")
        if schema_mode is SchemaMode.EVOLVE:
            writer = writer.option("mergeSchema", "true")
        self._sink(writer, target)

    def _replace(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite existing Delta table."""
        writer = frame.write.format("delta").option("optimizeWrite", "true").mode("overwrite")
        if schema_mode is SchemaMode.OVERWRITE:
            writer = writer.option("overwriteSchema", "true")
        self._sink(writer, target)

    def _replace_partitions(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        partition_cols: tuple[str, ...],
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite partitions present in frame."""
        rows = frame.select(*partition_cols).distinct().collect()
        if not rows:
            return

        predicates = [
            f"({' AND '.join(f'{c} = {_sql_literal(row[c])}' for c in partition_cols)})"
            for row in rows
        ]
        predicate = " OR ".join(predicates)

        writer = (
            frame.sortWithinPartitions(*partition_cols)
            .write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )
        self._sink(writer, target)

    def _replace_where(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        predicate: str,
        schema_mode: SchemaMode,
    ) -> None:
        """Overwrite rows matching SQL predicate."""
        writer = (
            frame.write.format("delta")
            .option("optimizeWrite", "true")
            .mode("overwrite")
            .option("replaceWhere", predicate)
        )
        self._sink(writer, target)

    def _upsert(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        spec: UpsertSpec,
        existing_schema: SparkPhysicalSchema,
    ) -> None:
        """Merge frame into target using Delta MERGE (delta-spark)."""
        _ = existing_schema
        merger, plan = self._matched_update_merger(frame, target, spec)
        merger.whenNotMatchedInsert(
            values=cast(dict[str, str | Column], plan.insert_values)
        ).execute()

    def _update(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        *,
        spec: UpdateSpec,
        existing_schema: SparkPhysicalSchema,
    ) -> None:
        """Merge frame into target using a matched-only Delta MERGE.

        No ``whenNotMatchedInsert`` clause is emitted, so unmatched source
        rows are ignored and the MERGE inserts zero rows by construction.
        """
        _ = existing_schema
        merger, _plan = self._matched_update_merger(frame, target, spec)
        merger.execute()

    def _matched_update_merger(
        self,
        frame: DataFrame,
        target: ResolvedTarget,
        spec: UpsertSpec | UpdateSpec,
    ) -> tuple[DeltaMergeBuilder, _MergePlan]:
        """Build the Delta MERGE up to the shared ``whenMatchedUpdate`` clause.

        Both :meth:`_upsert` and :meth:`_update` start from the same merge:
        partition-combo pre-filter, ON predicate, and the matched-update
        column map.  The caller chains its remaining clauses and executes.
        """
        if isinstance(target, CatalogTarget):
            dt = DeltaTable.forName(self._spark, target.catalog_ref.ref)
        else:
            dt = DeltaTable.forPath(self._spark, target.location.uri)

        combos = _collect_partition_combos(frame, spec.partition_cols, target.logical_ref.ref)
        merge_plan = _build_merge_plan(
            combos=combos,
            spec=spec,
            df_columns=tuple(frame.columns),
            target_alias=TARGET_ALIAS,
            source_alias=SOURCE_ALIAS,
        )

        merger = (
            dt.alias(TARGET_ALIAS)
            .merge(frame.alias(SOURCE_ALIAS), merge_plan.predicate)
            .whenMatchedUpdate(set=cast(dict[str, str | Column], merge_plan.update_set))
        )
        return merger, merge_plan

    def _read_existing_data(
        self,
        target: ResolvedTarget,
        frame: DataFrame,
        spec: HistorifySpec,
    ) -> DataFrame | None:
        """Read existing Delta data pruned to the partitions present in ``frame``.

        For ``CatalogTarget`` tables the catalog existence check is used as a
        guard.  For path targets an ``AnalysisException`` on load signals the
        table does not yet exist.

        When ``spec.partition_scope`` is set the returned DataFrame carries a
        filter on the partition columns.  Spark's Delta scan optimizer pushes
        column-level ``isin`` predicates to file-selection at execution time —
        only the matching Parquet partition directories are opened.

        Args:
            target: Resolved Delta target (catalog or path).
            frame:  Incoming Spark DataFrame; partition-column values are
                    collected with a small distinct action.
            spec:   Historify spec; ``partition_scope`` carries column names.

        Returns:
            DataFrame of existing rows (possibly filtered), or ``None`` if the
            table does not yet exist.
        """
        existing = self._load_existing_df(target)
        if existing is None or not spec.partition_scope:
            return existing
        return _filter_to_partitions(existing, frame, spec.partition_scope)

    def _load_existing_df(self, target: ResolvedTarget) -> DataFrame | None:
        """Load the full existing DataFrame from a catalog or path target."""
        if isinstance(target, CatalogTarget):
            if not self._spark.catalog.tableExists(target.catalog_ref.ref):
                return None
            return self._spark.table(target.catalog_ref.ref)
        with contextlib.suppress(AnalysisException):
            return self._spark.read.format("delta").load(target.location.uri)
        return None

    def _historify(
        self,
        frame: DataFrame,
        existing: DataFrame | None,
        target: ResolvedTarget,
        *,
        spec: HistorifySpec,
        params_instance: Any,
    ) -> HistorifyRepairReport | None:
        """Run SCD Type 2 transform and write result via existing write hooks."""
        result, report = scd2_transform(
            SparkHistorifyBackend(), frame, existing, spec, params_instance
        )
        if report is not None:
            _log.warning(
                "historify temporal rerun rewound future history: "
                "affected_key_count=%d dates_requiring_rerun=%s warnings=%s",
                len(report.affected_keys),
                [str(d) for d in report.dates_requiring_rerun],
                list(report.warnings),
            )
        if existing is None:
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
        return report

    def _write_file(
        self,
        frame: DataFrame,
        spec: FileSpec,
        params_instance: Any,
        /,
        *,
        streaming: bool,
    ) -> None:
        """Write to file (CSV, JSON, Parquet), resolving alias and template."""
        _ = streaming
        resolved = self._resolve_file_spec(spec, params_instance)
        write = resolve_format_handler(resolved.format, _FILE_WRITERS)
        write(frame, resolved)

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
    # Helpers
    # ====================================================================

    def _sink(self, writer: Any, target: ResolvedTarget) -> None:
        """Finalize write based on target type."""
        if isinstance(target, CatalogTarget):
            writer.saveAsTable(target.catalog_ref.ref)
        else:
            writer.save(target.location.uri)


__all__ = ["SparkTargetWriter", "_collect_partition_combos"]


# ========================================================================
# File writers — one per format Spark can write
# ========================================================================


def _checkpoints_locally(frame: object) -> bool:
    """Whether *frame* supports ``localCheckpoint()``.

    A pyspark 3.5 Spark Connect DataFrame is not a :class:`pyspark.sql.DataFrame`
    and raises ``NOT_IMPLEMENTED`` on ``localCheckpoint()``.
    """
    return isinstance(frame, DataFrame)


def _has_no_rows(pinned: DataFrame) -> bool:
    """Whether the pinned frame has no rows.

    A persisted frame is counted, which computes and caches every partition
    for the write; ``isEmpty()`` would stop at the first row.
    """
    if _checkpoints_locally(pinned):
        return bool(pinned.isEmpty())
    return int(pinned.count()) == 0


def _write_delta_file(frame: DataFrame, spec: FileSpec) -> None:
    frame.write.format("delta").mode("overwrite").save(spec.path)


def _write_csv_file(frame: DataFrame, spec: FileSpec) -> None:
    csv_opts = write_options_or_default(spec.write_options, CsvWriteOptions)
    writer = (
        frame.write.mode("overwrite")
        .option("sep", csv_opts.separator)
        .option("header", str(csv_opts.has_header).lower())
    )
    for key, value in csv_opts.kwargs:
        writer = writer.option(key, str(value))
    writer.csv(spec.path)


def _write_json_file(frame: DataFrame, spec: FileSpec) -> None:
    json_opts = write_options_or_default(spec.write_options, JsonWriteOptions)
    writer = frame.write.mode("overwrite")
    for key, value in json_opts.kwargs:
        writer = writer.option(key, str(value))
    writer.json(spec.path)


def _write_parquet_file(frame: DataFrame, spec: FileSpec) -> None:
    parquet_opts = write_options_or_default(spec.write_options, ParquetWriteOptions)
    writer = frame.write.mode("overwrite").option("compression", parquet_opts.compression)
    for key, value in parquet_opts.kwargs:
        writer = writer.option(key, str(value))
    writer.parquet(spec.path)


_FILE_WRITERS: dict[Format, Callable[[DataFrame, FileSpec], None]] = {
    Format.DELTA: _write_delta_file,
    Format.CSV: _write_csv_file,
    Format.JSON: _write_json_file,
    Format.PARQUET: _write_parquet_file,
}


def _filter_to_partitions(
    existing: DataFrame,
    incoming: DataFrame,
    partition_scope: tuple[str, ...],
) -> DataFrame:
    """Return ``existing`` filtered to the exact partition combos in ``incoming``.

    Collects the distinct partition-column combinations from ``incoming`` and
    builds an ``OR`` of ``AND`` equalities on ``existing``.  Spark's Delta scan
    optimizer pushes these predicates to file-selection, so only the partition
    directories that match the *exact* combinations are opened — no bounding
    hyper-rectangle over-read.

    Args:
        existing:        Full logical plan for the existing Delta table.
        incoming:        Incoming Spark DataFrame; only partition cols are used.
        partition_scope: Partition column names.

    Returns:
        ``existing`` with an exact-partition filter applied (still lazy).
    """
    import operator
    from functools import reduce

    from pyspark.sql import functions as F

    partition_rows = incoming.select(*partition_scope).distinct().collect()
    if not partition_rows:
        return existing.limit(0)

    predicates: list[Any] = []
    for row in partition_rows:
        ands = [F.col(col) == row[col] for col in partition_scope]
        predicates.append(reduce(operator.and_, ands))
    return existing.filter(reduce(operator.or_, predicates))


def _collect_partition_combos(
    frame: DataFrame,
    partition_cols: tuple[str, ...],
    table_ref: str,
) -> list[dict[str, Any]]:
    """Collect unique partition value combinations."""
    if not partition_cols:
        _warn_no_partition_cols(table_ref)
        return []
    combos = frame.select(*partition_cols).distinct().collect()
    result = [{c: row[c] for c in partition_cols} for row in combos]
    _log_partition_combos(result, table_ref)
    return result


def _sql_literal(value: Any) -> str:
    """Format *value* as a SQL literal safe for use in a predicate string.

    Handles the types typically found in Delta partition columns: integers,
    floats, strings, booleans, ``None``, and date/datetime objects (via their
    ``str()`` representation which produces ISO-8601 strings).

    Args:
        value: Python scalar value from a Spark Row.

    Returns:
        SQL literal string.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _spark_record_schema(record: LineageRecord) -> T.StructType:
    cols = get_lineage_schema(type(record))
    return T.StructType(
        [T.StructField(c.name, loom_type_to_spark(c.dtype), c.nullable) for c in cols]
    )
