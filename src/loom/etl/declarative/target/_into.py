"""Target builder types — IntoTable, IntoFile, IntoTemp, SchemaMode.

Public API — import from :mod:`loom.etl.declarative.target` or :mod:`loom.etl.declarative`.

Each ETL step declares exactly one target.  Write mode and schema mode are
set by chaining a write-intent method::

    IntoTable("staging.orders").append()
    IntoTable("staging.orders").replace()
    IntoTable("staging.orders").replace(schema=SchemaMode.EVOLVE)
    IntoTable("staging.orders").replace(schema=SchemaMode.OVERWRITE)
    IntoTable("staging.orders").replace_partitions("year", "month")
    IntoTable("staging.orders").replace_where(
        (col("year") == params.run_date.year) & (col("month") == params.run_date.month)
    )
    IntoTable("staging.orders").upsert(keys=("order_id",))
    IntoTable("staging.orders").update(keys=("order_id",), include=("status",))
    IntoFile("s3://exports/report_{run_date}.csv", format=Format.CSV)
"""

from __future__ import annotations

from typing import Any

from loom.core.routing import LogicalRef
from loom.etl.checkpoint import CheckpointScope
from loom.etl.declarative._format import Format
from loom.etl.declarative._utils import _clone_slots
from loom.etl.declarative._write_options import WriteOptions
from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._predicate import AndPred, EqPred, PredicateNode
from loom.etl.declarative.expr._refs import TableRef, UnboundColumnRef
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._schema_mode import SchemaMode
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec


def _build_eq_predicate(values: dict[str, ParamExpr]) -> PredicateNode:
    items = list(values.items())
    head_col, head_expr = items[0]
    result: PredicateNode = EqPred(left=UnboundColumnRef(head_col), right=head_expr)
    for col_name, expr in items[1:]:
        result = AndPred(left=result, right=EqPred(left=UnboundColumnRef(col_name), right=expr))
    return result


class IntoTable:
    """Declare a Delta table as the ETL step target.

    The write mode is set by chaining one of:
    :meth:`append`, :meth:`replace`, :meth:`replace_partitions`,
    :meth:`replace_where`, :meth:`upsert`, :meth:`update`.

    Args:
        ref: Logical table reference — ``str`` or :class:`~loom.etl.TableRef`.

    Example::

        # Full replace
        target = IntoTable("staging.orders").replace()

        # Replace only the partitions present in the batch
        target = IntoTable("staging.orders").replace_partitions("year", "month")

        # Replace a date-range for backfill
        target = IntoTable("staging.orders").replace_where(
            col("date").between(params.start_date, params.end_date)
        )
    """

    __slots__ = ("_ref", "_spec")

    def __init__(self, ref: str | LogicalRef | TableRef) -> None:
        table_ref = ref if isinstance(ref, TableRef) else TableRef(ref)
        self._ref = table_ref
        # Default write mode is REPLACE.  Call a write-mode method to change it.
        self._spec: Any = ReplaceSpec(table_ref=table_ref)

    def append(
        self,
        *,
        schema: SchemaMode = SchemaMode.STRICT,
        streaming: bool = False,
    ) -> IntoTable:
        """Write mode: append rows to the target table.

        Args:
            schema:    Schema evolution strategy.  Defaults to
                       :attr:`~SchemaMode.STRICT`.
            streaming: When ``True`` the Polars backend writes via an Arrow
                       ``RecordBatchReader`` instead of materialising.

        Returns:
            New ``IntoTable`` with APPEND mode.
        """
        return self._with(AppendSpec(table_ref=self._ref, schema_mode=schema, streaming=streaming))

    def replace(
        self,
        *,
        schema: SchemaMode = SchemaMode.STRICT,
        streaming: bool = False,
        allow_empty: bool = True,
    ) -> IntoTable:
        """Write mode: full replace of the target table.

        Overwrites all data in the table.  Use :meth:`replace_partitions` for
        partition-scoped overwrite or :meth:`replace_where` for predicate-scoped.

        Args:
            schema:      Schema evolution strategy.  Use
                         :attr:`~SchemaMode.OVERWRITE` to replace the table
                         schema entirely alongside the data.
            streaming:   Kept on the :class:`ReplaceSpec`; a full replace
                         does not read it.  The Polars backend collects the
                         frame with the streaming engine when the step sets
                         ``streaming = True``.
            allow_empty: When ``False``, a frame with no rows raises
                         :class:`~loom.etl.EmptyReplaceError` and the table
                         is neither replaced nor created.  Use it for a
                         table, such as a dimension, that an empty read must
                         never wipe.  Defaults to ``True``: an empty frame
                         replaces the table with an empty one.  The Spark
                         backend refuses a streaming DataFrame with
                         :exc:`ValueError`, since it cannot count its rows.

        Returns:
            New ``IntoTable`` with REPLACE mode.

        Example::

            target = IntoTable("silver.dim_dealer").replace(allow_empty=False)
        """
        return self._with(
            ReplaceSpec(
                table_ref=self._ref,
                schema_mode=schema,
                streaming=streaming,
                allow_empty=allow_empty,
            )
        )

    def replace_partitions(
        self,
        *cols: str,
        schema: SchemaMode = SchemaMode.STRICT,
        streaming: bool = False,
    ) -> IntoTable:
        """Write mode: replace the partitions present in the batch frame.

        Collects the distinct partition values from the frame at write time and
        builds the replace predicate dynamically — no params required.

        Despite the name, this never requires the table to be physically
        partitioned: the write is a Delta replaceWhere over the collected
        VALUES. When the table is not partitioned and the columns are plain
        keys (a date column on an unpartitioned fact), prefer the alias
        :meth:`replace_matching`, which says what actually happens.

        For replacing a specific partition whose values come from run params,
        use :meth:`replace_partition`.

        Args:
            *cols:   Partition column names to collect from the frame.
            schema:  Schema evolution strategy.

        Returns:
            New ``IntoTable`` with REPLACE_PARTITIONS mode.

        Raises:
            ValueError: If no column names are provided.

        Example::

            target = IntoTable("staging.orders").replace_partitions("year", "month")
        """
        if not cols:
            raise ValueError("replace_partitions: at least one partition column name is required.")
        return self._with(
            ReplacePartitionsSpec(
                table_ref=self._ref,
                partition_cols=cols,
                schema_mode=schema,
                streaming=streaming,
            )
        )

    def replace_matching(
        self,
        *cols: str,
        schema: SchemaMode = SchemaMode.STRICT,
        streaming: bool = False,
    ) -> IntoTable:
        """Write mode: replace the rows matching the frame's values of *cols*.

        Alias of :meth:`replace_partitions` with the semantics in the name: the
        writer collects the distinct values of *cols* present in the batch frame
        and issues a Delta replaceWhere over them — physical partitioning is
        never required. This is the natural write mode for a model table built
        slice by slice (a fact keyed by day, a queue keyed by its snapshot
        date): the run recomputes its slices whole and substitutes exactly
        those rows, idempotently.

        Args:
            *cols:   Column names whose frame values define the replaced rows.
            schema:  Schema evolution strategy.

        Returns:
            New ``IntoTable`` with REPLACE_PARTITIONS mode.

        Raises:
            ValueError: If no column names are provided.

        Example::

            target = IntoTable("mg.fact_family_matriculation").replace_matching("day")
        """
        if not cols:
            raise ValueError("replace_matching: at least one column name is required.")
        return self.replace_partitions(*cols, schema=schema, streaming=streaming)

    def replace_physical_partitions(
        self,
        *cols: str,
        schema: SchemaMode = SchemaMode.STRICT,
        streaming: bool = False,
    ) -> IntoTable:
        """Write mode: the TRUE partition replace — refuses on an unpartitioned table.

        Same collected-values predicate as :meth:`replace_partitions`, plus a
        write-time check that the Delta table is physically partitioned by
        every column in *cols*: on a partitioned table the replaceWhere prunes
        whole partitions instead of rewriting row groups, and asking for that
        on a table that cannot deliver it is a design smell this mode turns
        into an error. For value-based replacement on an unpartitioned table
        use :meth:`replace_matching`.

        Args:
            *cols:   Physical partition column names to collect from the frame.
            schema:  Schema evolution strategy.

        Returns:
            New ``IntoTable`` with REPLACE_PARTITIONS mode (physical check on).

        Raises:
            ValueError: If no column names are provided.

        Example::

            target = IntoTable("pg.snap_st_dgt_registration").replace_physical_partitions(
                "year", "month", "day"
            )
        """
        if not cols:
            raise ValueError(
                "replace_physical_partitions: at least one partition column name is required."
            )
        return self._with(
            ReplacePartitionsSpec(
                table_ref=self._ref,
                partition_cols=cols,
                schema_mode=schema,
                streaming=streaming,
                require_physical=True,
            )
        )

    def replace_partition(
        self,
        *,
        schema: SchemaMode = SchemaMode.STRICT,
        **partition: ParamExpr,
    ) -> IntoTable:
        """Write mode: replace a specific partition identified by exact column values from params.

        Use when the partition to replace is known at pipeline design time and
        its values come from run params.  The writer resolves each value against
        the concrete params at runtime and issues a Delta ``replaceWhere`` on the
        resulting equality predicate — no collect required.

        For dynamic replacement (values inferred from the batch), use
        :meth:`replace_partitions`.  For arbitrary predicates, use
        :meth:`replace_where`.

        Args:
            schema: Schema evolution strategy.
            **partition:
                Partition column name →
                :class:`~loom.etl.declarative.expr._params.ParamExpr` pairs
                (e.g. ``year=params.run_date.year``).

        Returns:
            New ``IntoTable`` with REPLACE_WHERE mode.

        Raises:
            ValueError: If no column=value pairs are provided.

        Example::

            target = IntoTable("staging.orders").replace_partition(
                year=params.run_date.year,
                month=params.run_date.month,
            )
        """
        if not partition:
            raise ValueError("replace_partition: at least one column=value pair is required.")
        predicate = _build_eq_predicate(partition)
        return self._with(
            ReplaceWhereSpec(
                table_ref=self._ref,
                replace_predicate=predicate,
                schema_mode=schema,
            )
        )

    def replace_where(
        self,
        predicate: PredicateNode,
        *,
        schema: SchemaMode = SchemaMode.STRICT,
        streaming: bool = False,
    ) -> IntoTable:
        """Write mode: replace rows matching an explicit predicate.

        The predicate is resolved against the run params and passed to Delta as
        ``replaceWhere``.  Only the matching data is overwritten — Delta uses
        partition pruning so only affected files are rewritten.

        Typical use case: backfill / reprocessing a date range.

        Args:
            predicate: Predicate built with the :func:`~loom.etl.col` /
                       :data:`~loom.etl.params` DSL.
            schema:    Schema evolution strategy.

        Returns:
            New ``IntoTable`` with REPLACE_WHERE mode.

        Example::

            target = IntoTable("staging.orders").replace_where(
                col("date").between(params.start_date, params.end_date)
            )
        """
        return self._with(
            ReplaceWhereSpec(
                table_ref=self._ref,
                replace_predicate=predicate,
                schema_mode=schema,
                streaming=streaming,
            )
        )

    def upsert(
        self,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...] = (),
        exclude: tuple[str, ...] = (),
        include: tuple[str, ...] = (),
        schema: SchemaMode = SchemaMode.STRICT,
    ) -> IntoTable:
        """Write mode: merge rows using the given key columns (UPSERT / MERGE).

        On first write the table is created from the frame.  Subsequent writes
        issue a Delta MERGE: matched rows are updated, unmatched rows are
        inserted.

        Declaring ``partition_cols`` is strongly recommended for large tables —
        it allows Delta to prune files at the log level before evaluating the
        join condition.  Without it, every MERGE forces a full table scan.

        Args:
            keys:           Columns that uniquely identify a row.  Used in the
                            MERGE ``ON`` join condition.
            partition_cols: Partition columns to include in the MERGE ``ON``
                            predicate and for Delta log pruning.  Must be a
                            subset of the frame columns.
            exclude:        Columns to exclude from ``UPDATE SET`` on match.
                            Keys and partition columns are always excluded.
                            Mutually exclusive with *include*.
            include:        Explicit allow-list of columns to update on match.
                            Keys and partition columns are always excluded even
                            if listed here.  Mutually exclusive with *exclude*.
            schema:         Schema evolution strategy.  Defaults to
                            :attr:`~SchemaMode.STRICT`.

        Returns:
            New ``IntoTable`` with UPSERT mode.

        Example::

            target = IntoTable("events.orders").upsert(
                keys=("order_id",),
                partition_cols=("year", "month"),
                exclude=("created_at",),
            )
        """
        return self._with(
            UpsertSpec(
                table_ref=self._ref,
                upsert_keys=keys,
                partition_cols=partition_cols,
                upsert_exclude=exclude,
                upsert_include=include,
                schema_mode=schema,
            )
        )

    def update(
        self,
        *,
        keys: tuple[str, ...],
        partition_cols: tuple[str, ...] = (),
        exclude: tuple[str, ...] = (),
        include: tuple[str, ...] = (),
        schema: SchemaMode = SchemaMode.STRICT,
    ) -> IntoTable:
        """Write mode: update matching rows only (matched-only MERGE).

        The insert-less sibling of :meth:`upsert`: rows whose keys match are
        updated; source rows without a match are IGNORED — nothing is ever
        inserted.  Use it when an insert would be a bug, e.g. repairing
        columns of an existing table.  Because it never inserts, the target
        table must already exist — the first-write creation path of
        :meth:`upsert` does not apply and a missing table is a write error.

        Declaring ``partition_cols`` is strongly recommended for large tables —
        it allows Delta to prune files at the log level before evaluating the
        join condition.  Without it, every MERGE forces a full table scan.

        Args:
            keys:           Columns that uniquely identify a row.  Used in the
                            MERGE ``ON`` join condition.
            partition_cols: Partition columns to include in the MERGE ``ON``
                            predicate and for Delta log pruning.  Must be a
                            subset of the frame columns.
            exclude:        Columns to exclude from ``UPDATE SET`` on match.
                            Keys and partition columns are always excluded.
                            Mutually exclusive with *include*.
            include:        Explicit allow-list of columns to update on match.
                            Keys and partition columns are always excluded even
                            if listed here.  Mutually exclusive with *exclude*.
            schema:         Schema evolution strategy.  Defaults to
                            :attr:`~SchemaMode.STRICT`.

        Returns:
            New ``IntoTable`` with UPDATE mode.

        Example::

            # Repair one column of an existing fact — never add rows
            target = IntoTable("mg.fact_orders").update(
                keys=("order_id",),
                partition_cols=("year", "month"),
                include=("family_id",),
            )
        """
        return self._with(
            UpdateSpec(
                table_ref=self._ref,
                keys=keys,
                partition_cols=partition_cols,
                exclude=exclude,
                include=include,
                schema_mode=schema,
            )
        )

    def _with(self, spec: Any) -> IntoTable:
        new = _clone_slots(self, IntoTable, IntoTable.__slots__)
        object.__setattr__(new, "_spec", spec)
        return new

    def _to_spec(self) -> Any:
        return self._spec

    def __repr__(self) -> str:
        mode = type(self._spec).__name__.removesuffix("Spec").lower()
        return f"IntoTable({self._ref.ref!r}, mode={mode!r})"


class IntoFile:
    """Declare a file as the ETL step target (CSV, JSON, XLSX, Parquet).

    The ``path`` supports ``{field_name}`` template placeholders resolved
    from params at runtime.

    Args:
        path:   File path or template, e.g. ``"s3://exports/orders_{run_date}.csv"``.
        format: :class:`~loom.etl.Format` of the output file.

    Example::

        target = IntoFile("s3://exports/summary_{run_date}.xlsx", format=Format.XLSX)
    """

    __slots__ = ("_path", "_format", "_is_alias", "_write_options")

    def __init__(self, path: str, *, format: Format) -> None:
        self._path = path
        self._format = format
        self._is_alias = False
        self._write_options: WriteOptions | None = None

    @classmethod
    def alias(cls, name: str, *, format: Format) -> IntoFile:
        """Declare a file target by logical alias resolved from storage config.

        The physical URI is looked up in ``storage.files`` at runtime via the
        injected :class:`~loom.etl.storage.FileLocator`.  Use this form when
        the path is environment-specific and should not be hard-coded in the
        pipeline.

        Args:
            name:   Logical file alias matching a ``storage.files[].name``
                    entry in the config YAML.
            format: :class:`~loom.etl.Format` of the output file.

        Returns:
            New ``IntoFile`` backed by a logical alias.

        Example::

            target = IntoFile.alias("exports_daily", format=Format.PARQUET)
        """
        instance = cls.__new__(cls)
        instance._path = name
        instance._format = format
        instance._is_alias = True
        instance._write_options = None
        return instance

    def with_options(self, options: WriteOptions) -> IntoFile:
        """Return a new ``IntoFile`` with format-specific write options.

        Args:
            options: Format-specific write options — use
                     :class:`~loom.etl.CsvWriteOptions` or
                     :class:`~loom.etl.ParquetWriteOptions`.

        Returns:
            New ``IntoFile`` with the options applied at write time.

        Example::

            target = IntoFile("s3://exports/report.csv", format=Format.CSV)
                .with_options(CsvWriteOptions(separator=";"))
        """
        new = _clone_slots(self, IntoFile, IntoFile.__slots__)
        object.__setattr__(new, "_write_options", options)
        return new

    def _to_spec(self) -> Any:
        return FileSpec(
            path=self._path,
            format=self._format,
            is_alias=self._is_alias,
            write_options=self._write_options,
        )

    def __repr__(self) -> str:
        return f"IntoFile({self._path!r}, format={self._format!r})"


class IntoTemp:
    """Declare an intermediate result that bypasses Delta and lives in tmp storage.

    The physical format is chosen automatically by
    :class:`~loom.etl.checkpoint.CheckpointStore` based on the DataFrame
    type returned by ``execute()``:

    * **Polars** — Arrow IPC via ``sink_ipc()`` (streaming write, no collect)
      and ``scan_ipc()`` (lazy, memory-mapped read with predicate pushdown).
    * **Spark** — Parquet directory via ``df.write.parquet()`` and
      ``spark.read.parquet()``.  Cuts the lineage DAG; Photon-optimised.

    Use :class:`~loom.etl.FromTemp` in a downstream step to consume the result.

    Args:
        name:   Logical name identifying this intermediate.  By default the
                name must be unique across the pipeline — the compiler raises
                if two steps write to the same name.
        scope:  Lifetime scope.  Defaults to :attr:`~loom.etl.CheckpointScope.RUN`.
        append: When ``True``, multiple steps may write to this name; their
                outputs are concatenated and exposed as one logical intermediate
                (fan-in pattern).  All writers for a given name must agree on
                the same ``append`` value — mixing ``True`` and ``False`` is a
                compile-time error.

    Example::

        # strict — only one step may write "normalized_orders"
        target = IntoTemp("normalized_orders")

        # fan-in — multiple partition steps write to the same intermediate
        target = IntoTemp("order_parts", append=True)
    """

    __slots__ = ("_append", "_name", "_scope")

    def __init__(
        self,
        name: str,
        *,
        scope: CheckpointScope = CheckpointScope.RUN,
        append: bool = False,
    ) -> None:
        self._name = name
        self._scope = scope
        self._append = append

    @property
    def temp_name(self) -> str:
        """Logical name of this intermediate."""
        return self._name

    @property
    def scope(self) -> CheckpointScope:
        """Lifetime scope."""
        return self._scope

    @property
    def append(self) -> bool:
        """Whether multiple steps may write to this name (fan-in)."""
        return self._append

    def _to_spec(self) -> Any:
        if self._append:
            return TempFanInSpec(temp_name=self._name, temp_scope=self._scope)
        return TempSpec(temp_name=self._name, temp_scope=self._scope)

    def __repr__(self) -> str:
        return f"IntoTemp({self._name!r}, scope={self._scope!r}, append={self._append!r})"
