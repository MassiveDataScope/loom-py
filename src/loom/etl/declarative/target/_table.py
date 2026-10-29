"""TABLE target variant specs — one frozen dataclass per write semantic.

All variants carry exactly the fields required for their write mode.
Format is always ``Format.DELTA`` — implicit, not stored.

Internal module — import from :mod:`loom.etl.declarative.target`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from loom.etl.declarative.expr._predicate import PredicateNode
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._schema_mode import SchemaMode


@dataclass(frozen=True)
class AppendSpec:
    """Append rows to an existing Delta table.

    Args:
        table_ref:   Logical table reference.
        schema_mode: Schema evolution strategy.  Defaults to ``STRICT``.
        streaming:   When ``True`` requests a streaming Arrow write.  In
                     this release the Polars backend honours the flag
                     only for :class:`ReplacePartitionsSpec`; other modes
                     accept the flag for forward-compatibility but still
                     materialise the frame.
    """

    table_ref: TableRef
    schema_mode: SchemaMode = SchemaMode.STRICT
    streaming: bool = False


@dataclass(frozen=True)
class ReplaceSpec:
    """Full overwrite of a Delta table.

    Args:
        table_ref:   Logical table reference.
        schema_mode: Schema evolution strategy.  ``OVERWRITE`` replaces the
                     table schema alongside the data.
        streaming:   Set by ``replace(streaming=...)``; no backend reads it
                     for a full replace.  The Polars backend collects the
                     frame with the streaming engine when the step sets
                     ``streaming = True``.
        allow_empty: When ``False`` the writer raises
                     :class:`~loom.etl.EmptyReplaceError` instead of
                     writing a frame with no rows.
    """

    table_ref: TableRef
    schema_mode: SchemaMode = SchemaMode.STRICT
    streaming: bool = False
    allow_empty: bool = True


@dataclass(frozen=True)
class ReplacePartitionsSpec:
    """Overwrite only the partitions present in the source frame.

    The writer collects distinct partition-column combinations from the
    frame and builds the ``replaceWhere`` predicate at write time.

    Args:
        table_ref:      Logical table reference.
        partition_cols: Partition columns used to build the predicate.
        schema_mode:    Schema evolution strategy.
        streaming:      When ``True`` the Polars backend writes via an
                        Arrow ``RecordBatchReader`` and computes the
                        partition predicate via a cheap projection over
                        the spool, bounding peak memory.
    """

    table_ref: TableRef
    partition_cols: tuple[str, ...]
    schema_mode: SchemaMode = SchemaMode.STRICT
    streaming: bool = False
    require_physical: bool = False
    """When ``True`` the writer refuses unless the table is PHYSICALLY
    partitioned by every column in ``partition_cols`` — the true
    partition-replace, where the predicate maps to whole-partition file
    pruning instead of a row-level rewrite."""


@dataclass(frozen=True)
class ReplaceWhereSpec:
    """Overwrite rows matching a static predicate (Delta ``replaceWhere``).

    The predicate is resolved from run params at write time and passed
    directly to delta-rs.

    Args:
        table_ref:         Logical table reference.
        replace_predicate: Predicate node built with the col/params DSL.
        schema_mode:       Schema evolution strategy.
        streaming:         When ``True`` requests a streaming Arrow write.
                           In this release the Polars backend honours
                           the flag only for
                           :class:`ReplacePartitionsSpec`; other modes
                           accept the flag for forward-compatibility but
                           still materialise the frame.
    """

    table_ref: TableRef
    replace_predicate: PredicateNode
    schema_mode: SchemaMode = SchemaMode.STRICT
    streaming: bool = False


@dataclass(frozen=True)
class UpsertSpec:
    """Merge rows into a Delta table using explicit merge keys (UPSERT).

    Args:
        table_ref:      Logical table reference.
        upsert_keys:    Columns that uniquely identify a row (MERGE ON).
        schema_mode:    Schema evolution strategy.
        partition_cols: Partition columns added to the MERGE ON predicate
                        for file-level pruning.  Strongly recommended for
                        large tables.
        upsert_exclude: Columns excluded from ``UPDATE SET`` on match.
                        Mutually exclusive with *upsert_include*.
        upsert_include: Explicit allow-list for ``UPDATE SET``.
                        Mutually exclusive with *upsert_exclude*.
        delete_flag:    Optional boolean source column. Rows where it is true
                        delete the matched target row and are not inserted
                        when unmatched. A false or null value keeps the normal
                        update or insert; null is read as false. The column is
                        removed before schema alignment, so it is not written
                        and does not appear in the update or insert maps.
    """

    table_ref: TableRef
    upsert_keys: tuple[str, ...]
    schema_mode: SchemaMode = SchemaMode.STRICT
    partition_cols: tuple[str, ...] = field(default_factory=tuple)
    upsert_exclude: tuple[str, ...] = field(default_factory=tuple)
    upsert_include: tuple[str, ...] = field(default_factory=tuple)
    delete_flag: str | None = None


def upsert_delete_flag_conflict(spec: UpsertSpec) -> str | None:
    """Return the upsert option ``delete_flag`` names, or ``None`` when it is usable.

    The option is ``keys``, ``partition_cols``, ``exclude``, or ``include``.
    ``None`` when ``delete_flag`` is unset or names none of those options.
    """
    flag = spec.delete_flag
    if flag is None:
        return None
    if flag in spec.upsert_keys:
        return "keys"
    if flag in spec.partition_cols:
        return "partition_cols"
    if flag in spec.upsert_exclude:
        return "exclude"
    if flag in spec.upsert_include:
        return "include"
    return None


@dataclass(frozen=True)
class UpdateSpec:
    """Merge rows into a Delta table updating matches only (matched-only MERGE).

    Same MERGE machinery as :class:`UpsertSpec`, minus the insert branch:
    source rows without a matching target row are silently ignored, so the
    write can never grow the table.  The ``upsert_*`` read-only aliases
    satisfy the shared merge-plan protocol without leaking upsert vocabulary
    into the public field names.

    Args:
        table_ref:      Logical table reference.
        keys:           Columns that uniquely identify a row (MERGE ON).
        schema_mode:    Schema evolution strategy.
        partition_cols: Partition columns added to the MERGE ON predicate
                        for file-level pruning.  Strongly recommended for
                        large tables.
        exclude:        Columns excluded from ``UPDATE SET`` on match.
                        Mutually exclusive with *include*.
        include:        Explicit allow-list for ``UPDATE SET``.
                        Mutually exclusive with *exclude*.
    """

    table_ref: TableRef
    keys: tuple[str, ...]
    schema_mode: SchemaMode = SchemaMode.STRICT
    partition_cols: tuple[str, ...] = field(default_factory=tuple)
    exclude: tuple[str, ...] = field(default_factory=tuple)
    include: tuple[str, ...] = field(default_factory=tuple)

    @property
    def upsert_keys(self) -> tuple[str, ...]:
        """Merge-protocol alias for :attr:`keys`."""
        return self.keys

    @property
    def upsert_exclude(self) -> tuple[str, ...]:
        """Merge-protocol alias for :attr:`exclude`."""
        return self.exclude

    @property
    def upsert_include(self) -> tuple[str, ...]:
        """Merge-protocol alias for :attr:`include`."""
        return self.include

    @property
    def delete_flag(self) -> None:
        """Merge-protocol field: an update never deletes, so it has no delete flag."""
        return None
