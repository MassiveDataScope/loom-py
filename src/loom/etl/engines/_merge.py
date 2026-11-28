"""Shared helpers for UPSERT/MERGE predicate and column-set construction.

Pure functions — no I/O, no backend dependency.  Used by both the Polars
and Spark writers to build the Delta MERGE ``ON`` predicate and the update /
insert column maps.

Pre-filter strategy
-------------------
Without a partition pre-filter the MERGE engine must scan the entire target
table to evaluate the join condition.  By prepending a literal partition
pre-filter we give Delta the information it needs to prune files at the log
level before the join runs::

    ((t.year=2023 AND t.month=1) OR (t.year=2024 AND t.month=3))
    AND (t.id = s.id AND t.year = s.year AND t.month = s.month)

The partition combos are collected from the source frame *before* the MERGE
call (Polars: ``df.unique(subset=…).to_dicts()``; Spark:
``df.select(…).distinct().collect()``).  Partition columns always have low
cardinality so the collect is cheap.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from loom.etl.declarative.target._table import MergeSpec
from loom.etl.engines._predicate import sql_literal
from loom.etl.event import EtlEvent, EtlKey

_log = logging.getLogger(__name__)

TARGET_ALIAS = "t"


SOURCE_ALIAS = "s"
_SQL_AND = " AND "


@dataclass(frozen=True)
class _MergePlan:
    """Prepared merge payload reused by backend-specific Delta APIs.

    The three predicate fields are ``None`` when the spec has no
    ``delete_flag``. A null flag is false: the delete predicate is
    ``coalesce(alias.flag, false)`` and the update and insert predicates
    are its negation.
    """

    predicate: str
    update_set: dict[str, str]
    insert_values: dict[str, str]
    matched_delete_predicate: str | None = None
    matched_update_predicate: str | None = None
    not_matched_insert_predicate: str | None = None


def _build_join_clause(
    keys: tuple[str, ...],
    partition_cols: tuple[str, ...],
    target_alias: str,
    source_alias: str,
) -> str:
    """Build the equality join clause from merge keys and partition columns.

    Args:
        keys:           Columns that uniquely identify a row.
        partition_cols: Partition columns (joined on as well).
        target_alias:   Alias for the target table in the MERGE statement.
        source_alias:   Alias for the source frame in the MERGE statement.

    Returns:
        SQL string, e.g. ``t.id = s.id AND t.year = s.year``
    """
    all_cols = (*keys, *partition_cols)
    return _SQL_AND.join(f"{target_alias}.{col} = {source_alias}.{col}" for col in all_cols)


def _build_upsert_predicate(
    combos: list[dict[str, Any]],
    spec: MergeSpec,
    target_alias: str,
    source_alias: str,
) -> str:
    """Build the full MERGE ON predicate.

    When partition combos are present, prepends a literal partition pre-filter
    so Delta can prune files at the log level before evaluating the join.

    Args:
        combos:       Distinct partition value combinations from the source frame.
                      Pass an empty list when no partition columns are declared.
        spec:         Target spec carrying ``keys`` and ``partition_cols``.
        target_alias: Target table alias.
        source_alias: Source frame alias.

    Returns:
        SQL predicate string for the MERGE ON clause.
    """
    join_clause = _build_join_clause(spec.keys, spec.partition_cols, target_alias, source_alias)
    if not combos or not spec.partition_cols:
        return join_clause
    partition_filter = _build_partition_predicate(combos, spec.partition_cols, alias=target_alias)
    return f"({partition_filter}) AND ({join_clause})"


def _build_upsert_update_cols(
    df_columns: tuple[str, ...],
    spec: MergeSpec,
) -> tuple[str, ...]:
    """Compute the columns to update on MATCH, respecting exclude/include.

    Keys and partition columns are always excluded from UPDATE SET — updating
    them would invalidate the match condition.

    If ``include`` is set, only those columns are updated (minus the
    always-excluded set).  If ``exclude`` is set, those additional
    columns are skipped.  The two options are mutually exclusive.

    Args:
        df_columns: Column names from the source frame, in original order.
        spec:       Target merge spec.

    Returns:
        Ordered tuple of column names to appear in UPDATE SET.
    """
    always_excluded = frozenset(spec.keys) | frozenset(spec.partition_cols)
    if spec.include:
        return tuple(c for c in spec.include if c not in always_excluded)
    excluded = always_excluded | frozenset(spec.exclude)
    return tuple(c for c in df_columns if c not in excluded)


def _build_update_set(
    update_cols: tuple[str, ...],
    source_alias: str,
) -> dict[str, str]:
    """Map each update column to its source-aliased expression.

    Args:
        update_cols:  Columns to include in UPDATE SET.
        source_alias: Source frame alias (e.g. ``"s"``).

    Returns:
        Dict mapping column name → ``"s.column"`` expression,
        e.g. ``{"name": "s.name", "value": "s.value"}``.
    """
    return {col: f"{source_alias}.{col}" for col in update_cols}


def _build_insert_values(
    all_columns: tuple[str, ...],
    source_alias: str,
) -> dict[str, str]:
    """Map every source column to its alias expression for NOT MATCHED INSERT.

    Args:
        all_columns:  All column names from the source frame.
        source_alias: Source frame alias (e.g. ``"s"``).

    Returns:
        Dict mapping column name → ``"s.column"`` expression.
    """
    return {col: f"{source_alias}.{col}" for col in all_columns}


def _without_delete_flag(columns: tuple[str, ...], flag: str | None) -> tuple[str, ...]:
    """Drop the *flag* column from ``columns`` when a flag is named."""
    if flag is None:
        return columns
    return tuple(column for column in columns if column != flag)


def _delete_flag_predicates(flag: str, source_alias: str) -> tuple[str, str]:
    """Return the matched-delete predicate and the predicate that keeps a row.

    A null flag is false.
    """
    coalesced = f"coalesce({source_alias}.{flag}, false)"
    return coalesced, f"NOT {coalesced}"


def _build_merge_plan(
    *,
    combos: list[dict[str, Any]],
    spec: MergeSpec,
    df_columns: tuple[str, ...],
    target_alias: str,
    source_alias: str,
    delete_flag: str | None = None,
) -> _MergePlan:
    """Build predicate and column maps required by Delta MERGE.

    A *delete_flag* adds the matched-delete clause and guards update and insert.
    """
    columns = _without_delete_flag(df_columns, delete_flag)
    predicate = _build_upsert_predicate(combos, spec, target_alias, source_alias)
    update_cols = _build_upsert_update_cols(columns, spec)
    update_set = _build_update_set(update_cols, source_alias)
    insert_values = _build_insert_values(columns, source_alias)
    if delete_flag is None:
        return _MergePlan(
            predicate=predicate,
            update_set=update_set,
            insert_values=insert_values,
        )
    delete_predicate, keep_predicate = _delete_flag_predicates(delete_flag, source_alias)
    return _MergePlan(
        predicate=predicate,
        update_set=update_set,
        insert_values=insert_values,
        matched_delete_predicate=delete_predicate,
        matched_update_predicate=keep_predicate,
        not_matched_insert_predicate=keep_predicate,
    )


def _warn_no_partition_cols(table_ref: str) -> None:
    """Emit a WARNING when no partition columns are declared on a MERGE write.

    Without partition columns the MERGE predicate cannot prune Delta log files,
    forcing a full table scan.  For large tables this can be very expensive.

    Args:
        table_ref: Logical table reference string (for the log message).
    """
    _log.warning(EtlEvent.MERGE_PARTITIONS_UNDECLARED, extra={EtlKey.TABLE: table_ref})


def _log_partition_combos(
    combos: list[dict[str, Any]],
    table_ref: str,
) -> None:
    """Log the number of distinct partition combinations that will be merged.

    Args:
        combos:    List of partition combination dicts collected from the frame.
        table_ref: Logical table reference string.
    """
    _log.debug(
        EtlEvent.MERGE_PARTITIONS_FOUND,
        extra={EtlKey.TABLE: table_ref, EtlKey.PARTITIONS: len(combos)},
    )


def _build_partition_predicate(
    rows: Iterable[Mapping[str, Any]],
    partition_cols: tuple[str, ...],
    *,
    alias: str = "",
) -> str:
    """Build a partition SQL predicate from distinct partition rows.

    Used for both ``replaceWhere`` (no alias) and MERGE ON pre-filters
    (aliased, e.g. ``t.year = 2024``).

    Backend-agnostic: callers extract the distinct rows from their DataFrame
    and pass them as an ``Iterable[Mapping[str, Any]]``.

    Args:
        rows:           Distinct partition-column rows (each a mapping of
                        column name → value).
        partition_cols: Ordered partition column names.
        alias:          Optional table alias prepended to column references
                        (e.g. ``"t"`` → ``t.year = 2024``).  Defaults to no
                        prefix (plain ``year = 2024``).

    Returns:
        SQL predicate string, e.g.
        ``(year = 2024 AND month = 1) OR (year = 2024 AND month = 2)``.
    """
    prefix = f"{alias}." if alias else ""
    clauses = [
        _SQL_AND.join(f"{prefix}{col} = {sql_literal(row[col])}" for col in partition_cols)
        for row in rows
    ]
    return " OR ".join(f"({c})" for c in clauses)
