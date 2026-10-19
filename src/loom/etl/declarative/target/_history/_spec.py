"""Compiled historify spec types."""

from __future__ import annotations

from dataclasses import dataclass

from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target._history._enums import (
    DeletePolicy,
    HistorifyInputMode,
    HistoryDateType,
)
from loom.etl.declarative.target._schema_mode import SchemaMode


@dataclass(frozen=True)
class HistorifySpec:
    """Compiled SCD Type 2 write spec. Produced by :meth:`IntoHistory._to_spec`.

    This frozen dataclass is the sole input to the backend engine. All
    configuration is resolved at declaration time — no runtime reflection occurs.

    Both ``table_ref`` and ``schema_mode`` are required for duck-type detection
    by the write-policy dispatcher (``_is_table_target_spec``).

    Args:
        table_ref: Logical Delta table reference.
        keys: Columns that uniquely identify the entity
            (e.g. ``(\"player_id\",)``).
        effective_date: Column name in LOG mode, or a
            :class:`~loom.etl.ParamExpr` in SNAPSHOT mode.
        mode: Input semantics — ``SNAPSHOT`` or ``LOG``.
        track: Columns whose value change triggers a new history row.
            ``None`` means every non-key column is tracked.
        overwrite: Columns refreshed to their latest value without opening a new
            history row. In SNAPSHOT mode they update the current open row when
            the entity is UNCHANGED. In LOG mode consecutive same-``track`` events
            are collapsed into one run: ``valid_from`` anchors to the first event,
            these columns take the last observed value, and every other payload
            column freezes at the transition-time (first) value.
            Must not overlap with ``keys`` or ``track``.
        delete_policy: Action for absent keys in SNAPSHOT mode.
            Ignored in LOG mode.
        partition_scope: Partition columns used to limit Delta reads/writes.
            Strongly recommended for large tables.
        valid_from: Name of the period-start boundary column.
        valid_to: Name of the period-end boundary column.
            ``NULL`` in the table means the vector is currently open. In LOG
            mode, same-instant events for one entity collapse the earlier
            vector to zero width (``valid_from == valid_to``).
        deleted_at: Name of the soft-delete audit column. Only written when
            ``delete_policy=SOFT_DELETE``.
        date_type: Precision of the ``valid_from`` / ``valid_to`` columns.
        schema_mode: Schema evolution strategy.
        allow_temporal_rerun: Allow re-weave when past-date data is loaded.
        delete_marker: LOG mode only. Name of a boolean frame column flagging
            tombstone events: a ``True`` row closes the entity's open vector one
            period before its effective date and opens none. Every gap in stored
            history is read as a deletion. Never written to the table. ``None``
            disables tombstones.
    """

    table_ref: TableRef
    keys: tuple[str, ...]
    effective_date: str | ParamExpr
    mode: HistorifyInputMode
    track: tuple[str, ...] | None = None
    overwrite: tuple[str, ...] | None = None
    delete_policy: DeletePolicy = DeletePolicy.CLOSE
    partition_scope: tuple[str, ...] | None = None
    valid_from: str = "valid_from"
    valid_to: str = "valid_to"
    deleted_at: str = "deleted_at"
    date_type: HistoryDateType = HistoryDateType.DATE
    schema_mode: SchemaMode = SchemaMode.STRICT
    allow_temporal_rerun: bool = False
    delete_marker: str | None = None


__all__ = ["HistorifySpec"]
