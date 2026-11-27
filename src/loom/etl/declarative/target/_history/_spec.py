"""Compiled historify spec types."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from loom.core.schema import SchemaMode
from loom.etl.declarative._declared_table import ModelBoundSpec
from loom.etl.declarative._history_date_type import HistoryDateType
from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.target._history._enums import DeletePolicy, HistorifyInputMode


@dataclass(frozen=True)
class HistorifySpec(ModelBoundSpec):
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
        date_type: Declared precision of the ``valid_from`` / ``valid_to`` columns;
            ``None`` follows the model's ``valid_from`` column, or ``DATE`` for a reference.
        schema_mode: Schema evolution strategy.
        allow_temporal_rerun: Allow re-weave when past-date data is loaded.
        delete_marker: LOG mode only. Name of a boolean frame column flagging
            tombstone events: a ``True`` row closes the entity's open vector one
            period before its effective date and opens none. Every gap in stored
            history is read as a deletion. Never written to the table. ``None``
            disables tombstones.
    """

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
    date_type: HistoryDateType | None = None
    schema_mode: SchemaMode = SchemaMode.STRICT
    allow_temporal_rerun: bool = False
    delete_marker: str | None = None

    @property
    def model_boundary_type(self) -> HistoryDateType | None:
        """The date type of the model's ``valid_from``, ``None`` without a model or a date."""
        table = self.model_table
        if table is None:
            return None
        return HistoryDateType.of_frame_type(table.column_types.get(self.valid_from))

    @property
    def resolved_date_type(self) -> HistoryDateType:
        """The precision of the boundary columns: declared, else the model's, else ``DATE``."""
        return self.date_type or self.model_boundary_type or HistoryDateType.DATE

    @property
    def declared_columns(self) -> Mapping[str, tuple[str, ...]]:
        """The model columns this spec names, by the argument that names them."""
        boundaries = {"valid_from": (self.valid_from,), "valid_to": (self.valid_to,)}
        if self.delete_policy is DeletePolicy.SOFT_DELETE:
            boundaries["deleted_at"] = (self.deleted_at,)
        return {
            "keys": self.keys,
            "track": self.track or (),
            "overwrite": self.overwrite or (),
            "partition_scope": self.partition_scope or (),
            **boundaries,
        }


__all__ = ["HistorifySpec"]
