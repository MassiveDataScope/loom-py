"""Historify domain enums for SCD Type 2 targets."""

from __future__ import annotations

from enum import StrEnum

from loom.etl.schema import DatetimeType, FrameType, LoomDtype


class HistorifyInputMode(StrEnum):
    """Input semantics for the SCD2 writer.

    Values:

    * ``SNAPSHOT`` — the frame is a **full snapshot** of the entity dimension.
      Keys absent from the snapshot are handled according to ``delete_policy``.
    * ``LOG`` — the frame carries individual **change events**; each row has an
      ``effective_date`` column that determines when the change took effect.
    """

    SNAPSHOT = "snapshot"
    LOG = "log"


class DeletePolicy(StrEnum):
    """Action applied to entity keys absent from an incoming snapshot.

    Only meaningful when ``mode=SNAPSHOT``; LOG mode ignores this setting.

    Values:

    * ``IGNORE``      — leave open vectors open. Use for partial or incremental
      snapshots where absence does not imply deletion.
    * ``CLOSE``       — close the open vector by setting
      ``valid_to = effective_date - 1``. Standard SCD2 behavior for full-dimension
      snapshots.
    * ``SOFT_DELETE`` — close the open vector and stamp a ``deleted_at`` column.
      Use when downstream systems require explicit deletion audit trails.
    """

    IGNORE = "ignore"
    CLOSE = "close"
    SOFT_DELETE = "soft_delete"


class HistoryDateType(StrEnum):
    """Column type used for ``valid_from`` / ``valid_to`` boundary columns.

    Values:

    * ``DATE`` — calendar date precision. Suitable for daily SCD2 pipelines.
    * ``TIMESTAMP`` — microsecond precision. Use for event-driven or sub-daily
      update patterns.
    """

    DATE = "date"
    TIMESTAMP = "timestamp"

    @classmethod
    def of_column(cls, frame_type: FrameType | None) -> HistoryDateType | None:
        """The boundary type a column of *frame_type* holds, ``None`` when it is not temporal."""
        if frame_type is LoomDtype.DATE:
            return HistoryDateType.DATE
        if isinstance(frame_type, DatetimeType):
            return HistoryDateType.TIMESTAMP
        return None


__all__ = ["HistorifyInputMode", "DeletePolicy", "HistoryDateType"]
