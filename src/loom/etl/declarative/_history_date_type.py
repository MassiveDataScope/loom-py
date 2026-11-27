"""The column type of the boundary columns of a history target."""

from __future__ import annotations

from enum import StrEnum

from loom.core.model.types import ColumnKind


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
    def of_kind(cls, kind: ColumnKind | None) -> HistoryDateType | None:
        """The boundary type a column of *kind* holds, ``None`` when it is not temporal."""
        match kind:
            case ColumnKind.DATE:
                return cls.DATE
            case ColumnKind.DATETIME:
                return cls.TIMESTAMP
            case _:
                return None
