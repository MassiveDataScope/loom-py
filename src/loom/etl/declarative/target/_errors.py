"""Errors raised by backend writers for table targets.

Internal module — import from :mod:`loom.etl.declarative.target` or :mod:`loom.etl`.
"""

from __future__ import annotations


class EmptyReplaceError(ValueError):
    """Raised when a ``replace(allow_empty=False)`` target receives no rows.

    The table is left as it was: the check runs before anything is written
    or created, on the rows the write would use. The Polars backend counts
    the collected frame; the Spark backend computes the frame once with
    ``localCheckpoint`` and both counts and writes that result.

    Args:
        table_ref: Logical reference of the target table.

    Attributes:
        table_ref: Logical reference of the target table.
    """

    def __init__(self, table_ref: str) -> None:
        self.table_ref = table_ref
        super().__init__(
            f"Refusing to replace {table_ref!r} with an empty frame: the target declares "
            "replace(allow_empty=False). The table was not modified."
        )


__all__ = ["EmptyReplaceError"]
