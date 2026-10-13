"""Errors raised by backend writers for table targets.

Internal module — import from :mod:`loom.etl.declarative.target` or :mod:`loom.etl`.
"""

from __future__ import annotations


class EmptyReplaceError(ValueError):
    """Raised when a ``replace(allow_empty=False)`` target receives no rows.

    The table is left as it was: the check runs after the frame is
    materialised and before anything is written or created.

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
