"""Errors raised by backend writers for table targets.

Internal module — import from :mod:`loom.etl.declarative.target` or :mod:`loom.etl`.
"""

from __future__ import annotations


class EmptyReplaceError(ValueError):
    """Raised when a ``replace(allow_empty=False)`` target receives no rows.

    The table is left as it was: the check runs before anything is written
    or created.

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


class UpsertDeleteFlagError(ValueError):
    """Raised when the ``upsert(delete_flag=...)`` column cannot be used.

    Raised before anything is written or created.

    Args:
        column:  Declared ``delete_flag`` column name.
        problem: What is wrong with the column.

    Attributes:
        column:  Declared ``delete_flag`` column name.
        problem: What is wrong with the column.
    """

    def __init__(self, column: str, problem: str) -> None:
        self.column = column
        self.problem = problem
        super().__init__(f"upsert() delete_flag={column!r} {problem}.")


class UnsupportedUpsertDeleteFlagError(ValueError):
    """Raised when a writer that cannot apply ``upsert(delete_flag=...)`` receives one.

    Args:
        writer: Name of the writer class.
        column: Declared ``delete_flag`` column name.

    Attributes:
        writer: Name of the writer class.
        column: Declared ``delete_flag`` column name.
    """

    def __init__(self, writer: str, column: str) -> None:
        self.writer = writer
        self.column = column
        super().__init__(f"{writer} does not support upsert(delete_flag={column!r}).")


__all__ = ["EmptyReplaceError", "UnsupportedUpsertDeleteFlagError", "UpsertDeleteFlagError"]
