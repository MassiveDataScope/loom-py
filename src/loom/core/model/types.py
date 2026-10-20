from __future__ import annotations

from typing import Annotated

import msgspec

from loom.core.model.field import ColumnType


def String(length: int | None = None) -> ColumnType:
    if length is None:
        return ColumnType("String")
    return ColumnType("String", args=(length,))


Integer = ColumnType("Integer")
BigInteger = ColumnType("BigInteger")
Float = ColumnType("Float")
Boolean = ColumnType("Boolean")
Text = ColumnType("Text")
JSON = ColumnType("JSON")
Bytes = ColumnType("Bytes")
Date = ColumnType("Date")
Time = ColumnType("Time")
Uuid = ColumnType("Uuid")


def DateTime(*, tz: bool = True) -> ColumnType:
    return ColumnType("DateTime", kwargs={"timezone": tz})


def Numeric(*, precision: int | None = None, scale: int | None = None) -> ColumnType:
    """A ``NUMERIC`` column; with no precision it is unbounded.

    Raises:
        ValueError: A scale is given without a precision; SQLAlchemy would
            ignore it.
    """
    if scale is not None and precision is None:
        raise ValueError("Numeric: a scale needs a precision")
    return ColumnType("Numeric", kwargs={"precision": precision, "scale": scale})


class _JsonMarker:
    """Sentinel that marks a str field as containing serialized JSON."""

    __slots__ = ()


JsonStr = Annotated[str, _JsonMarker()]

PositiveInt = Annotated[int, msgspec.Meta(gt=0)]
"""An integer above zero: a size, a count or a timeout where zero makes no sense."""
