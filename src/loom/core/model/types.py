from __future__ import annotations

from typing import Annotated

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


def DateTime(*, tz: bool = True) -> ColumnType:
    return ColumnType("DateTime", kwargs={"timezone": tz})


def Numeric(*, precision: int = 10, scale: int = 2) -> ColumnType:
    return ColumnType("Numeric", args=(precision, scale))


class _JsonMarker:
    """Sentinel that marks a str field as containing serialized JSON."""

    __slots__ = ()


JsonStr = Annotated[str, _JsonMarker()]
