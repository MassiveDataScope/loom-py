from __future__ import annotations

import sys
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any, Final

import msgspec

from loom.core.errors import InvalidValue


class ColumnKind(StrEnum):
    """Every kind of column loom can describe; the registry the translators are checked against."""

    STRING = "String"
    TEXT = "Text"
    INTEGER = "Integer"
    BIG_INTEGER = "BigInteger"
    TINY_INTEGER = "TinyInteger"
    SMALL_INTEGER = "SmallInteger"
    FLOAT = "Float"
    BOOLEAN = "Boolean"
    NUMERIC = "Numeric"
    JSON = "JSON"
    BYTES = "Bytes"
    DATE = "Date"
    TIME = "Time"
    DATETIME = "DateTime"
    UUID = "Uuid"
    POSTGRES_JSONB = "Postgres.JSONB"
    POSTGRES_UUID = "Postgres.UUID"
    POSTGRES_TSVECTOR = "Postgres.TSVECTOR"
    POSTGRES_INET = "Postgres.INET"
    POSTGRES_ARRAY = "Postgres.ARRAY"

    @property
    def is_json(self) -> bool:
        """Whether a column of this kind holds a JSON document or a list of items."""
        return self in _JSON_KINDS


_JSON_KINDS: Final = frozenset(
    {ColumnKind.JSON, ColumnKind.POSTGRES_JSONB, ColumnKind.POSTGRES_ARRAY}
)


@dataclass(frozen=True, slots=True)
class ColumnType:
    """Backend column type descriptor stored inside ``Annotated[...]``.

    It is hashable, as ``Annotated`` metadata must be: the keyword options are
    held as ``(name, value)`` pairs.
    """

    type_name: ColumnKind
    args: tuple[Any, ...] = ()
    kwargs: tuple[tuple[str, Any], ...] = ()

    def option(self, name: str, default: Any = None) -> Any:
        """Return the keyword option *name*, or *default* when it is not set."""
        return dict(self.kwargs).get(name, default)

    def portable(self) -> ColumnType | None:
        """The portable column this type is stored as outside its dialect.

        A portable type is its own counterpart; a Postgres type answers the
        portable one it stands for, and ``None`` when it has none, as a
        tsvector or an array, whose item type is part of the column.
        """
        return _PORTABLE_COUNTERPART.get(self.type_name, self)


def String(length: int | None = None) -> ColumnType:
    if length is None:
        return ColumnType(ColumnKind.STRING)
    return ColumnType(ColumnKind.STRING, args=(length,))


TinyInteger = ColumnType(ColumnKind.TINY_INTEGER)
SmallInteger = ColumnType(ColumnKind.SMALL_INTEGER)
Integer = ColumnType(ColumnKind.INTEGER)
BigInteger = ColumnType(ColumnKind.BIG_INTEGER)
Float = ColumnType(ColumnKind.FLOAT)
Boolean = ColumnType(ColumnKind.BOOLEAN)
Text = ColumnType(ColumnKind.TEXT)
JSON = ColumnType(ColumnKind.JSON)
Bytes = ColumnType(ColumnKind.BYTES)
Date = ColumnType(ColumnKind.DATE)
Time = ColumnType(ColumnKind.TIME)
Uuid = ColumnType(ColumnKind.UUID)


def DateTime(*, tz: bool = True) -> ColumnType:
    return ColumnType(ColumnKind.DATETIME, kwargs=(("timezone", tz),))


def Numeric(*, precision: int | None = None, scale: int | None = None) -> ColumnType:
    """A ``NUMERIC`` column; with no precision it is unbounded.

    Raises:
        InvalidValue: A scale is given without a precision, the precision is
            below 1, or the scale is outside ``0..precision``.
    """
    if scale is not None and precision is None:
        raise InvalidValue("Numeric: a scale needs a precision")
    if precision is not None and precision < 1:
        raise InvalidValue(f"Numeric: precision must be at least 1, got {precision}")
    if scale is not None and precision is not None and not 0 <= scale <= precision:
        raise InvalidValue(f"Numeric: scale must be between 0 and the precision, got {scale}")
    return ColumnType(ColumnKind.NUMERIC, kwargs=(("precision", precision), ("scale", scale)))


class JsonMarker:
    """Sentinel that marks a str field as containing serialized JSON."""

    __slots__ = ()


JsonStr = Annotated[str, JsonMarker()]

PositiveInt = Annotated[int, msgspec.Meta(gt=0)]
"""An integer above zero: a size, a count or a timeout where zero makes no sense."""

PositiveSeconds = Annotated[float, msgspec.Meta(gt=0, le=sys.float_info.max)]
"""A finite number of seconds above zero: a window or a timeout; refuses ``inf`` and ``nan``."""

NonNegativeInt = Annotated[int, msgspec.Meta(ge=0)]
"""An integer at or above zero: an extra allowance where none at all is valid."""


class _PostgresTypes:
    """Namespace for PostgreSQL-specific column types."""

    JSONB = ColumnType(ColumnKind.POSTGRES_JSONB)
    UUID = ColumnType(ColumnKind.POSTGRES_UUID)
    """Postgres only; :data:`loom.core.model.Uuid` is the portable UUID column."""
    TSVECTOR = ColumnType(ColumnKind.POSTGRES_TSVECTOR)
    INET = ColumnType(ColumnKind.POSTGRES_INET)

    @staticmethod
    def ARRAY(item: ColumnType) -> ColumnType:
        return ColumnType(ColumnKind.POSTGRES_ARRAY, args=(item,))


Postgres = _PostgresTypes()

_PORTABLE_COUNTERPART: Final[Mapping[ColumnKind, ColumnType | None]] = {
    ColumnKind.POSTGRES_JSONB: JSON,
    ColumnKind.POSTGRES_UUID: Uuid,
    ColumnKind.POSTGRES_INET: String(),
    ColumnKind.POSTGRES_ARRAY: None,
    ColumnKind.POSTGRES_TSVECTOR: None,
}
