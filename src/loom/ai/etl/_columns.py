"""Polars columns of a declared agent output, typed by the output and not by the rows."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Final

import msgspec
import msgspec.inspect as mi
import polars as pl

from loom.core.model import loom_type

DECIMAL_DTYPE: Final = pl.Decimal(38, 9)


@dataclass(frozen=True)
class Column:
    """One answer column: its Polars type and how a JSON-mode value becomes a cell.

    Attributes:
        dtype: Polars type of the column, fixed by the declared output.
        convert: Turns one non-null JSON-mode value into the cell value.
    """

    dtype: pl.DataType
    convert: Callable[[Any], Any]

    def cell(self, value: Any) -> Any:
        """Return the cell holding *value*; ``None`` stays null."""
        return None if value is None else self.convert(value)


def output_columns(output_type: type[Any]) -> dict[str, Column]:
    """Return one column per field of *output_type*, keyed by the field's wire name.

    ``str`` and string choices (``Literal``, ``Enum``) are ``String``; ``int``
    and integer choices ``Int64``; ``float`` ``Float64``; ``bool``
    ``Boolean``; ``date`` ``Date``; ``datetime`` a UTC ``Datetime`` (naive when
    the field requires naive values, a naive value otherwise read as UTC);
    ``Decimal`` ``Decimal(38, 9)``; lists and sets ``List``; nested structs
    ``Struct``; an optional field the type of its value. Any other shape is
    ``String`` holding the JSON of the value.

    Args:
        output_type: A strict ``msgspec.Struct`` or ``pydantic.BaseModel``.

    Returns:
        The columns, in field order.
    """
    return {name: _column(mi.type_info(annotation)) for name, annotation in _fields(output_type)}


def _fields(output_type: type[Any]) -> Iterable[tuple[str, Any]]:
    if loom_type(output_type).library == "msgspec":
        return [(f.encode_name, f.type) for f in msgspec.structs.fields(output_type)]
    return [
        (info.serialization_alias or info.alias or name, info.annotation)
        for name, info in output_type.model_fields.items()
    ]


def _column(info: mi.Type) -> Column:
    if isinstance(info, mi.Metadata):
        return _column(info.type)
    if isinstance(info, mi.UnionType):
        return _optional(info)
    if isinstance(info, mi.LiteralType):
        return _choice(info.values)
    if isinstance(info, mi.EnumType):
        return _choice(tuple(member.value for member in info.cls))
    if isinstance(info, mi.CollectionType):
        return _list(_column(info.item_type))
    if isinstance(info, mi.StructType | mi.DataclassType | mi.TypedDictType):
        return _struct({f.encode_name: _column(f.type) for f in info.fields})
    if isinstance(info, mi.DateTimeType):
        return Column(pl.Datetime("us", None if info.tz is False else "UTC"), _datetime)
    return _SCALARS.get(type(info), _JSON)


def _optional(info: mi.UnionType) -> Column:
    values = [member for member in info.types if not isinstance(member, mi.NoneType)]
    return _column(values[0]) if len(values) == 1 else _JSON


def _choice(values: tuple[Any, ...]) -> Column:
    if all(isinstance(value, bool) for value in values):
        return _SCALARS[mi.BoolType]
    if all(isinstance(value, int) and not isinstance(value, bool) for value in values):
        return _SCALARS[mi.IntType]
    if all(isinstance(value, str) for value in values):
        return _SCALARS[mi.StrType]
    return _JSON


def _list(item: Column) -> Column:
    return Column(pl.List(item.dtype), lambda values: [item.cell(value) for value in values])


def _struct(fields: Mapping[str, Column]) -> Column:
    def convert(value: Mapping[str, Any]) -> dict[str, Any]:
        return {name: column.cell(value.get(name)) for name, column in fields.items()}

    return Column(pl.Struct({name: column.dtype for name, column in fields.items()}), convert)


def _same(value: Any) -> Any:
    return value


def _datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is None else parsed.astimezone(UTC)


def _decimal(value: str | float | int) -> Decimal:
    return Decimal(str(value))


def _json(value: Any) -> str:
    return value if isinstance(value, str) else msgspec.json.encode(value).decode()


_JSON: Final = Column(pl.String(), _json)

_SCALARS: Final[Mapping[type[mi.Type], Column]] = MappingProxyType(
    {
        mi.StrType: Column(pl.String(), _same),
        mi.IntType: Column(pl.Int64(), _same),
        mi.FloatType: Column(pl.Float64(), float),
        mi.BoolType: Column(pl.Boolean(), _same),
        mi.DateType: Column(pl.Date(), date.fromisoformat),
        mi.DecimalType: Column(DECIMAL_DTYPE, _decimal),
    }
)
