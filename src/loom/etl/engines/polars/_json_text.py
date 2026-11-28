"""Native decoding of JSON text into Polars dtypes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Self

import polars as pl
from polars.datatypes import DataTypeClass

_Dtype = pl.DataType | DataTypeClass

_DATE_FORMAT: Final = "%Y-%m-%d"
_INSTANT_FORMAT: Final = "%Y-%m-%dT%H:%M:%S%.f%#z"
_DATE_WITH_TIME: Final = r"^(\d{4}-\d{2}-\d{2})[T ].*$"
_DATE_ONLY: Final = r"^(\d{4}-\d{2}-\d{2})$"
_SPACE_SEPARATED: Final = r"^(\d{4}-\d{2}-\d{2}) "
_WITHOUT_OFFSET: Final = r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?)$"


def _instance_of(dtype: _Dtype) -> pl.DataType:
    """Return *dtype* as an instance, whether it arrives as a class or an instance."""
    return dtype() if isinstance(dtype, DataTypeClass) else dtype


def _text_dtype(dtype: _Dtype) -> pl.DataType:
    """Return *dtype* with every date, timestamp and fixed-width array leaf read as text."""
    resolved = _instance_of(dtype)
    match resolved:
        case pl.Date() | pl.Datetime():
            return pl.String()
        case pl.Struct():
            return pl.Struct([pl.Field(f.name, _text_dtype(f.dtype)) for f in resolved.fields])
        case pl.List() | pl.Array():
            return pl.List(_text_dtype(resolved.inner))
        case _:
            return resolved


@dataclass(frozen=True, slots=True)
class JsonTextDecoder:
    """Decodes JSON text into a Polars dtype natively, with no Python per row.

    A date is read from ``YYYY-MM-DD`` text, optionally followed by a ``T`` or a
    space and a time. A timestamp is read from ISO text with an optional offset,
    a ``T`` or space separator, or a date alone; text with no offset is UTC and an
    offset is converted to the instant it denotes. A number is not a date or a
    timestamp. A fixed-width array of a wrong width is invalid.

    Args:
        strict: Whether invalid text raises instead of becoming null.
    """

    strict: bool

    @classmethod
    def refusing(cls) -> Self:
        """A decoder where invalid text raises."""
        return cls(strict=True)

    @classmethod
    def nulling(cls) -> Self:
        """A decoder where invalid text becomes null."""
        return cls(strict=False)

    def decode(self, column: pl.Expr, dtype: _Dtype) -> pl.Expr:
        """Return *column*, JSON text, decoded into *dtype*."""
        return self._converted(column.str.json_decode(_text_dtype(dtype)), dtype)

    def _converted(self, expr: pl.Expr, dtype: _Dtype) -> pl.Expr:
        resolved = _instance_of(dtype)
        if _text_dtype(resolved) == resolved:
            return expr
        match resolved:
            case pl.Date():
                return self._date(expr)
            case pl.Datetime():
                return self._instant(expr, resolved)
            case pl.Struct():
                return self._struct(expr, resolved)
            case pl.Array():
                return self._elements(expr, resolved.inner).list.to_array(resolved.size)
            case pl.List():
                return self._elements(expr, resolved.inner)
            case _:
                return expr

    def _elements(self, expr: pl.Expr, inner: _Dtype) -> pl.Expr:
        return expr.list.eval(self._converted(pl.element(), inner))

    def _struct(self, expr: pl.Expr, dtype: pl.Struct) -> pl.Expr:
        fields = [
            self._converted(expr.struct.field(f.name), f.dtype).alias(f.name) for f in dtype.fields
        ]
        return expr.struct.with_fields(*fields)

    def _date(self, expr: pl.Expr) -> pl.Expr:
        return expr.str.replace(_DATE_WITH_TIME, "${1}").str.to_date(
            format=_DATE_FORMAT, strict=self.strict
        )

    def _instant(self, expr: pl.Expr, dtype: pl.Datetime) -> pl.Expr:
        text = (
            expr.str.replace(_DATE_ONLY, "${1}T00:00:00")
            .str.replace(_SPACE_SEPARATED, "${1}T")
            .str.replace(_WITHOUT_OFFSET, "${1}Z")
        )
        parsed = text.str.to_datetime(
            format=_INSTANT_FORMAT,
            time_unit=dtype.time_unit,
            time_zone=dtype.time_zone or "UTC",
            strict=self.strict,
        )
        return parsed if dtype.time_zone else parsed.dt.replace_time_zone(None)
