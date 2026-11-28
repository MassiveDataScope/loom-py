"""Decoding of JSON text columns, reading dates and timestamps from ISO text."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Self

import polars as pl
from polars.datatypes import DataTypeClass

_Dtype = pl.DataType | DataTypeClass

_DATE_FORMAT: Final = "%Y-%m-%d"
_INSTANT_FORMAT: Final = "%Y-%m-%dT%H:%M:%S%.f%#z"
_DATE_ONLY: Final = r"^(\d{4}-\d{2}-\d{2})$"
_SPACE_SEPARATED: Final = r"^(\d{4}-\d{2}-\d{2}) "
_WITHOUT_OFFSET: Final = r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?)$"


def _text_dtype(dtype: _Dtype) -> _Dtype:
    match dtype:
        case pl.Date() | pl.Datetime():
            return pl.String()
        case pl.Struct():
            return pl.Struct([pl.Field(f.name, _text_dtype(f.dtype)) for f in dtype.fields])
        case pl.List():
            return pl.List(_text_dtype(dtype.inner))
        case _:
            return dtype


@dataclass(frozen=True, slots=True)
class JsonTextDecoder:
    """Decodes JSON text into a Polars dtype natively, with no Python per row.

    Polars reads a timestamp only in one fixed shape, and silently nulls any
    other. Here every date and timestamp leaf is decoded as text and then
    converted: a date from ``YYYY-MM-DD``, a timestamp from ISO text with an
    optional offset, a ``T`` or a space separator, and a date alone. Text with
    no offset is UTC, and an offset is converted to the instant it denotes.
    A number under a date or timestamp field is not a valid text for it.

    Args:
        strict: Whether invalid temporal text raises instead of becoming null.
    """

    strict: bool

    @classmethod
    def refusing(cls) -> Self:
        """A decoder where invalid temporal text raises :exc:`polars.exceptions.ComputeError`."""
        return cls(strict=True)

    @classmethod
    def nulling(cls) -> Self:
        """A decoder where invalid temporal text becomes null."""
        return cls(strict=False)

    def decode(self, column: pl.Expr, dtype: _Dtype) -> pl.Expr:
        """Return *column*, JSON text, decoded into *dtype*."""
        return self._converted(column.str.json_decode(_text_dtype(dtype)), dtype)

    def _converted(self, expr: pl.Expr, dtype: _Dtype) -> pl.Expr:
        match dtype:
            case pl.Date():
                return expr.str.to_date(format=_DATE_FORMAT, strict=self.strict)
            case pl.Datetime():
                return self._instant(expr, dtype)
            case pl.Struct():
                return self._struct(expr, dtype)
            case pl.List():
                if _text_dtype(dtype.inner) == dtype.inner:
                    return expr
                return expr.list.eval(self._converted(pl.element(), dtype.inner))
            case _:
                return expr

    def _struct(self, expr: pl.Expr, dtype: pl.Struct) -> pl.Expr:
        converted = [
            self._converted(expr.struct.field(f.name), f.dtype).alias(f.name)
            for f in dtype.fields
            if _text_dtype(f.dtype) != f.dtype
        ]
        return expr.struct.with_fields(*converted) if converted else expr

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
