"""Tolerant JSON normalisation for ``JsonMismatch.NULL`` columns."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Final

import polars as pl

from loom.etl.schema._schema import (
    ArrayType,
    DatetimeType,
    DecimalType,
    FrameType,
    ListType,
    LoomDtype,
    StructType,
)

_INTEGERS: Final = frozenset(
    {
        LoomDtype.INT8,
        LoomDtype.INT16,
        LoomDtype.INT32,
        LoomDtype.INT64,
        LoomDtype.UINT8,
        LoomDtype.UINT16,
        LoomDtype.UINT32,
        LoomDtype.UINT64,
    }
)
_FLOATS: Final = frozenset({LoomDtype.FLOAT32, LoomDtype.FLOAT64})
_EPOCH: Final = date(1970, 1, 1)


def _is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value: Any) -> bool:
    return _is_integer(value) or isinstance(value, float)


def _text_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _epoch_days_or_none(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return (datetime.fromisoformat(value).date() - _EPOCH).days
    except ValueError:
        return None


def _decimal_or_none(value: Any, decimal: DecimalType) -> Any:
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        return None
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        return None
    fits = parsed.is_finite() and abs(parsed) < Decimal(10) ** (decimal.precision - decimal.scale)
    return value if fits else None


@dataclass(frozen=True, slots=True)
class TolerantJson:
    """Rewrites JSON text so ``str.json_decode`` of *frame_type* always succeeds.

    Malformed JSON, a struct or list mismatch and a value that does not fit its
    declared primitive type become null, as Spark ``from_json`` in ``PERMISSIVE``
    mode does for that field alone; any other JSON value under a ``str`` field
    becomes its JSON text. Python runs once per row of the column it is applied to.

    A date is read from an ISO string, as Spark does. A number under a timestamp
    field is null here, where Spark reads it as epoch seconds.

    Args:
        frame_type: Type the column is decoded into; struct field names are the
            names found in the documents.
    """

    frame_type: FrameType

    def normalise(self, column: pl.Expr) -> pl.Expr:
        """Return *column* as JSON text that decodes into :attr:`frame_type`."""
        return column.map_elements(self._text, return_dtype=pl.String)

    def _text(self, document: str | None) -> str | None:
        if document is None:
            return None
        try:
            parsed = json.loads(document)
        except ValueError:
            return None
        coerced = self._coerce(self.frame_type, parsed)
        if coerced is None:
            return None
        if coerced is parsed:
            return document
        return json.dumps(coerced, ensure_ascii=False)

    def _coerce(self, frame_type: FrameType, value: Any) -> Any:
        match frame_type:
            case StructType(fields=fields):
                if not isinstance(value, dict):
                    return None
                return {f.name: self._coerce(f.dtype, value.get(f.name)) for f in fields}
            case ListType(inner=inner) | ArrayType(inner=inner):
                if not isinstance(value, list):
                    return None
                return [self._coerce(inner, item) for item in value]
            case _ if value is None:
                return None
            case LoomDtype.UTF8:
                return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
            case dtype if dtype in _INTEGERS:
                return value if _is_integer(value) else None
            case dtype if dtype in _FLOATS:
                return value if _is_number(value) else None
            case LoomDtype.BOOLEAN:
                return value if isinstance(value, bool) else None
            case LoomDtype.DATE:
                return _epoch_days_or_none(value)
            case LoomDtype.DATETIME | DatetimeType():
                return _text_or_none(value)
            case DecimalType():
                return _decimal_or_none(value, frame_type)
            case _:
                return value
