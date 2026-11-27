"""Tolerant JSON decoding for ``JsonMismatch.NULL`` columns."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
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

from ._dtype import frame_type_to_polars
from ._json_failure import is_json_decode_failure

_EPOCH: Final = date(1970, 1, 1)
_DECIMAL_TEXT: Final = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")


def _integer_or_none(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _number_or_none(value: Any) -> int | float | None:
    is_number = isinstance(value, int | float) and not isinstance(value, bool)
    return value if is_number and math.isfinite(value) else None


def _bool_or_none(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _text_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) else None


def _text_of(value: Any) -> str:
    return value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)


def _epoch_days_or_none(value: Any) -> int | None:
    try:
        return (datetime.fromisoformat(value).date() - _EPOCH).days
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: Any, decimal: DecimalType) -> Any:
    text = str(value)
    if not _DECIMAL_TEXT.fullmatch(text):
        return None
    integer_digits = decimal.precision - decimal.scale
    return value if abs(Decimal(text)) < Decimal(10) ** integer_digits else None


def _decoded_or_none(documents: pl.Series, dtype: pl.DataType) -> pl.Series | None:
    try:
        return documents.str.json_decode(dtype)
    except pl.exceptions.ComputeError as error:
        if is_json_decode_failure(error):
            return None
        raise


_SCALAR_READERS: Final[Mapping[FrameType, Callable[[Any], Any]]] = {
    **dict.fromkeys(
        (
            LoomDtype.INT8,
            LoomDtype.INT16,
            LoomDtype.INT32,
            LoomDtype.INT64,
            LoomDtype.UINT8,
            LoomDtype.UINT16,
            LoomDtype.UINT32,
            LoomDtype.UINT64,
        ),
        _integer_or_none,
    ),
    **dict.fromkeys((LoomDtype.FLOAT32, LoomDtype.FLOAT64), _number_or_none),
    LoomDtype.BOOLEAN: _bool_or_none,
    LoomDtype.UTF8: _text_of,
    LoomDtype.DATE: _epoch_days_or_none,
    LoomDtype.DATETIME: _text_or_none,
}


@dataclass(frozen=True, slots=True)
class TolerantJson:
    """Decodes JSON text into *frame_type* without ever raising on a mismatch.

    The column is first decoded natively by Polars, with no Python per row.
    Only a batch Polars refuses falls back to Python, per row: each field whose
    JSON type is wrong for its declared type is nulled and its siblings kept,
    as Spark ``from_json`` in ``PERMISSIVE`` mode does; any other JSON value
    under a ``str`` field becomes its JSON text, and ``NaN`` and ``Infinity``
    become null. A row Polars still refuses after that, and malformed JSON,
    become a null value for that row, so the normalisation only drives parity
    with Spark and is not what guarantees the decode.

    A batch Polars decodes natively keeps what Polars coerces, such as a float
    under an integer field or a boolean under a number, which Spark nulls.

    A date is read from an ISO string, as Spark does. A number under a
    timestamp field is null here, where Spark reads it as epoch seconds for a
    timezone-aware field.

    Args:
        frame_type: Type the column is decoded into; struct field names are the
            names found in the documents.
    """

    frame_type: FrameType

    def decode(self, column: pl.Expr) -> pl.Expr:
        """Return *column*, JSON text, decoded into :attr:`frame_type`."""
        return column.map_batches(
            self._decode_batch,
            return_dtype=frame_type_to_polars(self.frame_type),
            is_elementwise=True,
        )

    def _decode_batch(self, batch: pl.Series) -> pl.Series:
        dtype = frame_type_to_polars(self.frame_type)
        decoded = _decoded_or_none(batch, dtype)
        if decoded is not None:
            return decoded
        normalised = pl.Series(batch.name, [self._text(d) for d in batch], dtype=pl.String)
        decoded = _decoded_or_none(normalised, dtype)
        if decoded is not None:
            return decoded
        accepted = [
            text if text is None or _decoded_or_none(pl.Series([text]), dtype) is not None else None
            for text in normalised
        ]
        return pl.Series(batch.name, accepted, dtype=pl.String).str.json_decode(dtype)

    def _text(self, document: str | None) -> str | None:
        if document is None:
            return None
        try:
            parsed = json.loads(document, parse_constant=lambda _: None)
        except ValueError:
            return None
        coerced = self._coerce(self.frame_type, parsed)
        return None if coerced is None else json.dumps(coerced, ensure_ascii=False)

    def _coerce(self, frame_type: FrameType, value: Any) -> Any:
        if value is None:
            return None
        match frame_type:
            case StructType(fields=fields):
                if not isinstance(value, dict):
                    return None
                return {f.name: self._coerce(f.dtype, value.get(f.name)) for f in fields}
            case ListType(inner=inner) | ArrayType(inner=inner):
                if not isinstance(value, list):
                    return None
                return [self._coerce(inner, item) for item in value]
            case DecimalType():
                return _decimal_or_none(value, frame_type)
            case DatetimeType():
                return _text_or_none(value)
            case _:
                reader = _SCALAR_READERS.get(frame_type)
                return value if reader is None else reader(value)
