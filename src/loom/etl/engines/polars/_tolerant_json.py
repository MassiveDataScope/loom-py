"""Tolerant JSON decoding for ``JsonMismatch.NULL`` columns."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

import msgspec
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
from ._json_text import JsonTextDecoder

_CHUNK_ROWS: Final = 8_192
_DECIMAL_TEXT: Final = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")
_DECODER: Final = msgspec.json.Decoder()
_ENCODER: Final = msgspec.json.Encoder()


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
    return value if isinstance(value, str) else _ENCODER.encode(value).decode()


def _decimal_or_none(value: Any, decimal: DecimalType) -> Any:
    text = str(value)
    if not _DECIMAL_TEXT.fullmatch(text):
        return None
    integer_digits = decimal.precision - decimal.scale
    return value if abs(Decimal(text)) < Decimal(10) ** integer_digits else None


def _parsed(document: str) -> Any:
    try:
        return _DECODER.decode(document)
    except msgspec.DecodeError:
        return json.loads(document, parse_constant=lambda _: None)


def _decoded(documents: pl.Series, dtype: pl.DataType) -> pl.Series:
    column = JsonTextDecoder.nulling().decode(pl.first(), dtype)
    return documents.to_frame().select(column).to_series()


def _decoded_or_none(documents: pl.Series, dtype: pl.DataType) -> pl.Series | None:
    try:
        return _decoded(documents, dtype)
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
    LoomDtype.DATE: _text_or_none,
    LoomDtype.DATETIME: _text_or_none,
}


@dataclass(frozen=True, slots=True)
class TolerantJson:
    """Decodes JSON text into *frame_type* without ever raising on a mismatch.

    Every row is normalised in Python, so the result of a row never depends on
    the rows around it: each field whose JSON type is wrong for its declared
    type is nulled and its siblings kept, as Spark ``from_json`` in
    ``PERMISSIVE`` mode does; any other JSON value under a ``str`` field becomes
    its JSON text, and ``NaN`` and ``Infinity`` become null. The normalised
    text is then decoded by Polars, in chunks of :data:`_CHUNK_ROWS` rows so the
    memory held does not depend on the batch size. Should Polars still refuse a
    row, only that row becomes null. Any failure normalising a row, such as
    malformed JSON, nesting too deep to read or text Polars cannot hold, makes
    that row null.

    Dates and timestamps are read from ISO text as :class:`JsonTextDecoder`
    does, and invalid text is null. A number under a timestamp field is null
    here, where Spark reads it as epoch seconds for a timezone-aware field.

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
        offsets = range(0, max(len(batch), 1), _CHUNK_ROWS)
        return pl.concat(
            [self._decode_chunk(batch.slice(offset, _CHUNK_ROWS), dtype) for offset in offsets]
        )

    def _decode_chunk(self, chunk: pl.Series, dtype: pl.DataType) -> pl.Series:
        encoded = pl.Series(chunk.name, [self._encoded(d) for d in chunk], dtype=pl.Binary)
        normalised = encoded.cast(pl.String)
        decoded = _decoded_or_none(normalised, dtype)
        if decoded is not None:
            return decoded
        accepted = [
            text if text is None or _decoded_or_none(pl.Series([text]), dtype) is not None else None
            for text in normalised
        ]
        return _decoded(pl.Series(chunk.name, accepted, dtype=pl.String), dtype)

    def _encoded(self, document: str | None) -> bytes | None:
        if document is None:
            return None
        try:
            return _ENCODER.encode(self._coerce(self.frame_type, _parsed(document)))
        except Exception:  # noqa: BLE001
            return None

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
