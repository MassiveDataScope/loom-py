"""Tolerant JSON decoding for ``JsonMismatch.NULL`` columns."""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Final

import msgspec
import polars as pl

from loom.etl.schema._schema import (
    ArrayType,
    DatetimeType,
    DecimalType,
    EnumType,
    FrameType,
    ListType,
    LoomDtype,
    StructType,
)

from ._dtype import frame_type_to_polars
from ._json_text import JsonTextDecoder

_CHUNK_ROWS: Final = 8_192
_DECIMAL_TEXT: Final = re.compile(r"-?[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")
_DECODER: Final = msgspec.json.Decoder()
_ENCODER: Final = msgspec.json.Encoder()
_FLOAT32_MAX: Final = 3.4028234663852886e38


@dataclass(frozen=True, slots=True)
class _Accepts:
    """The JSON Python types a leaf accepts, compared by exact type."""

    types: frozenset[type]

    def read(self, value: Any) -> Any:
        """Return *value* when its type is accepted, else ``None``."""
        return value if type(value) in self.types else None


@dataclass(frozen=True, slots=True)
class _Bounded:
    """The JSON numbers a leaf accepts, within an inclusive range."""

    types: frozenset[type]
    lowest: float
    highest: float
    as_type: type

    def read(self, value: Any) -> Any:
        """Return *value* as :attr:`as_type` when it is accepted and in range, else ``None``."""
        accepted = type(value) in self.types and self.lowest <= value <= self.highest
        return self.as_type(value) if accepted else None


_INTEGER: Final = frozenset({int})
_NUMBER: Final = frozenset({int, float})
_LEAF_RULES: Final[Mapping[FrameType, _Accepts | _Bounded]] = {
    LoomDtype.INT8: _Bounded(_INTEGER, -(2**7), 2**7 - 1, int),
    LoomDtype.INT16: _Bounded(_INTEGER, -(2**15), 2**15 - 1, int),
    LoomDtype.INT32: _Bounded(_INTEGER, -(2**31), 2**31 - 1, int),
    LoomDtype.INT64: _Bounded(_INTEGER, -(2**63), 2**63 - 1, int),
    LoomDtype.UINT8: _Bounded(_INTEGER, 0, 2**8 - 1, int),
    LoomDtype.UINT16: _Bounded(_INTEGER, 0, 2**16 - 1, int),
    LoomDtype.UINT32: _Bounded(_INTEGER, 0, 2**32 - 1, int),
    LoomDtype.UINT64: _Bounded(_INTEGER, 0, 2**64 - 1, int),
    LoomDtype.FLOAT32: _Bounded(_NUMBER, -_FLOAT32_MAX, _FLOAT32_MAX, float),
    LoomDtype.FLOAT64: _Bounded(_NUMBER, -sys.float_info.max, sys.float_info.max, float),
    LoomDtype.BOOLEAN: _Accepts(frozenset({bool})),
    LoomDtype.DATE: _Accepts(frozenset({str})),
    LoomDtype.DATETIME: _Accepts(frozenset({str})),
}


def _text_of(value: Any) -> str:
    """Return *value* if it is text, else its JSON text."""
    return value if isinstance(value, str) else _ENCODER.encode(value).decode()


def _decimal_of(value: Any, decimal: DecimalType) -> Any:
    """Return *value* when it fits *decimal*, else ``None``."""
    text = str(value)
    if not _DECIMAL_TEXT.fullmatch(text):
        return None
    integer_digits = decimal.precision - decimal.scale
    return value if abs(Decimal(text)) < Decimal(10) ** integer_digits else None


def _parsed(document: str) -> Any:
    """Return the Python value of *document*, reading ``NaN`` and ``Infinity`` as ``None``."""
    try:
        return _DECODER.decode(document)
    except msgspec.DecodeError:
        return json.loads(document, parse_constant=lambda _: None)


@dataclass(frozen=True, slots=True)
class TolerantJson:
    """Decodes JSON text into *frame_type*, nulling what does not match instead of raising.

    Each row is normalised in Python on its own: a field whose JSON value does not
    fit its declared type becomes null and its siblings are kept, a value under a
    ``str`` field becomes its JSON text, and a row that is not JSON becomes null.
    Dates and timestamps follow :class:`JsonTextDecoder`.

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
        chunks = [batch.slice(offset, _CHUNK_ROWS) for offset in range(0, len(batch), _CHUNK_ROWS)]
        dtype = frame_type_to_polars(self.frame_type)
        column = JsonTextDecoder.nulling().decode(pl.first(), dtype)
        return pl.concat([self._decode_chunk(chunk, column) for chunk in chunks or [batch]])

    def _decode_chunk(self, chunk: pl.Series, column: pl.Expr) -> pl.Series:
        normalised = pl.Series(
            chunk.name, [self._encoded(document) for document in chunk], dtype=pl.Binary
        )
        return normalised.cast(pl.String).to_frame().select(column).to_series()

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
            case ListType(inner=inner):
                if not isinstance(value, list):
                    return None
                return [self._coerce(inner, item) for item in value]
            case ArrayType(inner=inner, width=width):
                if not isinstance(value, list) or len(value) != width:
                    return None
                return [self._coerce(inner, item) for item in value]
            case DecimalType():
                return _decimal_of(value, frame_type)
            case EnumType(categories=categories):
                return value if type(value) is str and value in categories else None
            case LoomDtype.UTF8:
                return _text_of(value)
            case DatetimeType():
                return _LEAF_RULES[LoomDtype.DATETIME].read(value)
            case _:
                rule = _LEAF_RULES.get(frame_type)
                return value if rule is None else rule.read(value)
