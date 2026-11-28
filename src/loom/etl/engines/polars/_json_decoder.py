"""Decoding of JSON text columns into Polars dtypes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from functools import partial
from typing import Final

import msgspec
import polars as pl
from polars.datatypes import DataTypeClass

from loom.etl.schema import (
    DatetimeType,
    DecimalType,
    FrameType,
    JsonFit,
    JsonMismatch,
    JsonRefusal,
    LoomDtype,
)
from loom.etl.schema._json_fit import Misfit, Rule

from ._dtype import frame_type_to_polars

_EPOCH_DATE: Final = date(1970, 1, 1)
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_MICROSECOND: Final = timedelta(microseconds=1)
_NANOSECOND_INSTANTS: Final = 2**63 // 1000
_CHUNK_ROWS: Final = 8_192
_ENCODER: Final = msgspec.json.Encoder()


class _EpochDays:
    """A date as the days from the epoch."""

    def fit(self, value: date) -> int:
        """The days from the epoch to *value*."""
        return (value - _EPOCH_DATE).days


class _InstantText:
    """An instant as ``YYYY-MM-DDTHH:MM:SS.ffffffZ`` text, within the range of its time unit."""

    def __init__(self, time_unit: str) -> None:
        self._holds_microseconds = _NANOSECOND_INSTANTS if time_unit == "ns" else None

    def fit(self, value: datetime) -> str:
        """*value* as text.

        Raises:
            Misfit: The 64-bit count of nanoseconds cannot hold *value*.
        """
        reach = value if value.tzinfo else value.replace(tzinfo=UTC)
        limit = self._holds_microseconds
        if limit is not None and abs((reach - _EPOCH) // _MICROSECOND) > limit:
            raise Misfit("an instant its timestamp type holds")
        return value.replace(tzinfo=None).isoformat(timespec="microseconds") + "Z"


class _PlainDecimal:
    """A decimal as its digits, never in scientific notation."""

    def fit(self, value: Decimal) -> str:
        """*value* in positional notation."""
        return format(value, "f")


def _reader_rule(frame_type: FrameType) -> Rule | None:
    """The rule that writes the fitted value of a *frame_type* leaf the way Polars reads it."""
    match frame_type:
        case LoomDtype.DATE:
            return _EpochDays()
        case DatetimeType(time_unit=time_unit):
            return _InstantText(time_unit)
        case LoomDtype.DATETIME:
            return _InstantText("us")
        case DecimalType():
            return _PlainDecimal()
        case _:
            return None


def _readable_dtype(dtype: pl.DataType | DataTypeClass) -> pl.DataType:
    """*dtype* with every array read as a list and every timestamp zone as UTC."""
    resolved = dtype() if isinstance(dtype, DataTypeClass) else dtype
    match resolved:
        case pl.Datetime():
            return pl.Datetime(resolved.time_unit, "UTC") if resolved.time_zone else resolved
        case pl.Struct():
            return pl.Struct([pl.Field(f.name, _readable_dtype(f.dtype)) for f in resolved.fields])
        case pl.List() | pl.Array():
            return pl.List(_readable_dtype(resolved.inner))
        case _:
            return resolved


@dataclass(frozen=True, slots=True)
class JsonDecoder:
    """Decodes JSON text into *frame_type*; a value that does not fit follows *on_mismatch*.

    Each document is fitted to the type in Python and Polars' JSON reader then builds
    the column from the fitted text.

    Args:
        frame_type: Type the column is decoded into.
        on_mismatch: Whether a value that does not fit raises or becomes null.
    """

    frame_type: FrameType
    on_mismatch: JsonMismatch

    def decode(self, column: pl.Expr) -> pl.Expr:
        """Return *column*, JSON text, decoded into :attr:`frame_type`.

        Raises:
            JsonLeafUnsupported: A leaf of :attr:`frame_type` has no JSON rule.
            JsonRefusal: When the frame is consumed, under
                :attr:`~loom.etl.declarative.source.JsonMismatch.FAIL`, a value does not fit.
        """
        fit = JsonFit.of(self.frame_type, self.on_mismatch, _reader_rule)
        dtype = frame_type_to_polars(self.frame_type)
        return column.map_batches(
            partial(_decode_batch, fit, dtype, _readable_dtype(dtype)),
            return_dtype=dtype,
            is_elementwise=True,
        )


def _decode_batch(
    fit: JsonFit, dtype: pl.DataType, readable: pl.DataType, batch: pl.Series
) -> pl.Series:
    chunks = [batch.slice(offset, _CHUNK_ROWS) for offset in range(0, len(batch), _CHUNK_ROWS)]
    decoded = pl.concat([_decode_chunk(fit, chunk, readable) for chunk in chunks or [batch]])
    return decoded if readable == dtype else decoded.cast(dtype)


def _decode_chunk(fit: JsonFit, chunk: pl.Series, readable: pl.DataType) -> pl.Series:
    try:
        fitted = [fit.document(text) for text in chunk]
    except JsonRefusal as refusal:
        raise refusal.in_column(chunk.name) from refusal
    return (
        pl.Series(
            chunk.name,
            [None if document is None else _ENCODER.encode(document) for document in fitted],
            dtype=pl.Binary,
        )
        .cast(pl.String)
        .str.json_decode(readable)
    )
