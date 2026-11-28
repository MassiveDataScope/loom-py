"""Decoding of JSON text columns into Polars dtypes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import polars as pl
from polars.datatypes import DataTypeClass

from loom.etl.schema._json_fit import JsonFit, JsonMismatch, JsonRefusal
from loom.etl.schema._schema import FrameType

from ._dtype import frame_type_to_polars

_CHUNK_ROWS: Final = 8_192


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
        JsonFit.of(self.frame_type, self.on_mismatch)
        return column.map_batches(
            self._decode_batch,
            return_dtype=frame_type_to_polars(self.frame_type),
            is_elementwise=True,
        )

    def _decode_batch(self, batch: pl.Series) -> pl.Series:
        fit = JsonFit.of(self.frame_type, self.on_mismatch)
        dtype = frame_type_to_polars(self.frame_type)
        readable = _readable_dtype(dtype)
        chunks = [batch.slice(offset, _CHUNK_ROWS) for offset in range(0, len(batch), _CHUNK_ROWS)]
        decoded = pl.concat(
            [self._decode_chunk(fit, chunk, readable) for chunk in chunks or [batch]]
        )
        return decoded if readable == dtype else decoded.cast(dtype)

    def _decode_chunk(self, fit: JsonFit, chunk: pl.Series, readable: pl.DataType) -> pl.Series:
        try:
            documents = [fit.document(text) for text in chunk]
        except JsonRefusal as refusal:
            raise JsonRefusal(
                f"{refusal}, in JSON column {chunk.name} as {self.frame_type}"
            ) from refusal
        return (
            pl.Series(chunk.name, documents, dtype=pl.Binary)
            .cast(pl.String)
            .str.json_decode(readable)
        )
