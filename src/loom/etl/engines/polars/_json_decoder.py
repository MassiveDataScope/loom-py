"""Decoding of JSON text columns into Polars dtypes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import polars as pl
import pyarrow as pa

from loom.etl.schema import FrameType, JsonFit, JsonMismatch, JsonRefusal

from ._dtype import frame_type_to_polars

_CHUNK_ROWS: Final = 8_192


def _build_type(arrow_type: pa.DataType) -> pa.DataType:
    """*arrow_type* with every dictionary a plain value and every large list a list."""
    if pa.types.is_dictionary(arrow_type):
        return arrow_type.value_type
    if pa.types.is_struct(arrow_type):
        return pa.struct([field.with_type(_build_type(field.type)) for field in arrow_type])
    if pa.types.is_fixed_size_list(arrow_type):
        return pa.list_(_build_type(arrow_type.value_type), arrow_type.list_size)
    if pa.types.is_large_list(arrow_type):
        return pa.list_(_build_type(arrow_type.value_type))
    return arrow_type


@dataclass(frozen=True, slots=True)
class JsonDecoder:
    """Decodes JSON text into the column of a frame type, under a mismatch policy.

    Args:
        fit: How each document is fitted to the type.
        dtype: The Polars dtype of the decoded column.
        arrow_type: The Arrow type the fitted values are built as.
    """

    fit: JsonFit
    dtype: pl.DataType
    arrow_type: pa.DataType

    @classmethod
    def of(cls, frame_type: FrameType, on_mismatch: JsonMismatch) -> JsonDecoder:
        """The decoder of *frame_type* under *on_mismatch*.

        Raises:
            JsonLeafUnsupported: A leaf of *frame_type* has no JSON rule.
        """
        fit = JsonFit.of(frame_type, on_mismatch)
        dtype = frame_type_to_polars(frame_type)
        return cls(fit, dtype, _build_type(pl.Series(dtype=dtype).to_arrow().type))

    def decode(self, column: pl.Expr) -> pl.Expr:
        """Return *column*, JSON text, decoded into the dtype of this decoder.

        Raises:
            JsonRefusal: When the frame is consumed, under
                :attr:`~loom.etl.declarative.source.JsonMismatch.FAIL`, a value does not fit.
        """
        return column.map_batches(self._series, return_dtype=self.dtype, is_elementwise=True)

    def _series(self, batch: pl.Series) -> pl.Series:
        if len(batch) <= _CHUNK_ROWS:
            return self._chunk(batch)
        offsets = range(0, len(batch), _CHUNK_ROWS)
        return pl.concat([self._chunk(batch.slice(offset, _CHUNK_ROWS)) for offset in offsets])

    def _chunk(self, batch: pl.Series) -> pl.Series:
        try:
            documents = [self.fit.document(text) for text in batch]
        except JsonRefusal as refusal:
            raise JsonRefusal(f"{refusal}, in JSON column {batch.name}") from refusal
        return pl.Series(batch.name, pa.array(documents, type=self.arrow_type)).cast(
            self.dtype, strict=False
        )
