"""Decoding of JSON text columns into Polars dtypes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import polars as pl

from loom.etl.schema import FrameType, JsonFit, JsonMismatch, JsonRefusal

from ._dtype import frame_type_to_polars

_CHUNK_ROWS: Final = 8_192


@dataclass(frozen=True, slots=True)
class JsonDecoder:
    """Decodes JSON text into the column of a frame type, under a mismatch policy.

    Args:
        fit: How each document is fitted to the type.
        dtype: The Polars dtype of the decoded column.
    """

    fit: JsonFit
    dtype: pl.DataType

    @classmethod
    def of(cls, frame_type: FrameType, on_mismatch: JsonMismatch) -> JsonDecoder:
        """The decoder of *frame_type* under *on_mismatch*.

        Raises:
            JsonLeafUnsupported: A leaf of *frame_type* has no JSON rule.
        """
        return cls(JsonFit.of(frame_type, on_mismatch), frame_type_to_polars(frame_type))

    def decode(self, column: pl.Expr) -> pl.Expr:
        """Return *column*, JSON text, decoded into the dtype of this decoder.

        Raises:
            JsonRefusal: When the frame is consumed, under
                :attr:`~loom.etl.declarative.source.JsonMismatch.FAIL`, a value does not fit.
        """
        return column.map_batches(self._series, return_dtype=self.dtype, is_elementwise=True)

    def _series(self, batch: pl.Series) -> pl.Series:
        offsets = range(0, max(len(batch), 1), _CHUNK_ROWS)
        return pl.concat([self._chunk(batch.slice(offset, _CHUNK_ROWS)) for offset in offsets])

    def _chunk(self, batch: pl.Series) -> pl.Series:
        try:
            documents = [self.fit.document(text) for text in batch]
        except JsonRefusal as refusal:
            raise JsonRefusal(f"{refusal}, in JSON column {batch.name}") from refusal
        return pl.Series(batch.name, documents, dtype=self.dtype, strict=True)
