"""Tolerant JSON normalisation for ``JsonMismatch.NULL`` columns."""

from __future__ import annotations

import json
from dataclasses import dataclass
from functools import singledispatchmethod
from typing import Any

import polars as pl

from loom.etl.schema._schema import ArrayType, FrameType, ListType, LoomDtype, StructType


@dataclass(frozen=True)
class TolerantJson:
    """Rewrites JSON text so ``str.json_decode`` of *frame_type* always succeeds.

    Malformed JSON and a struct or list mismatch become null; an object or
    array under a ``str`` field becomes its JSON text. Python runs once per row
    of the column it is applied to.

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

    @singledispatchmethod
    def _coerce(self, frame_type: FrameType, value: Any) -> Any:
        if value is not None and frame_type is LoomDtype.UTF8:
            return self._as_text(value)
        return value

    @_coerce.register
    def _coerce_struct(self, frame_type: StructType, value: Any) -> Any:
        if not isinstance(value, dict):
            return None
        return {f.name: self._coerce(f.dtype, value.get(f.name)) for f in frame_type.fields}

    @_coerce.register(ListType)
    @_coerce.register(ArrayType)
    def _coerce_list(self, frame_type: ListType | ArrayType, value: Any) -> Any:
        if not isinstance(value, list):
            return None
        return [self._coerce(frame_type.inner, item) for item in value]

    @staticmethod
    def _as_text(value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)
