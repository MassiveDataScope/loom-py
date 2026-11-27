"""Tolerant JSON normalisation for ``JsonMismatch.NULL`` columns."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import polars as pl

from loom.etl.schema._schema import ArrayType, FrameType, ListType, LoomDtype, StructType


@dataclass(frozen=True, slots=True)
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
            case LoomDtype.UTF8 if isinstance(value, dict | list):
                return json.dumps(value, ensure_ascii=False)
            case LoomDtype.UTF8 if value is not None:
                return str(value)
            case _:
                return value
