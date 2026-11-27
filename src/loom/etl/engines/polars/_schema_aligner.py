"""Polars schema aligner implementation."""

from __future__ import annotations

from typing import Any, Generic, TypeVar

import polars as pl
from polars.datatypes import DataTypeClass as _DataTypeClass

PolarsFrameT = TypeVar("PolarsFrameT", pl.LazyFrame, pl.DataFrame)


class PolarsSchemaAligner(Generic[PolarsFrameT]):
    """Aligns a lazy or an eager frame; the aligner is the frame type it was built for.

    Args:
        decodes_json_text: Whether a text column is decoded into the struct or list the
            schema declares for it, instead of cast.
    """

    def __init__(self, decodes_json_text: bool = False) -> None:
        self._decodes_json_text = decodes_json_text

    def column_types(self, frame: PolarsFrameT) -> dict[str, Any]:
        return dict(frame.collect_schema())

    def iter_schema(self, schema: pl.Schema) -> list[tuple[str, Any]]:
        return [(name, dtype) for name, dtype in schema.items()]

    def cast_column(
        self, name: str, dtype: pl.DataType | _DataTypeClass, source: pl.DataType | _DataTypeClass
    ) -> tuple[str, pl.Expr]:
        """Cast a column; JSON text becomes the struct or list its model declares."""
        column = pl.col(name)
        if self._decodes_json_text and self.is_json_text(source, dtype):
            return name, column.str.json_decode(dtype)
        return name, self._cast_expr(column, dtype)

    @staticmethod
    def is_json_text(
        source: pl.DataType | _DataTypeClass, dtype: pl.DataType | _DataTypeClass
    ) -> bool:
        """Whether a column of type *source* is JSON text for the struct or list *dtype*."""
        return source == pl.String and isinstance(dtype, pl.Struct | pl.List)

    def _cast_expr(self, expr: pl.Expr, dtype: pl.DataType | _DataTypeClass) -> pl.Expr:
        if isinstance(dtype, pl.Struct):
            field_exprs = [
                self._cast_expr(expr.struct.field(f.name), f.dtype).alias(f.name)
                for f in dtype.fields
            ]
            return pl.struct(field_exprs)
        return expr.cast(dtype)

    def null_column(self, name: str, dtype: pl.DataType | _DataTypeClass) -> tuple[str, pl.Expr]:
        return name, pl.lit(None).cast(dtype)

    def apply_casts(self, frame: PolarsFrameT, casts: list[tuple[str, pl.Expr]]) -> PolarsFrameT:
        return frame.with_columns([expr.alias(name) for name, expr in casts])

    def select_columns(self, frame: PolarsFrameT, names: list[str]) -> PolarsFrameT:
        return frame.select(names)
