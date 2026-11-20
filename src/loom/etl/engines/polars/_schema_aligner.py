"""Polars schema aligner implementation."""

from __future__ import annotations

from typing import Any

import polars as pl
from polars.datatypes import DataTypeClass as _DataTypeClass


class PolarsSchemaAligner:
    def get_frame_columns(self, frame: pl.LazyFrame) -> set[str]:
        return set(frame.collect_schema().names())

    def iter_schema(self, schema: pl.Schema) -> list[tuple[str, Any]]:
        return [(name, dtype) for name, dtype in schema.items()]

    def cast_column(self, name: str, dtype: pl.DataType | _DataTypeClass) -> tuple[str, pl.Expr]:
        return name, self._cast_expr(pl.col(name), dtype)

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

    def apply_casts(self, frame: pl.LazyFrame, casts: list[tuple[str, pl.Expr]]) -> pl.LazyFrame:
        return frame.with_columns([expr.alias(name) for name, expr in casts])

    def select_columns(self, frame: pl.LazyFrame, names: list[str]) -> pl.LazyFrame:
        return frame.select(names)
