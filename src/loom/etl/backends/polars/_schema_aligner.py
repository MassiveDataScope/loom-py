"""Polars schema aligner implementation."""

from __future__ import annotations

from typing import Any

import polars as pl
from polars.datatypes import DataTypeClass as _DataTypeClass

from loom.etl.schema._schema import SchemaError

_Dtype = pl.DataType | _DataTypeClass


def refuse_lossy_casts(frame_schema: pl.Schema, table_schema: pl.Schema) -> None:
    """Refuse a frame column whose cast to the table's type would lose its values.

    A ``Struct`` cast to ``String`` becomes Polars' display text (``{1.0}``)
    and a ``String`` cast to a ``List`` wraps or nulls each value; a ``List``
    cast to ``String`` and a ``String`` aligned to a ``Struct`` fail inside
    Polars without naming the table's type. None of them is applied, at any
    depth of a ``Struct`` or ``List``.

    Raises:
        SchemaError: Naming the first such column and both types.
    """
    for name, table_dtype in table_schema.items():
        frame_dtype = frame_schema.get(name)
        if frame_dtype is not None and _is_lossy(frame_dtype, table_dtype):
            raise SchemaError(
                f"column {name!r} is {frame_dtype} in the frame and {table_dtype} in the "
                "table; casting one to the other loses its values. Rewrite the table with "
                "SchemaMode.OVERWRITE to change the column's type."
            )


def _is_lossy(source: _Dtype, target: _Dtype) -> bool:
    if isinstance(source, pl.Struct) and isinstance(target, pl.Struct):
        fields = {field.name: field.dtype for field in source.fields}
        return any(
            field.name in fields and _is_lossy(fields[field.name], field.dtype)
            for field in target.fields
        )
    if isinstance(source, pl.List) and isinstance(target, pl.List):
        return _is_lossy(source.inner, target.inner)
    return (_is_nested(source) and target == pl.String) or (
        source == pl.String and _is_nested(target)
    )


def _is_nested(dtype: _Dtype) -> bool:
    return isinstance(dtype, pl.Struct | pl.List)


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
