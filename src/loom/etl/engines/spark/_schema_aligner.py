"""Spark schema aligner implementation."""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


class SparkSchemaAligner:
    def get_frame_columns(self, frame: DataFrame) -> set[str]:
        return set(frame.columns)

    def iter_schema(self, schema: T.StructType) -> list[tuple[str, Any]]:
        return [(f.name, f.dataType) for f in schema.fields]

    def cast_column(self, name: str, dtype: T.DataType) -> tuple[str, Column]:
        return name, self._cast_col(name, dtype)

    def _cast_col(self, col_ref: str, dtype: T.DataType) -> Column:
        if isinstance(dtype, T.StructType):
            field_exprs = [
                self._cast_col(f"{col_ref}.{f.name}", f.dataType).alias(f.name)
                for f in dtype.fields
            ]
            return F.struct(field_exprs)
        return F.col(col_ref).cast(dtype)

    def null_column(self, name: str, dtype: T.DataType) -> tuple[str, Column]:
        return name, F.lit(None).cast(dtype)

    def apply_casts(self, frame: DataFrame, casts: list[tuple[str, Column]]) -> DataFrame:
        for name, expr in casts:
            frame = frame.withColumn(name, expr)
        return frame

    def select_columns(self, frame: DataFrame, names: list[str]) -> DataFrame:
        return frame.select(names)
