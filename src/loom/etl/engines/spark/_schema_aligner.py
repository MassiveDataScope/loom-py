"""Spark schema aligner implementation."""

from __future__ import annotations

from typing import Any

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


class SparkSchemaAligner:
    """Aligns a DataFrame."""

    def column_types(self, frame: DataFrame) -> dict[str, Any]:
        return {f.name: f.dataType for f in frame.schema.fields}

    def iter_schema(self, schema: T.StructType) -> list[tuple[str, Any]]:
        return [(f.name, f.dataType) for f in schema.fields]

    def cast_column(self, name: str, dtype: T.DataType, source: T.DataType) -> tuple[str, Column]:
        """Cast a column; JSON text becomes the struct or array the schema declares for it."""
        if self.is_json_text_target(source, dtype):
            return name, F.from_json(F.col(name), dtype, {"mode": "FAILFAST", "timeZone": "UTC"})
        return name, self._cast_col(name, dtype)

    def is_json_text_target(self, source: T.DataType, dtype: T.DataType) -> bool:
        """Whether a column of type *source* is cast to *dtype* by decoding it; never by default."""
        return False

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


class SparkModelSchemaAligner(SparkSchemaAligner):
    """Aligns to a model's schema, where a text column is JSON for a struct or array field."""

    def is_json_text_target(self, source: T.DataType, dtype: T.DataType) -> bool:
        """Whether a column of type *source* is JSON text for the struct or array *dtype*."""
        return isinstance(source, T.StringType) and isinstance(dtype, T.StructType | T.ArrayType)
