"""Schema, contracts, and table-reference primitives for ETL."""

from __future__ import annotations

from loom.etl.declarative.expr._refs import TableRef, col

from ._contract import (
    JsonContract,
    SchemaContract,
    annotation_to_loom_type,
    resolve_json_type,
    resolve_schema,
)
from ._schema import (
    ArrayType,
    CategoricalType,
    ColumnSchema,
    DatetimeType,
    DecimalType,
    DurationType,
    EnumType,
    ListType,
    LoomDtype,
    LoomType,
    SchemaError,
    SchemaNotFoundError,
    StructField,
    StructType,
)

__all__ = [
    "SchemaContract",
    "JsonContract",
    "resolve_schema",
    "resolve_json_type",
    "annotation_to_loom_type",
    "ColumnSchema",
    "LoomDtype",
    "LoomType",
    "SchemaNotFoundError",
    "SchemaError",
    "ListType",
    "ArrayType",
    "StructType",
    "StructField",
    "DecimalType",
    "DatetimeType",
    "DurationType",
    "CategoricalType",
    "EnumType",
    "TableRef",
    "col",
]
