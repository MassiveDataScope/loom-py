"""Schema, contracts, and table-reference primitives for ETL."""

from __future__ import annotations

from loom.etl.declarative.expr._refs import TableRef, col

from ._contract import (
    JsonContract,
    SchemaContract,
    annotation_to_frame_type,
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
    FrameType,
    ListType,
    LoomDtype,
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
    "annotation_to_frame_type",
    "ColumnSchema",
    "LoomDtype",
    "FrameType",
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
