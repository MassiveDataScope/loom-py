"""Schema, contracts and model tables for ETL."""

from __future__ import annotations

from ._contract import (
    JsonContract,
    SchemaContract,
    annotation_to_frame_type,
    resolve_json_type,
    resolve_schema,
)
from ._model import FrameKey, KeyProblem, LakeRefusal, ModelTable
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
    "ModelTable",
    "FrameKey",
    "KeyProblem",
    "LakeRefusal",
]
