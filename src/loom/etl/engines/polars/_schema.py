"""Polars physical schema types and Delta schema reader."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TypeGuard, cast

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError
from polars.datatypes import DataTypeClass

from loom.core.errors import InvalidType, InvalidValue
from loom.etl.declarative.target import SchemaMode
from loom.etl.engines._schema_aligner import SchemaAlignmentPolicy
from loom.etl.engines.polars._dtype import frame_type_to_polars
from loom.etl.engines.polars._schema_aligner import PolarsFrameT, PolarsSchemaAligner
from loom.etl.schema._schema import SchemaError, StructType

_log = logging.getLogger(__name__)

__all__ = [
    "PolarsPhysicalSchema",
    "apply_schema_polars",
    "read_delta_physical_schema",
]


@dataclass(frozen=True)
class PolarsPhysicalSchema:
    """Physical schema snapshot for a resolved Polars target.

    Args:
        schema: Native Polars schema for write-time alignment.
        partition_columns: Ordered partition columns.
        decodes_json_text: Whether a frame column of JSON text is decoded into the struct or
            list the schema declares, as it is for a model's schema.
    """

    schema: pl.Schema
    partition_columns: tuple[str, ...] = ()
    decodes_json_text: bool = False

    @classmethod
    def of_model(cls, struct: StructType) -> PolarsPhysicalSchema:
        """The physical schema of a model's *struct*: its fields in order, JSON text decoded."""
        polars_struct = cast(pl.Struct, frame_type_to_polars(struct))
        return cls(schema=pl.Schema(polars_struct.to_schema()), decodes_json_text=True)

    def align(self, frame: PolarsFrameT, mode: SchemaMode) -> PolarsFrameT:
        """Align *frame* with this schema under *mode*, refusing casts that lose values.

        Raises:
            SchemaError: A column's cast to the table's type would lose its values.
        """
        if mode is not SchemaMode.OVERWRITE:
            self._refuse_lossy_casts(frame.collect_schema())
        aligner = PolarsSchemaAligner[PolarsFrameT](self.decodes_json_text)
        return SchemaAlignmentPolicy(aligner).align(frame, self.schema, mode)

    def _refuse_lossy_casts(self, frame_schema: pl.Schema) -> None:
        """Refuse a frame column whose cast to the table's type would lose its values.

        A ``Struct`` cast to ``String`` becomes Polars' display text (``{1.0}``
        for a ``Float64`` field) and a ``String`` cast to a ``List`` wraps or
        nulls each value; a ``List`` cast to ``String`` and a ``String`` aligned
        to a ``Struct`` fail inside Polars without naming the table's type. None
        of them is applied, at any depth of a ``Struct`` or ``List``. The check is
        by dtype, so an all-null ``String`` column is refused for a ``List`` too. With
        *decodes_json_text* a top-level ``String`` column is JSON text, decoded rather than cast,
        so it is not refused.
        The Spark writer does not refuse these casts: it writes a ``Struct`` or
        ``List`` into a ``String`` column as text (``{1.0}``, ``[1]``).

        Raises:
            SchemaError: Naming the first such column and both types.
        """
        for name, table_dtype in self.schema.items():
            frame_dtype = frame_schema.get(name)
            if frame_dtype is None:
                continue
            if self.decodes_json_text and PolarsSchemaAligner.is_json_text(
                frame_dtype, table_dtype
            ):
                continue
            if _is_lossy(frame_dtype, table_dtype):
                raise SchemaError(
                    f"column {name!r} is {frame_dtype} in the frame and {table_dtype} in the "
                    "table; casting one to the other loses its values. Rewrite the table with "
                    "SchemaMode.OVERWRITE to change the column's type."
                )


def apply_schema_polars(
    frame: PolarsFrameT, schema: pl.Schema | None, mode: SchemaMode
) -> PolarsFrameT:
    """Align *frame* with a plain *schema*; ``None`` is a table with no schema."""
    if schema is not None:
        return PolarsPhysicalSchema(schema).align(frame, mode)
    return SchemaAlignmentPolicy(PolarsSchemaAligner[PolarsFrameT]()).align(frame, None, mode)


def _is_lossy(source: pl.DataType | DataTypeClass, target: pl.DataType | DataTypeClass) -> bool:
    if isinstance(source, pl.Struct) and isinstance(target, pl.Struct):
        fields = {field.name: field.dtype for field in source.fields}
        return any(
            field.name in fields and _is_lossy(fields[field.name], field.dtype)
            for field in target.fields
        )
    if isinstance(source, pl.List) and isinstance(target, pl.List):
        return _is_lossy(source.inner, target.inner)
    return (isinstance(source, pl.Struct | pl.List) and target == pl.String) or (
        source == pl.String and isinstance(target, pl.Struct | pl.List)
    )


_DECIMAL_TYPE_RE = re.compile(r"^decimal\((?P<precision>\d+),(?P<scale>\d+)\)$")
_PRIMITIVE_TYPES: dict[str, Callable[[], pl.DataType]] = {
    "byte": pl.Int8,
    "short": pl.Int16,
    "integer": pl.Int32,
    "long": pl.Int64,
    "float": pl.Float32,
    "double": pl.Float64,
    "boolean": pl.Boolean,
    "string": pl.String,
    "binary": pl.Binary,
    "date": pl.Date,
    "timestamp": lambda: pl.Datetime("us", "UTC"),
    "timestamp_ntz": lambda: pl.Datetime("us"),
    "null": pl.Null,
    "void": pl.Null,
}


def read_delta_physical_schema(
    uri: str,
    storage_options: dict[str, str] | None = None,
) -> PolarsPhysicalSchema | None:
    """Read physical schema directly from a Delta table URI.

    Args:
        uri: Delta table URI/path (including ``uc://catalog.schema.table``).
        storage_options: Optional delta-rs storage options.

    Returns:
        Physical schema when the table exists, else ``None``.
    """
    try:
        dt = DeltaTable(uri, storage_options=storage_options or None)
    except TableNotFoundError:
        return None
    schema_json = _delta_schema_to_json(dt.schema())
    return PolarsPhysicalSchema(
        schema=_delta_json_to_polars_schema(schema_json),
        partition_columns=tuple(dt.metadata().partition_columns),
    )


def _delta_schema_to_json(raw_schema: object) -> object:
    """Extract JSON-serializable schema from DeltaTable schema object."""
    to_json_method = getattr(raw_schema, "to_json", None)
    if callable(to_json_method):
        payload = to_json_method()
        if isinstance(payload, str):
            return json.loads(payload)

    json_method = getattr(raw_schema, "json", None)
    if not callable(json_method):
        raise InvalidType(f"Unsupported Delta schema object: {type(raw_schema)!r}")
    return json_method()


def _delta_json_to_polars_schema(raw_schema: object) -> pl.Schema:
    """Convert Delta JSON schema to Polars Schema."""
    if not isinstance(raw_schema, Mapping):
        raise InvalidType(f"Unsupported Delta schema payload: {type(raw_schema)!r}")

    root_type = raw_schema.get("type")
    if root_type != "struct":
        raise InvalidType("Delta root schema must be struct")

    fields = raw_schema.get("fields")
    return pl.Schema(_struct_fields_to_dict(fields))


def _struct_fields_to_dict(raw_fields: object) -> dict[str, pl.DataType]:
    """Convert Delta struct fields to Polars field dict."""
    if not _is_sequence(raw_fields):
        raise InvalidType(f"Delta struct fields must be a sequence, got {type(raw_fields)!r}")
    field_nodes: Sequence[object] = tuple(raw_fields)

    fields: dict[str, pl.DataType] = {}
    for raw_field in field_nodes:
        name, dtype = _parse_field(raw_field)
        fields[name] = dtype
    return fields


def _parse_field(raw_field: object) -> tuple[str, pl.DataType]:
    """Parse a single Delta field into (name, dtype)."""
    if not isinstance(raw_field, Mapping):
        raise InvalidType(f"Delta field must be a mapping, got {type(raw_field)!r}")
    name = raw_field.get("name")
    if not isinstance(name, str):
        raise InvalidType("Delta field name must be a str")
    if not name:
        raise InvalidValue("Delta field name must not be empty")
    return name, _delta_type_to_polars(raw_field.get("type"))


def _delta_type_to_polars(raw_type: object) -> pl.DataType:
    """Convert Delta type to Polars DataType."""
    if isinstance(raw_type, str):
        primitive = _PRIMITIVE_TYPES.get(raw_type)
        if primitive is not None:
            return primitive()
        decimal_match = _DECIMAL_TYPE_RE.match(raw_type)
        if decimal_match is not None:
            precision = int(decimal_match.group("precision"))
            scale = int(decimal_match.group("scale"))
            return pl.Decimal(precision=precision, scale=scale)
        raise InvalidType("Unsupported Delta primitive type")

    if not isinstance(raw_type, Mapping):
        raise InvalidType(f"Delta type node must be str or mapping, got {type(raw_type)!r}")

    node_type = raw_type.get("type")
    if node_type == "array":
        return pl.List(_delta_type_to_polars(raw_type.get("elementType")))
    if node_type == "struct":
        return pl.Struct(_struct_fields_to_dict(raw_type.get("fields")))
    if node_type == "map":
        key_type = _delta_type_to_polars(raw_type.get("keyType"))
        value_type = _delta_type_to_polars(raw_type.get("valueType"))
        return pl.List(pl.Struct({"key": key_type, "value": value_type}))
    raise InvalidType("Unsupported Delta nested type node")


def _is_sequence(value: object) -> TypeGuard[Sequence[object]]:
    return isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray)
