"""Contract introspection — convert annotated classes to Loom schema types.

A *contract* is any annotated class (``msgspec.Struct``, ``dataclass``, or
plain Python class with field annotations) or an existing
:data:`~loom.etl._schema.FrameType` / ``tuple[ColumnSchema, ...]``.

Public entry points
-------------------

``resolve_schema``
    Converts a schema contract to ``tuple[ColumnSchema, ...]``.  Used by
    :meth:`~loom.etl._source.FromTable.with_schema` and
    :meth:`~loom.etl._source.FromFile.with_schema` so callers can pass either
    an explicit tuple or an annotated class.

``resolve_json_type``
    Converts a JSON column contract to a single :data:`~loom.etl._schema.FrameType`.
    Used by :meth:`~loom.etl._source.FromTable.parse_json` and
    :meth:`~loom.etl._source.FromFile.parse_json`.

``annotation_to_frame_type``
    Converts one Python type annotation to a :data:`~loom.etl._schema.FrameType`.

Every annotation goes through core inference and the lake translator of
:mod:`loom.etl.schema._model`, so a model, a msgspec struct, a dataclass and a
plain annotated class with the same annotations give the same schema.

Internal module — not part of the public API.
"""

from __future__ import annotations

import typing
from typing import Any, TypeAlias

from loom.core.errors import InvalidType
from loom.core.model.inference import resolve_annotation
from loom.etl.schema._model import LakeTranslator
from loom.etl.schema._schema import (
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
    StructType,
)

SchemaContract: TypeAlias = tuple[ColumnSchema, ...] | type[Any]
"""Contract accepted by ``with_schema``: a column-schema tuple or an annotated class."""

JsonContract: TypeAlias = FrameType | type[Any]
"""Contract accepted by ``parse_json``: a :data:`FrameType`, an annotated class or ``list[X]``."""


_LOOM_INSTANCES = (
    LoomDtype,
    ListType,
    ArrayType,
    StructType,
    DecimalType,
    DatetimeType,
    DurationType,
    CategoricalType,
    EnumType,
)


def resolve_schema(contract: tuple[ColumnSchema, ...] | type[Any]) -> tuple[ColumnSchema, ...]:
    """Convert a schema contract to a ``tuple[ColumnSchema, ...]``.

    Args:
        contract: Either a ``tuple[ColumnSchema, ...]`` (returned as-is) or
                  an annotated class whose fields map to column schemas.

    Returns:
        Tuple of :class:`~loom.etl._schema.ColumnSchema` entries.

    Raises:
        InvalidType: If *contract* is not a supported schema contract form.
    """
    if isinstance(contract, tuple):
        return contract
    if isinstance(contract, type):
        return LakeTranslator(contract.__name__).schema_of(contract)
    raise InvalidType(
        f"Expected tuple[ColumnSchema, ...] or an annotated class, got {type(contract).__name__!r}."
    )


def resolve_json_type(contract: Any) -> FrameType:
    """Convert a JSON column contract to a :data:`~loom.etl._schema.FrameType`.

    Args:
        contract: A :data:`~loom.etl._schema.FrameType` (returned as-is),
                  an annotated class (converted to
                  :class:`~loom.etl._schema.StructType`), or a ``list[X]`` generic alias
                  (converted to :class:`~loom.etl._schema.ListType`).

    Returns:
        :data:`~loom.etl._schema.FrameType` for use with Polars
        ``str.json_decode`` or Spark ``from_json``.

    Raises:
        InvalidType: If *contract* cannot be converted to a FrameType.
    """
    if isinstance(contract, _LOOM_INSTANCES):
        return contract
    if typing.get_origin(contract) is list:
        return annotation_to_frame_type(contract)
    if isinstance(contract, type):
        return LakeTranslator(contract.__name__).struct_of(contract)
    raise InvalidType("Expected a FrameType, an annotated class, or list[...] generic.")


def annotation_to_frame_type(ann: Any) -> FrameType:
    """Convert a Python type annotation to the :data:`~loom.etl.schema.FrameType` it declares.

    This is the mapping :func:`resolve_schema` applies to each field of an
    annotated class. ``X | None`` and ``Optional[X]`` map as ``X``.

    Args:
        ann: A scalar type, ``list[X]``, a record (msgspec struct or dataclass,
             mapped to a :class:`~loom.etl.schema.StructType`) or a
             :data:`~loom.etl.schema.FrameType` (returned as-is).

    Returns:
        The matching :data:`~loom.etl.schema.FrameType`.

    Raises:
        InvalidValue: If the lake has no type for *ann*.

    Example::

        >>> annotation_to_frame_type(list[int] | None)
        ListType(inner=<LoomDtype.INT64: 'Int64'>)
    """
    if isinstance(ann, _LOOM_INSTANCES):
        return ann
    return LakeTranslator("annotation").frame_type(resolve_annotation(ann))
