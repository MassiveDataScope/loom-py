"""Contract introspection — convert annotated classes to Loom schema types.

A *contract* is any annotated class (``msgspec.Struct``, ``dataclass``, or
plain Python class with field annotations) or an existing
:data:`~loom.etl.schema.FrameType` / ``tuple[ColumnSchema, ...]``.

Public entry points
-------------------

``resolve_schema``
    Converts a schema contract to ``tuple[ColumnSchema, ...]``.  Used by
    :meth:`~loom.etl.declarative.source.FromTable.with_schema` and
    :meth:`~loom.etl.declarative.source.FromFile.with_schema` so callers can pass either
    an explicit tuple or an annotated class.

Every annotation goes through core inference and the lake translator of
:mod:`loom.etl.schema._model`, so a model, a msgspec struct, a dataclass and a
plain annotated class with the same annotations give the same schema.
``LakeTranslator.json_type`` and ``LakeTranslator.annotation_type`` resolve the
JSON column contract of ``parse_json`` and a single annotation.

Internal module — not part of the public API.
"""

from __future__ import annotations

from typing import Any, TypeAlias

from loom.core.errors import InvalidType
from loom.etl.schema._model import LakeTranslator
from loom.etl.schema._schema import ColumnSchema, FrameType

SchemaContract: TypeAlias = tuple[ColumnSchema, ...] | type[Any]
"""Contract accepted by ``with_schema``: a column-schema tuple or an annotated class."""

JsonContract: TypeAlias = FrameType | type[Any]
"""Contract accepted by ``parse_json``: a :data:`FrameType`, an annotated class or ``list[X]``."""


def resolve_schema(contract: tuple[ColumnSchema, ...] | type[Any]) -> tuple[ColumnSchema, ...]:
    """Convert a schema contract to a ``tuple[ColumnSchema, ...]``.

    Args:
        contract: Either a ``tuple[ColumnSchema, ...]`` (returned as-is) or
                  an annotated class whose fields map to column schemas.

    Returns:
        Tuple of :class:`~loom.etl.schema.ColumnSchema` entries.

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
