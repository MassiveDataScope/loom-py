"""Contract introspection — convert annotated classes to Loom schema types.

A *contract* is any annotated class (``msgspec.Struct``, ``dataclass``, or
plain Python class with field annotations) or an existing
:data:`~loom.etl._schema.LoomType` / ``tuple[ColumnSchema, ...]``.

Public entry points
-------------------

``resolve_schema``
    Converts a schema contract to ``tuple[ColumnSchema, ...]``.  Used by
    :meth:`~loom.etl._source.FromTable.with_schema` and
    :meth:`~loom.etl._source.FromFile.with_schema` so callers can pass either
    an explicit tuple or an annotated class.

``resolve_json_type``
    Converts a JSON column contract to a single :data:`~loom.etl._schema.LoomType`.
    Used by :meth:`~loom.etl._source.FromTable.parse_json` and
    :meth:`~loom.etl._source.FromFile.parse_json`.

``annotation_to_loom_type``
    Converts one Python type annotation to a :data:`~loom.etl._schema.LoomType`.

Supported annotation forms
--------------------------

* Python primitive types: ``int``, ``float``, ``str``, ``bool``, ``bytes``,
  ``datetime.datetime``, ``datetime.date``.
* Generic lists: ``list[X]`` → :class:`~loom.etl._schema.ListType`.
* ``Optional[X]`` / ``X | None`` → inner type (nullable is the default anyway).
* Annotated class (Struct / dataclass / plain) → :class:`~loom.etl._schema.StructType`.
* Existing :data:`~loom.etl._schema.LoomType` instances → passed through.

Internal module — not part of the public API.
"""

from __future__ import annotations

import datetime
import types as _builtin_types
import typing
from typing import Any, TypeAlias

from loom.etl.schema._schema import (
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
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Public TypeAliases — used in FromTable / FromFile signatures
# ---------------------------------------------------------------------------

#: Contract accepted by ``with_schema``: either an explicit column-schema tuple
#: or an annotated class whose fields are mapped to :class:`ColumnSchema` entries.
SchemaContract: TypeAlias = tuple[ColumnSchema, ...] | type[Any]

#: Contract accepted by ``parse_json``: a :data:`LoomType` (passthrough), an
#: annotated class (→ :class:`StructType`), or a ``list[X]`` generic alias
#: (→ :class:`ListType`).  ``list[X]`` is a ``types.GenericAlias`` at runtime,
#: not a plain ``type``, so the union includes ``Any`` to capture it without
#: widening the documented intent.
JsonContract: TypeAlias = LoomType | type[Any]

# ---------------------------------------------------------------------------
# Primitive Python type → LoomDtype
# ---------------------------------------------------------------------------

_PY_TO_LOOM: dict[type, LoomDtype] = {
    int: LoomDtype.INT64,
    float: LoomDtype.FLOAT64,
    str: LoomDtype.UTF8,
    bool: LoomDtype.BOOLEAN,
    bytes: LoomDtype.BINARY,
    datetime.datetime: LoomDtype.DATETIME,
    datetime.date: LoomDtype.DATE,
}

# All concrete LoomType classes for isinstance checks
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

# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


def resolve_schema(contract: tuple[ColumnSchema, ...] | type[Any]) -> tuple[ColumnSchema, ...]:
    """Convert a schema contract to a ``tuple[ColumnSchema, ...]``.

    Args:
        contract: Either a ``tuple[ColumnSchema, ...]`` (returned as-is) or
                  an annotated class whose fields map to column schemas.

    Returns:
        Tuple of :class:`~loom.etl._schema.ColumnSchema` entries.

    Raises:
        TypeError: If *contract* is not a supported schema contract form.
    """
    if isinstance(contract, tuple):
        return contract
    if isinstance(contract, type):
        return _class_to_schema(contract)
    raise TypeError(
        f"Expected tuple[ColumnSchema, ...] or an annotated class, got {type(contract).__name__!r}."
    )


def resolve_json_type(contract: Any) -> LoomType:
    """Convert a JSON column contract to a :data:`~loom.etl._schema.LoomType`.

    Args:
        contract: A :data:`~loom.etl._schema.LoomType` (returned as-is),
                  an annotated class (converted to :class:`~loom.etl._schema.StructType`),
                  or a ``list[X]`` generic alias (converted to
                  :class:`~loom.etl._schema.ListType`).

    Returns:
        :data:`~loom.etl._schema.LoomType` for use with Polars
        ``str.json_decode`` or Spark ``from_json``.

    Raises:
        TypeError: If *contract* cannot be converted to a LoomType.
    """
    if isinstance(contract, _LOOM_INSTANCES):
        return contract
    if typing.get_origin(contract) is list:
        args = typing.get_args(contract)
        inner = annotation_to_loom_type(args[0]) if args else LoomDtype.UTF8
        return ListType(inner=inner)
    if isinstance(contract, type):
        return _class_to_struct_type(contract)
    raise TypeError(
        f"Expected a LoomType, an annotated class, or list[...] generic; got {contract!r}."
    )


def annotation_to_loom_type(ann: Any) -> LoomType:
    """Convert a Python type annotation to the :data:`~loom.etl.schema.LoomType` it declares.

    This is the mapping :func:`resolve_schema` applies to each field of an
    annotated class. ``X | None`` and ``Optional[X]`` map as ``X``.

    Args:
        ann: ``int``, ``float``, ``str``, ``bool``, ``bytes``,
             ``datetime.datetime``, ``datetime.date``, ``list[X]``, an
             annotated class (mapped to a :class:`~loom.etl.schema.StructType`)
             or a :data:`~loom.etl.schema.LoomType` (returned as-is).

    Returns:
        The matching :data:`~loom.etl.schema.LoomType`.

    Raises:
        TypeError: If *ann* has no Loom equivalent.

    Example::

        >>> annotation_to_loom_type(list[int] | None)
        ListType(inner=<LoomDtype.INT64: 'Int64'>)
    """
    if isinstance(ann, _LOOM_INSTANCES):
        return ann
    if ann in _PY_TO_LOOM:
        return _PY_TO_LOOM[ann]
    is_opt, inner = _strip_optional(ann)
    if is_opt:
        return annotation_to_loom_type(inner)
    origin = typing.get_origin(ann)
    if origin is list:
        args = typing.get_args(ann)
        inner_type = annotation_to_loom_type(args[0]) if args else LoomDtype.UTF8
        return ListType(inner=inner_type)
    if isinstance(ann, type) and hasattr(ann, "__annotations__"):
        return _class_to_struct_type(ann)
    raise TypeError(
        f"Cannot convert annotation {ann!r} to LoomType. "
        "Supported: int, float, str, bool, bytes, datetime, date, list[X], "
        "LoomType instances, and annotated classes."
    )


#: Alias of :func:`annotation_to_loom_type`.
_annotation_to_loom_type = annotation_to_loom_type


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _class_to_schema(cls: type[Any]) -> tuple[ColumnSchema, ...]:
    hints = _get_type_hints(cls)
    return tuple(
        ColumnSchema(name=name, dtype=annotation_to_loom_type(ann)) for name, ann in hints.items()
    )


def _class_to_struct_type(cls: type[Any]) -> StructType:
    hints = _get_type_hints(cls)
    fields = tuple(
        StructField(name=name, dtype=annotation_to_loom_type(ann)) for name, ann in hints.items()
    )
    return StructType(fields=fields)


def _get_type_hints(cls: type[Any]) -> dict[str, Any]:
    try:
        hints = typing.get_type_hints(cls)
    except (TypeError, NameError) as exc:
        raise TypeError(f"Cannot introspect annotations of {cls.__name__!r}: {exc}") from exc
    if not hints:
        raise TypeError(
            f"Class {cls.__name__!r} has no type annotations. "
            "Add field annotations or pass an explicit tuple[ColumnSchema, ...] instead."
        )
    return hints


def _strip_optional(ann: Any) -> tuple[bool, Any]:
    """Return ``(True, inner)`` if *ann* is ``Optional[X]`` or ``X | None``."""
    is_new_union = isinstance(ann, _builtin_types.UnionType)
    is_old_union = typing.get_origin(ann) is typing.Union
    if not (is_new_union or is_old_union):
        return False, ann
    args = typing.get_args(ann)
    non_none = [a for a in args if a is not type(None)]
    if len(non_none) == 1 and len(non_none) < len(args):
        return True, non_none[0]
    return False, ann
