"""Build typed structs from values keyed by internal field name."""

from __future__ import annotations

import ipaddress
from collections.abc import Mapping
from functools import cache
from typing import Any, TypeVar, get_args, get_origin

import msgspec

StructT = TypeVar("StructT", bound=msgspec.Struct)
# What a Postgres driver returns for ``inet``; a model field holds its text form.
_NETWORK_VALUES = (
    ipaddress.IPv4Address,
    ipaddress.IPv6Address,
    ipaddress.IPv4Interface,
    ipaddress.IPv6Interface,
)
_FieldPlan = dict[str, tuple[str, type[msgspec.Struct] | None]]


def _nested_struct(annotation: Any) -> type[msgspec.Struct] | None:
    """Return the struct type *annotation* wraps (``X``, ``list[X]``, ``X | None``)."""
    if isinstance(annotation, type):
        return annotation if issubclass(annotation, msgspec.Struct) else None
    origin = get_origin(annotation)
    if isinstance(origin, type) and issubclass(origin, Mapping):
        return None
    for arg in get_args(annotation):
        nested = _nested_struct(arg)
        if nested is not None:
            return nested
    return None


@cache
def _field_plan(struct_type: type[msgspec.Struct]) -> _FieldPlan:
    return {
        field.name: (field.encode_name or field.name, _nested_struct(field.type))
        for field in msgspec.structs.fields(struct_type)
    }


def _nest(value: Any, nested: type[msgspec.Struct]) -> Any:
    if isinstance(value, Mapping):
        return to_struct(nested, value)
    if isinstance(value, msgspec.Struct) and not isinstance(value, nested):
        return to_struct(nested, msgspec.structs.asdict(value))
    if isinstance(value, list):
        return [_nest(item, nested) for item in value]
    return value


def _holds_struct(value: Any) -> bool:
    if isinstance(value, msgspec.Struct):
        return True
    return isinstance(value, list) and any(isinstance(item, msgspec.Struct) for item in value)


def _coerce(value: Any, nested: type[msgspec.Struct] | None) -> Any:
    if isinstance(value, _NETWORK_VALUES):
        return str(value)
    if nested is not None:
        return _nest(value, nested)
    return msgspec.to_builtins(value) if _holds_struct(value) else value


def to_struct(struct_type: type[StructT], values: Mapping[str, Any]) -> StructT:
    """Convert *values* into *struct_type*, applying the field annotations.

    Keys are internal (snake_case) field names; they are mapped to the
    struct's encoded names first so ``rename`` settings are honoured, at the
    top level and inside fields annotated with a struct type (``Related``,
    ``list[Related]``, ``Related | None``) whose values arrive as mappings.
    Values stored in a looser form (an enum's value, an ISO datetime string,
    a decimal string) come back as the annotated type; a network address
    the driver decoded from ``inet`` comes back as its text. Struct instances
    placed in a field whose annotation is not a struct (``list[dict[str,
    Any]]`` relations, ``Any``) are reduced to their builtin form so the
    annotation holds. A mapping annotation such as ``dict[str, Related]`` is
    left to ``msgspec`` as-is.

    Args:
        struct_type: Target struct class.
        values: Field values keyed by internal field name.

    Returns:
        A *struct_type* instance.

    Raises:
        msgspec.ValidationError: If a value cannot be converted to its
            annotated type.
    """
    plan = _field_plan(struct_type)
    encoded: dict[str, Any] = {}
    for key, value in values.items():
        encode_name, nested = plan.get(key, (key, None))
        encoded[encode_name] = _coerce(value, nested)
    return msgspec.convert(encoded, type=struct_type)
