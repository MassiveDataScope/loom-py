"""The one decoder for configuration.

Every config read passes through :func:`convert_config` and keeps the
``strict`` it has today. :func:`dump_config` masks secrets. Without the encode
hook a dump of a secret, an address or a redacted mapping fails closed.
"""

from __future__ import annotations

import types
from typing import Any, TypeVar, Union, cast, get_args, get_origin

import msgspec

from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret, mask_structure

T = TypeVar("T")
_UNION = (Union, types.UnionType)


class _Decoder:
    """Build a ``Secret``, ``Dsn`` or ``RedactedMap`` while decoding config."""

    __slots__ = ("_strict",)

    def __init__(self, strict: bool) -> None:
        self._strict = strict

    def __call__(self, typ: Any, value: Any) -> Any:
        if typ is object or typ is Any:
            return value
        origin = get_origin(typ) or typ
        if origin is Secret:
            return self._secret(typ, value)
        if typ is Dsn or typ is RedactedMap:
            return self._typed(value, typ)
        raise TypeError(
            f"Decoding objects of type {getattr(typ, '__name__', typ)!r} is unsupported"
        )

    def _secret(self, typ: Any, value: Any) -> Secret[Any]:
        """Decode the inner type and wrap it. A blank string is refused.

        The inner convert names a path of its own. Drop that path so the outer
        convert reports the field that failed, and so the message carries the
        expected type rather than the rejected value.
        """
        args = get_args(typ)
        inner = args[0] if args else str
        if isinstance(value, str) and not value.strip():
            raise ValueError("must not be empty")
        try:
            converted = msgspec.convert(value, type=inner, strict=self._strict, dec_hook=self)
        except msgspec.ValidationError as exc:
            raise TypeError(str(exc).split(" - at ", 1)[0]) from None
        return Secret(converted)

    def _typed(self, value: Any, target: type[Dsn] | type[RedactedMap]) -> Dsn | RedactedMap:
        """Build ``target`` from a raw value, or return one that already is.

        A string becomes a ``Dsn``. A dict becomes a ``RedactedMap``. msgspec
        calls that dict an ``object`` in the error.
        """
        if isinstance(value, target):
            return cast("Dsn | RedactedMap", value)
        if target is Dsn:
            accepted: type[Any] = str
            expected = "str"
        else:
            accepted = dict
            expected = "object"
        if not isinstance(value, accepted):
            raise TypeError(f"Expected `{expected}`, got `{type(value).__name__}`")
        return target(value)


def convert_config(data: object, target: type[T], *, strict: bool) -> T:
    """Decode *data* as *target*.

    ``strict`` is the caller's: the loader stays lax and the sections that
    decode strictly stay strict. A secret, an address or a redacted mapping
    is built here. A blank optional address or secret becomes ``None``; a
    blank required one is an error. The error names the path and the expected
    type, never the value.

    Args:
        data: Resolved configuration, already a builtin.
        target: Struct or annotation to build.
        strict: Forwarded to ``msgspec``.

    Returns:
        The decoded value.

    Raises:
        msgspec.ValidationError: When *data* does not match *target*.
    """
    return msgspec.convert(
        _blank_optional(data, target),
        type=target,
        strict=strict,
        dec_hook=_Decoder(strict),
    )


def _blank_optional(data: object, target: Any) -> object:
    """Turn a blank optional address or secret into ``None``.

    A required address still reaches the decoder, which refuses it. Each shape
    copies its container only when a nested value changes.
    """
    if _is_blank_optional(target, data):
        return None
    origin = get_origin(target)
    if origin in _UNION:
        return _blank_union(data, target)
    if origin in (list, tuple):
        return _blank_sequence(data, target)
    if origin is dict:
        return _blank_mapping(data, target)
    if isinstance(data, dict):
        return _blank_struct(data, target)
    return data


def _blank_union(data: object, target: Any) -> object:
    member = next((arg for arg in get_args(target) if arg is not type(None)), None)
    if member is None:
        return data
    return _blank_optional(data, member)


def _blank_sequence(data: object, target: Any) -> object:
    args = get_args(target)
    if not isinstance(data, list) or not args:
        return data
    element = args[0]
    items = [_blank_optional(item, element) for item in data]
    if all(new is old for new, old in zip(items, data, strict=True)):
        return data
    return items


def _blank_mapping(data: object, target: Any) -> object:
    args = get_args(target)
    if not isinstance(data, dict) or len(args) != 2:
        return data
    values = {key: _blank_optional(item, args[1]) for key, item in data.items()}
    if all(values[key] is data[key] for key in data):
        return data
    return values


def _blank_struct(data: dict[str, Any], target: object) -> object:
    if not isinstance(target, type) or not issubclass(target, msgspec.Struct):
        return data
    copied = dict(data)
    changed = False
    for field in msgspec.structs.fields(target):
        if field.name not in copied:
            continue
        updated = _blank_optional(copied[field.name], field.type)
        if updated is copied[field.name]:
            continue
        copied[field.name] = updated
        changed = True
    if changed:
        return copied
    return data


def _is_blank_optional(target: Any, data: object) -> bool:
    if not isinstance(data, str) or data.strip():
        return False
    if get_origin(target) not in _UNION:
        return False
    args = get_args(target)
    if type(None) not in args:
        return False
    return any(_is_address_or_secret(arg) for arg in args)


def _is_address_or_secret(annotation: Any) -> bool:
    origin = get_origin(annotation) or annotation
    return origin is Secret or annotation is Dsn


def dump_config(value: object) -> Any:
    """Return builtins for *value*, with secrets and addresses masked.

    Raises:
        TypeError: When *value* holds a type this encoder does not know.
    """
    return msgspec.to_builtins(value, enc_hook=_encode)


def _encode(value: Any) -> Any:
    if isinstance(value, Secret):
        return MASK
    if isinstance(value, Dsn):
        return str(value)
    if isinstance(value, RedactedMap):
        return mask_structure(dict(value))
    raise TypeError(f"Encoding objects of type {type(value).__name__} is unsupported")


__all__ = ["convert_config", "dump_config"]
