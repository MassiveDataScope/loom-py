"""The one decoder for configuration.

Every config read passes through :func:`convert_config` and keeps the
``strict`` it has today. :func:`dump_config` masks secrets. Without the encode
hook a dump of a secret, an address or a redacted mapping fails closed.
"""

from __future__ import annotations

from typing import Any, TypeVar, get_args, get_origin

import msgspec

from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret, mask_structure

T = TypeVar("T")


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
            args = get_args(typ)
            inner = args[0] if args else str
            if isinstance(value, str) and not value.strip():
                raise ValueError("must not be empty")
            try:
                converted = msgspec.convert(value, type=inner, strict=self._strict, dec_hook=self)
            except msgspec.ValidationError as exc:
                raise TypeError(str(exc).split(" - at ", 1)[0]) from None
            return Secret(converted)
        if typ is Dsn:
            if isinstance(value, Dsn):
                return value
            if not isinstance(value, str):
                raise TypeError(f"Expected `str`, got `{type(value).__name__}`")
            return Dsn(value)
        if typ is RedactedMap:
            if isinstance(value, RedactedMap):
                return value
            if not isinstance(value, dict):
                raise TypeError(f"Expected `object`, got `{type(value).__name__}`")
            return RedactedMap(value)
        raise TypeError(
            f"Decoding objects of type {getattr(typ, '__name__', typ)!r} is unsupported"
        )


def convert_config(data: object, target: type[T], *, strict: bool) -> T:
    """Decode *data* as *target*.

    ``strict`` is the caller's: the loader stays lax and the sections that
    decode strictly stay strict. A secret, an address or a redacted mapping
    is built here. The error names the path and the expected type, never the
    value.

    Args:
        data: Resolved configuration, already a builtin.
        target: Struct or annotation to build.
        strict: Forwarded to ``msgspec``.

    Returns:
        The decoded value.

    Raises:
        msgspec.ValidationError: When *data* does not match *target*.
    """
    return msgspec.convert(data, type=target, strict=strict, dec_hook=_Decoder(strict))


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
