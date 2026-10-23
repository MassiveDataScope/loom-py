"""Values that stay out of logs, reprs and dumps until a driver is built.

:class:`Secret`, :class:`Dsn` and :class:`RedactedMap` are not strings. ``repr``,
``str`` and ``format`` hide the secret; the driver reads it with :meth:`reveal`.
A mapping or a sequence revealed that way is a copy.
"""

from __future__ import annotations

import copy
import re
import types
from collections.abc import Iterator, Mapping, Sequence
from functools import cache
from typing import Any, Generic, TypeVar, Union, get_args, get_origin

import msgspec

T = TypeVar("T")

MASK = "***"
_UNION = (Union, types.UnionType)
_SECRET_TOKENS = frozenset({"password", "passwd", "pwd", "secret", "token", "apikey", "credential"})
_EXACT_NAMES = frozenset({"webhook_url", "authorization", "cookie", "authmechanismproperties"})
_KEY_EXCEPTIONS = frozenset({"primary_key", "foreign_key", "partition_key", "sort_key"})
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


class Secret(Generic[T]):
    """A credential. Display is always ``***``; the value comes from :meth:`reveal`.

    Args:
        value: The credential. A blank string is refused.

    Raises:
        ValueError: When *value* is a blank string. The message does not include it.
    """

    __slots__ = ("_value",)

    def __init__(self, value: T) -> None:
        if isinstance(value, str) and not value.strip():
            raise ValueError("must not be empty")
        self._value = value

    def reveal(self) -> T:
        """Return the credential. A mapping or a sequence is a copy."""
        value = self._value
        if isinstance(value, (dict, list)):
            return copy.deepcopy(value)
        return value

    def __repr__(self) -> str:
        return MASK

    def __str__(self) -> str:
        return MASK

    def __format__(self, format_spec: str) -> str:
        return MASK

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Secret):
            return bool(self._value == other._value)
        return bool(self._value == other)

    def __hash__(self) -> int:
        return hash(self._value)


class Dsn:
    """A connection or service address.

    ``repr``, ``str`` and ``format`` keep the scheme, user, host, port, path
    and ordinary query parameters, and replace the password and secret query
    values with ``***``. An address with no secret is shown whole. The text
    the driver dials comes from :meth:`reveal`.

    Args:
        value: The address as written.

    Raises:
        TypeError: When *value* is not text.
        ValueError: When the address carries an empty password. The message
            does not include the address.
    """

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        if not isinstance(value, str):
            raise TypeError("Expected `str`")
        _reject_empty_password(value)
        self._value = value

    def reveal(self) -> str:
        """Return the address as written, password included."""
        return self._value

    @property
    def scheme(self) -> str:
        """The text before ``://``, or the whole address when it has no scheme."""
        return self._value.split("://", 1)[0]

    def query_names(self) -> frozenset[str]:
        """Names of the query parameters, in any order. Values are not returned."""
        _body, query, _fragment = _split_address(self._value)
        return frozenset(name for name, _mark, _item in _query_items(query))

    def has_userinfo(self) -> bool:
        """Whether the authority carries a user, with or without a password."""
        body, _query, _fragment = _split_address(self._value)
        _scheme, separator, rest = body.partition("://")
        return bool(separator) and _userinfo_at(rest) != -1

    def __repr__(self) -> str:
        return mask_dsn(self._value)

    def __str__(self) -> str:
        return mask_dsn(self._value)

    def __format__(self, format_spec: str) -> str:
        return mask_dsn(self._value)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Dsn):
            return self._value == other._value
        if isinstance(other, str):
            return self._value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._value)


class RedactedMap(Mapping[str, Any]):
    """A free mapping whose secret-named values are hidden in display.

    Item access returns what was stored. :meth:`reveal` returns a copy for the
    driver. Nested mappings and sequences follow the same name rule.

    Args:
        value: The mapping as written.

    Raises:
        TypeError: When *value* is not a mapping.
        ValueError: When a secret-named entry is a blank string.
    """

    __slots__ = ("_value",)

    def __init__(self, value: Mapping[str, Any]) -> None:
        if not isinstance(value, Mapping):
            raise TypeError("Expected `object`")
        copied = copy.deepcopy(dict(value))
        _reject_blank_secret_entries(copied, "")
        self._value = copied

    @classmethod
    def empty(cls) -> RedactedMap:
        """A mapping with no entries, for a struct field's default."""
        return cls({})

    def reveal(self) -> dict[str, Any]:
        """Return a copy of the mapping, secrets included."""
        return copy.deepcopy(self._value)

    def identity(self) -> tuple[tuple[str, object], ...]:
        """Hashable identity. Its ``repr`` hides secret values."""
        return _identity_mapping(self._value)

    def __getitem__(self, key: str) -> Any:
        return self._value[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._value)

    def __len__(self) -> int:
        return len(self._value)

    def __repr__(self) -> str:
        return repr(mask_structure(self._value))

    def __str__(self) -> str:
        return str(mask_structure(self._value))

    def __format__(self, format_spec: str) -> str:
        return str(mask_structure(self._value))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RedactedMap):
            return self._value == other._value
        if isinstance(other, Mapping):
            return self._value == other
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.identity())

    def __deepcopy__(self, memo: dict[int, Any]) -> RedactedMap:
        return RedactedMap(copy.deepcopy(self._value, memo))


def reveal_for_driver(value: object) -> Any:
    """Return *value* as the driver receives it.

    Args:
        value: A client option or a mapping of them.

    Returns:
        The revealed value. A mapping is walked; anything else is returned
        as stored.
    """
    if isinstance(value, Mapping) and not isinstance(value, RedactedMap):
        return {key: reveal_for_driver(item) for key, item in value.items()}
    if isinstance(value, (Secret, Dsn, RedactedMap)):
        return value.reveal()
    return value


def is_secret_name(name: str) -> bool:
    """Whether *name* is a credential, by token rather than by substring.

    The name is split on ``_``, ``.``, ``-`` and camel-case boundaries, then
    compared in lower case. It is a secret when a token is ``password``,
    ``passwd``, ``pwd``, ``secret``, ``token``, ``apikey`` or ``credential``,
    when the last token is ``key``, or when the whole name is ``webhook_url``,
    ``authorization``, ``cookie`` or ``authmechanismproperties``. ``keyboard``
    and ``passive`` are not secrets. ``primary_key``, ``foreign_key``,
    ``partition_key`` and ``sort_key`` stay visible; ``api_key`` and
    ``signing_key`` do not.

    Args:
        name: A field name, a query parameter or a mapping key.
    """
    stripped = name.strip()
    if not stripped:
        return False
    lowered = stripped.lower()
    compact = re.sub(r"[^a-z0-9]", "", lowered)
    if lowered in _EXACT_NAMES or compact in _EXACT_NAMES:
        return True
    tokens = _tokens(stripped)
    if "_".join(tokens) in _KEY_EXCEPTIONS:
        return False
    if any(token in _SECRET_TOKENS for token in tokens):
        return True
    return bool(tokens) and tokens[-1] == "key"


def mask_dsn(value: str) -> str:
    """Return *value* with its password and secret query values replaced by ``***``.

    The user, host, port, path and ordinary parameters stay. An ``@`` in the
    path does not hide the host. An address with no secret is returned as written.
    """
    body, query, fragment = _split_address(value)
    return _mask_authority(body) + _mask_query(query) + fragment


def _split_address(value: str) -> tuple[str, str, str]:
    """Return the authority, the query including ``?``, and the fragment including ``#``."""
    hash_at = value.find("#")
    if hash_at == -1:
        body, fragment = value, ""
    else:
        body, fragment = value[:hash_at], value[hash_at:]
    query_at = body.find("?")
    if query_at == -1:
        return body, "", fragment
    return body[:query_at], body[query_at:], fragment


def mask_structure(value: Any) -> Any:
    """Return *value* with secret-named entries replaced by ``***``, at every level."""
    if isinstance(value, Mapping):
        return {str(key): _mask_entry(str(key), item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        masked = [mask_structure(item) for item in value]
        return tuple(masked) if isinstance(value, tuple) else masked
    if isinstance(value, Secret):
        return MASK
    if isinstance(value, Dsn):
        return str(value)
    if isinstance(value, RedactedMap):
        return mask_structure(dict(value))
    return value


def ensure_redacted(struct: msgspec.Struct) -> None:
    """Refuse a secret, address or free mapping stored as plain text.

    Called from struct construction. A :class:`Secret`, :class:`Dsn` or
    :class:`RedactedMap` field that still holds the raw decoded value is a
    leak, because a frozen struct cannot wrap it afterwards.

    Raises:
        TypeError: Naming the field and the type it must hold, never the value.
        ValueError: When an address field is blank, naming the field.
    """
    for name, kind in _redacted_fields(type(struct)):
        value = getattr(struct, name)
        if value is None:
            continue
        if not isinstance(value, kind):
            raise TypeError(f"{name} must be {kind.__name__}")
        if kind is Dsn and not str(value).strip():
            raise ValueError(f"{name} must not be empty")


@cache
def _redacted_fields(
    struct_type: type[msgspec.Struct],
) -> tuple[tuple[str, type[Any]], ...]:
    found: list[tuple[str, type[Any]]] = []
    for field in msgspec.structs.fields(struct_type):
        kind = _redaction_kind(field.type)
        if kind is not None:
            found.append((field.name, kind))
    return tuple(found)


def _redaction_kind(annotation: Any) -> type[Any] | None:
    for option in _union_members(annotation):
        origin = get_origin(option) or option
        if origin is Secret:
            return Secret
        if option is Dsn:
            return Dsn
        if option is RedactedMap:
            return RedactedMap
    return None


def _union_members(annotation: Any) -> tuple[Any, ...]:
    if get_origin(annotation) in _UNION:
        return tuple(arg for arg in get_args(annotation) if arg is not type(None))
    return (annotation,)


def _tokens(name: str) -> list[str]:
    split = _CAMEL_BOUNDARY.sub(" ", name).lower()
    return [token for token in re.split(r"[^a-z0-9]+", split) if token]


def _userinfo_at(rest: str) -> int:
    """Index of the ``@`` ending userinfo, or -1.

    A slash before that ``@`` is part of the password. An ``@`` that only
    appears after the path has started is left in the path, so the host stays.
    """
    slash = rest.find("/")
    before_path = slash if slash != -1 else len(rest)
    at = rest.rfind("@", 0, before_path)
    if at != -1:
        return at
    if slash == -1:
        return -1
    at = rest.find("@", slash)
    if at == -1 or ":" not in rest[:at]:
        return -1
    host = rest[at + 1 :]
    if not host or host.startswith("/"):
        return -1
    return at


def _reject_empty_password(value: str) -> None:
    body, query, _fragment = _split_address(value)
    _scheme, separator, rest = body.partition("://")
    if separator:
        at = _userinfo_at(rest)
        if at != -1:
            userinfo = rest[:at]
            if ":" in userinfo and not userinfo.partition(":")[2]:
                raise ValueError("password must not be empty")
    for name, mark, item in _query_items(query):
        if mark and is_secret_name(name) and not item.strip():
            raise ValueError("password must not be empty")


def _query_items(query: str) -> list[tuple[str, str, str]]:
    if not query:
        return []
    body = query[1:] if query.startswith("?") else query
    items: list[tuple[str, str, str]] = []
    for piece in body.split("&"):
        if not piece:
            continue
        name, mark, item = piece.partition("=")
        items.append((name, mark, item))
    return items


def _mask_authority(body: str) -> str:
    scheme, separator, rest = body.partition("://")
    if not separator:
        return body
    at = _userinfo_at(rest)
    if at == -1:
        return body
    userinfo = rest[:at]
    colon = userinfo.find(":")
    if colon == -1:
        return body
    return f"{scheme}://{userinfo[:colon]}:{MASK}@{rest[at + 1 :]}"


def _mask_query(query: str) -> str:
    if not query:
        return ""
    prefix, body = ("?", query[1:]) if query.startswith("?") else ("", query)
    parts: list[str] = []
    for piece in body.split("&"):
        if not piece:
            parts.append(piece)
            continue
        name, separator, _item = piece.partition("=")
        if separator and is_secret_name(name):
            parts.append(f"{name}={MASK}")
        else:
            parts.append(piece)
    return prefix + "&".join(parts)


def _mask_entry(key: str, value: Any) -> Any:
    if value is None:
        return None
    if is_secret_name(key):
        return MASK
    return mask_structure(value)


def _reject_blank_secret_entries(value: object, prefix: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            name = str(key)
            path = f"{prefix}.{name}" if prefix else name
            if is_secret_name(name) and isinstance(item, str) and not item.strip():
                raise ValueError(f"{path} must not be empty")
            _reject_blank_secret_entries(item, path)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _reject_blank_secret_entries(item, prefix)


def _identity_mapping(value: Mapping[str, Any]) -> tuple[tuple[str, object], ...]:
    return tuple(sorted((str(key), _identity_value(str(key), item)) for key, item in value.items()))


def _identity_value(key: str, value: object) -> object:
    if is_secret_name(key):
        return Secret(_freeze(value))
    return _freeze(value)


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return _identity_mapping(value)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, Secret):
        return Secret(_freeze(value.reveal()))
    if isinstance(value, Dsn):
        return value
    if isinstance(value, RedactedMap):
        return value.identity()
    return value


__all__ = [
    "MASK",
    "Dsn",
    "RedactedMap",
    "Secret",
    "ensure_redacted",
    "is_secret_name",
    "mask_dsn",
    "mask_structure",
    "reveal_for_driver",
]
