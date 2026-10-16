"""Backend-neutral cursor tokens for keyset pagination.

A token is the base64url form of a msgspec-encoded record
``{"b": backend, "k": [sort key values...], "id": tie-breaker}``, where the
tie-breaker is the primary-key value, or the list of its values when the key
is composite.  Every
backend issues and consumes the same shape, so a token from another backend
or in a legacy format is rejected at decode time with
:class:`~loom.core.repository.abc.errors.UnsupportedQuery`.
"""

from __future__ import annotations

import base64
from collections.abc import Sequence
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID

import msgspec

from loom.core.model.struct import LoomFrozenStruct
from loom.core.repository.abc.errors import UnsupportedQuery


class Cursor(LoomFrozenStruct, frozen=True):
    """Decoded cursor position.

    Args:
        backend: Name of the backend that issued the token.
        keys: Sort key values of the last row on the previous page, in sort order.
        tie_breaker: Primary key of that row; a tuple in key order when the
            key is composite.
    """

    backend: str
    keys: tuple[object, ...]
    tie_breaker: object


class _DateTimeKey(msgspec.Struct, tag="datetime"):
    v: datetime


class _DateKey(msgspec.Struct, tag="date"):
    v: date


class _UuidKey(msgspec.Struct, tag="uuid"):
    v: UUID


class _DecimalKey(msgspec.Struct, tag="decimal"):
    v: Decimal


_Key = int | float | str | bool | None | _DateTimeKey | _DateKey | _UuidKey | _DecimalKey


class _Token(msgspec.Struct):
    b: str
    k: list[_Key]
    id: _Key | list[_Key]


def _wrap(value: object) -> _Key:
    if isinstance(value, Enum):
        value = value.value
    if value is None or isinstance(value, int | float | str):
        return value
    if isinstance(value, datetime):
        return _DateTimeKey(value)
    if isinstance(value, date):
        return _DateKey(value)
    if isinstance(value, UUID):
        return _UuidKey(value)
    if isinstance(value, Decimal):
        return _DecimalKey(value)
    raise TypeError(f"Unsupported cursor key type: {type(value).__name__}")


def _unwrap(key: _Key) -> object:
    if isinstance(key, _DateTimeKey | _DateKey | _UuidKey | _DecimalKey):
        return key.v
    return key


def encode_cursor(backend: str, keys: Sequence[object], tie_breaker: object) -> str:
    """Encode a cursor position into an opaque URL-safe token.

    Args:
        backend: Name of the issuing backend.
        keys: Sort key values of the last row on the page, in sort order.
        tie_breaker: Primary key of that row; a tuple in key order when the
            key is composite.

    Returns:
        base64url token that :func:`decode_cursor` accepts for ``backend``.

    Raises:
        TypeError: If a key has a type the token format cannot carry.
    """
    tie: _Key | list[_Key] = (
        [_wrap(value) for value in tie_breaker]
        if isinstance(tie_breaker, tuple)
        else _wrap(tie_breaker)
    )
    token = _Token(b=backend, k=[_wrap(key) for key in keys], id=tie)
    return base64.urlsafe_b64encode(msgspec.json.encode(token)).decode()


def decode_cursor(
    token: str, backend: str, model: str, *, key_count: int | None = None, id_count: int = 1
) -> Cursor:
    """Decode a token issued by :func:`encode_cursor` for ``backend``.

    Args:
        token: Opaque token supplied by the client.
        backend: Name of the backend decoding the token.
        model: Qualified model name, used in the error.
        key_count: Number of sort keys the query orders by; when given, a
            token carrying a different number is rejected.
        id_count: Number of primary-key columns. A single-column key is
            carried as a scalar and a composite key as a list of that many
            values; any other token is rejected.

    Returns:
        The decoded :class:`Cursor`.

    Raises:
        UnsupportedQuery: If the token is undecodable, in a legacy format,
            was issued by another backend or does not match the sort or the
            primary key.
    """
    try:
        record = msgspec.json.decode(base64.urlsafe_b64decode(token.encode()), type=_Token)
    except (ValueError, msgspec.DecodeError) as exc:
        raise UnsupportedQuery(backend, model, "cursor token is not valid") from exc
    if record.b != backend:
        raise UnsupportedQuery(backend, model, "cursor token was issued by another backend")
    if key_count is not None and len(record.k) != key_count:
        raise UnsupportedQuery(backend, model, "cursor token does not match the sort")
    carried = len(record.id) if isinstance(record.id, list) else None
    if carried != (id_count if id_count > 1 else None):
        raise UnsupportedQuery(backend, model, "cursor token does not match the primary key")
    return Cursor(
        backend=record.b,
        keys=tuple(_unwrap(key) for key in record.k),
        tie_breaker=(
            tuple(_unwrap(key) for key in record.id)
            if isinstance(record.id, list)
            else _unwrap(record.id)
        ),
    )
