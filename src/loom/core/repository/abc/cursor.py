"""Signed, strictly decoded cursor tokens for keyset pagination.

A token carries a position — the sort key values of the last row on a page
and its primary key — and is issued and verified by one :class:`CursorCodec`.

Format (``signed`` and ``ephemeral`` modes)::

    base64url(payload) "." base64url(version || len(key_id) || key_id || mac)

``payload`` is the msgspec JSON record ``{"b": backend, "k": [keys...], "id":
tie_breaker}``; the tie-breaker is a list when the primary key is composite.
``mac`` is HMAC-SHA256 over a domain-separated input: a fixed context string,
the version byte, the key id, the binding and the payload, each variable part
length-prefixed. The binding is never carried in the token: it is recomputed
from the request (model, normalised filters, sort and the caller's scope), so
a cursor replayed under another binding fails verification. The signature is
checked in constant time before the payload is parsed.

The ``unsigned`` mode emits ``base64url(payload)`` alone, for tests and
debugging. Strict decoding applies in every mode: a malformed token, a naive
datetime, a key of an unknown type, a token of another backend or for another
sort or primary key, or holding a value of another type than its column, is
refused with :class:`~loom.core.repository.abc.errors.UnsupportedQuery`,
whose message never echoes the token. See ADR 0004.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import logging
import re
import secrets
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Final, Literal
from uuid import UUID

import msgspec

from loom.core.config.errors import ConfigError
from loom.core.config.keys import ConfigKey
from loom.core.environment import configured_variable
from loom.core.model.struct import LoomFrozenStruct
from loom.core.repository.abc.errors import UnsupportedQuery
from loom.core.repository.abc.query import FilterSpec, QuerySpec

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

_logger = logging.getLogger(__name__)

_Signing = Literal["signed", "ephemeral", "unsigned"]
_SIGNED: Final = "signed"
_EPHEMERAL: Final = "ephemeral"
_UNSIGNED: Final = "unsigned"
_SIGNING_MODES: Final = frozenset({_SIGNED, _EPHEMERAL, _UNSIGNED})

MIN_KEY_BYTES: Final = 32
"""Shortest accepted HMAC-SHA256 key: the hash output length (NIST SP 800-107)."""

_CONTEXT: Final = b"loom.cursor\x00"
_VERSION: Final = 1
_MAC_BYTES: Final = hashlib.sha256().digest_size
_EPHEMERAL_KEY_ID: Final = b"ephemeral"
_KEY_ID: Final = re.compile(r"[A-Za-z0-9._-]{1,64}")
_BASE64URL: Final = re.compile(r"[A-Za-z0-9_-]*")
_INVALID: Final = "cursor token is not valid"
_OPT_OUT: Final = "persistence.cursors.signing: ephemeral in development.yaml"
# Types a decoded value must already have: msgspec would otherwise parse a
# string into them, and the keyset predicate would bind that string.
_CARRIED_TYPES: Final = (datetime, date, time, UUID, Decimal)


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


class CursorKeySource(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Where one cursor signing key is read from; exactly one source is set.

    The key itself never appears in configuration, so a dump of the config
    cannot leak it. The key is the UTF-8 text found there with surrounding
    whitespace removed, at least :data:`MIN_KEY_BYTES` bytes long.

    Args:
        path: File holding the key (a mounted secret).
        env: Environment variable holding the key (how ECS/Fargate inject secrets).

    Raises:
        ValueError: When neither or both sources are set.
    """

    path: str | None = None
    env: str | None = None

    def __post_init__(self) -> None:
        if (self.path is None) == (self.env is None):
            raise ValueError("a cursor key needs exactly one source: 'path' or 'env'")


class CursorConfig(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The ``persistence.cursors`` section.

    Args:
        signing: ``signed`` (the default when absent) signs with ``keys``;
            ``ephemeral`` signs with a random per-process key; ``unsigned``
            signs nothing. Both opt-outs are set explicitly per environment
            file (``development.yaml``, ``test.yaml``) and warn at startup.
        current_key_id: Key that signs new cursors; it must be in ``keys``.
        keys: Every key that verifies cursors, by key id. Older ids stay
            here during a rotation so cursors they signed keep verifying.

    Example YAML::

        persistence:
          cursors:
            current_key_id: "2026-10"
            keys:
              "2026-10": {env: CURSOR_KEY_2026_10}
              "2026-07": {path: /run/secrets/cursor-2026-07}
    """

    signing: _Signing = "signed"
    current_key_id: str | None = None
    keys: dict[str, CursorKeySource] = {}


class _DateTimeKey(msgspec.Struct, tag="datetime", forbid_unknown_fields=True):
    v: Annotated[datetime, msgspec.Meta(tz=True)]


class _DateKey(msgspec.Struct, tag="date", forbid_unknown_fields=True):
    v: date


class _TimeKey(msgspec.Struct, tag="time", forbid_unknown_fields=True):
    v: time


class _UuidKey(msgspec.Struct, tag="uuid", forbid_unknown_fields=True):
    v: UUID


class _DecimalKey(msgspec.Struct, tag="decimal", forbid_unknown_fields=True):
    v: Decimal


_Key = int | float | str | bool | None | _DateTimeKey | _DateKey | _TimeKey | _UuidKey | _DecimalKey


class _Token(msgspec.Struct, forbid_unknown_fields=True):
    b: str
    k: list[_Key]
    id: _Key | list[_Key]


class CursorCodec:
    """Issues and verifies cursor tokens bound to the query that produced them.

    Build it once at startup, from configuration with :meth:`from_config`,
    and share it: the persistence backends register it in DI, so a service
    paginating by hand resolves the same codec the repositories use.

    Args:
        signing: ``signed`` (``keys`` and ``current_key_id`` required),
            ``ephemeral`` or ``unsigned``. The two opt-outs ignore ``keys``
            and log a WARNING when built.
        keys: Signing keys by key id, each at least :data:`MIN_KEY_BYTES` long.
        current_key_id: Key id that signs new tokens.

    Raises:
        ConfigError: When the mode is unknown, or a ``signed`` codec has no
            key, a short key, a key id outside ``[A-Za-z0-9._-]{1,64}`` or a
            ``current_key_id`` it does not hold.

    Example::

        codec = CursorCodec.from_context(ctx)  # or container.resolve(CursorCodec)
        token = codec.encode(position, model="billing.Invoice", query=query, scope=tenant_id)
        position = codec.decode(
            token,
            backend="sqlalchemy",
            model="billing.Invoice",
            query=query,
            scope=tenant_id,
            columns=scalar_column_annotations(Invoice),
            primary_key=("id",),
        )
    """

    __slots__ = ("_current", "_keys", "_signing")

    def __init__(
        self,
        *,
        signing: _Signing,
        keys: Mapping[str, bytes] | None = None,
        current_key_id: str | None = None,
    ) -> None:
        if signing not in _SIGNING_MODES:
            raise ConfigError(f"persistence.cursors.signing {signing!r} is not a signing mode")
        self._signing = signing
        if signing == _SIGNED:
            self._keys, self._current = _keyring(keys or {}, current_key_id)
        elif signing == _EPHEMERAL:
            self._current = _EPHEMERAL_KEY_ID
            self._keys = {_EPHEMERAL_KEY_ID: secrets.token_bytes(MIN_KEY_BYTES)}
            _logger.warning(
                "cursor signing is ephemeral: a random key generated for this process signs "
                "cursors, so they do not survive a restart and other instances refuse them. "
                "Configure persistence.cursors keys outside development."
            )
        else:
            self._current = b""
            self._keys = {}
            _logger.warning(
                "cursor signing is unsigned: cursors carry no signature, so any client can "
                "forge one. Use it only for tests and debugging, never in production."
            )

    @classmethod
    def from_context(cls, ctx: ConfigContext) -> CursorCodec:
        """Build the codec of the ``persistence.cursors`` section of ``ctx``.

        Args:
            ctx: Application configuration; the section may be absent.

        Returns:
            The codec, built as :meth:`from_config` builds it.

        Raises:
            ConfigError: As :meth:`from_config`, or when the section is invalid.
        """
        return cls.from_config(
            ctx.section_or_default(ConfigKey.PERSISTENCE_CURSORS, CursorConfig, CursorConfig())
        )

    @classmethod
    def from_config(cls, config: CursorConfig) -> CursorCodec:
        """Build the codec of a ``persistence.cursors`` section, reading its keys.

        Args:
            config: The section, or ``CursorConfig()`` when it is absent.

        Returns:
            The codec for the resolved signing mode.

        Raises:
            ConfigError: When a key cannot be read, is empty or too short, or
                the keyring is invalid; messages never carry key material.
        """
        if config.signing != _SIGNED:
            return cls(signing=config.signing)
        keys = {key_id: _read_key(key_id, spec) for key_id, spec in config.keys.items()}
        return cls(signing=_SIGNED, keys=keys, current_key_id=config.current_key_id)

    def encode(self, cursor: Cursor, *, model: str, query: QuerySpec, scope: str) -> str:
        """Issue the token for ``cursor``, bound to ``model``, ``query`` and ``scope``.

        Args:
            cursor: Position of the last row on the page. A naive datetime
                key is taken as UTC and carried with an explicit offset.
            model: Qualified name of the model being paginated.
            query: Query the page answers; its filters and sort are bound.
            scope: Caller scope the cursor is valid in, such as the tenant id;
                ``""`` binds the cursor to no scope, deliberately.

        Returns:
            URL-safe token.

        Raises:
            TypeError: If a key has a type the token format cannot carry.
        """
        tie_breaker = cursor.tie_breaker
        tie: _Key | list[_Key] = (
            [_wrap(value) for value in tie_breaker]
            if isinstance(tie_breaker, tuple)
            else _wrap(tie_breaker)
        )
        token = _Token(b=cursor.backend, k=[_wrap(key) for key in cursor.keys], id=tie)
        payload = msgspec.json.encode(token)
        if self._signing == _UNSIGNED:
            return _b64encode(payload)
        key_id = self._current
        mac = _mac(self._keys[key_id], key_id, _binding(model, query, scope), payload)
        header = bytes((_VERSION, len(key_id))) + key_id
        return f"{_b64encode(payload)}.{_b64encode(header + mac)}"

    def decode(
        self,
        token: str,
        *,
        backend: str,
        model: str,
        query: QuerySpec,
        scope: str,
        columns: Mapping[str, Any],
        primary_key: Sequence[str],
    ) -> Cursor:
        """Verify and decode a client-supplied ``token``.

        Args:
            token: Token as received from the client.
            backend: Name of the backend serving the query.
            model: Qualified model name; bound and used in the error.
            query: Query being served; its filters and sort must match the
                ones the token was issued for, and it must sort by as many
                keys as the token carries.
            scope: Caller scope; must equal the scope the token was issued in.
            columns: Annotation of each column a token may carry a value of,
                by name; every sort field and primary-key column must be here.
            primary_key: Primary-key column names in key order. A
                single-column key is carried as a scalar, a composite key as
                a list of one value per column.

        Returns:
            The decoded position; datetime keys are always aware.

        Raises:
            UnsupportedQuery: If the token is malformed, unsigned or forged,
                signed by an unknown key, issued for another binding or
                backend, or does not match the sort or the primary key in
                number or type of values.
        """
        payload = self._verified_payload(token, model, query, scope)
        if payload is None:
            raise UnsupportedQuery(backend, model, _INVALID)
        try:
            record = msgspec.json.decode(payload, type=_Token)
        except msgspec.DecodeError:
            # The decoder's message may quote the payload: never chain it.
            raise UnsupportedQuery(backend, model, _INVALID) from None
        sort_types = tuple(columns.get(spec.field) for spec in query.sort)
        id_types = tuple(columns.get(name) for name in primary_key)
        return _position(record, backend, model, sort_types=sort_types, id_types=id_types)

    def _verified_payload(
        self, token: str, model: str, query: QuerySpec, scope: str
    ) -> bytes | None:
        """The payload bytes once the signature checks out, or ``None``."""
        if self._signing == _UNSIGNED:
            return None if "." in token else _b64decode(token)
        body, separator, signature = token.partition(".")
        payload = _b64decode(body)
        tag = _b64decode(signature) if separator else None
        if payload is None or tag is None or len(tag) < 2 or tag[0] != _VERSION:
            return None
        id_end = 2 + tag[1]
        key = self._keys.get(tag[2:id_end])
        if key is None or len(tag) != id_end + _MAC_BYTES:
            return None
        expected = _mac(key, tag[2:id_end], _binding(model, query, scope), payload)
        return payload if hmac.compare_digest(expected, tag[id_end:]) else None


def _keyring(
    keys: Mapping[str, bytes], current_key_id: str | None
) -> tuple[dict[bytes, bytes], bytes]:
    if not keys:
        raise ConfigError(
            "cursor signing needs a key: set persistence.cursors.current_key_id and "
            f"persistence.cursors.keys, or opt out for development with {_OPT_OUT}"
        )
    for key_id, key in keys.items():
        if _KEY_ID.fullmatch(key_id) is None:
            raise ConfigError(f"cursor key id {key_id!r} must match [A-Za-z0-9._-]{{1,64}}")
        if len(key) < MIN_KEY_BYTES:
            raise ConfigError(f"cursor key {key_id!r} is shorter than {MIN_KEY_BYTES} bytes")
    if current_key_id is None or current_key_id not in keys:
        raise ConfigError("persistence.cursors.current_key_id must name a configured key")
    return {key_id.encode(): key for key_id, key in keys.items()}, current_key_id.encode()


def _read_key(key_id: str, spec: CursorKeySource) -> bytes:
    """Read one key; errors name the key id and never the material or the path."""
    if spec.env is not None:
        text = configured_variable(spec.env)
        if text is None:
            raise ConfigError(f"cursor key {key_id!r}: environment variable {spec.env} is not set")
    else:
        try:
            text = Path(str(spec.path)).read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            raise ConfigError(f"cursor key {key_id!r}: its file could not be read") from None
    material = text.strip().encode()
    if not material:
        raise ConfigError(f"cursor key {key_id!r} is empty")
    return material


def _mac(key: bytes, key_id: bytes, binding: bytes, payload: bytes) -> bytes:
    message = b"".join(
        (
            _CONTEXT,
            bytes((_VERSION, len(key_id))),
            key_id,
            len(binding).to_bytes(4, "big"),
            binding,
            payload,
        )
    )
    return hmac.new(key, message, hashlib.sha256).digest()


def _binding(model: str, query: QuerySpec, scope: str) -> bytes:
    """Canonical bytes of what a token is bound to; page size and page are not part of it."""
    group = query.filters
    filters = (
        None
        if group is None
        else [group.op, [msgspec.Raw(item) for item in sorted(map(_filter_bytes, group.filters))]]
    )
    sort = [[spec.field, spec.direction] for spec in query.sort]
    return msgspec.json.encode([model, scope, filters, sort])


def _filter_bytes(spec: FilterSpec) -> bytes:
    return msgspec.json.encode([spec.field, spec.op, _utc(spec.value)], order="deterministic")


def _utc(value: object) -> object:
    """``value`` with every datetime as aware UTC; a naive one is taken as UTC."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    if isinstance(value, list | tuple):
        return [_utc(item) for item in value]
    if isinstance(value, set | frozenset):
        return frozenset(_utc(item) for item in value)
    return value


def _wrap(value: object) -> _Key:
    if isinstance(value, Enum):
        value = value.value
    if value is None or isinstance(value, int | float | str):
        return value
    if isinstance(value, datetime):
        return _DateTimeKey(value.replace(tzinfo=UTC) if value.tzinfo is None else value)
    if isinstance(value, date):
        return _DateKey(value)
    if isinstance(value, time):
        return _TimeKey(value)
    if isinstance(value, UUID):
        return _UuidKey(value)
    if isinstance(value, Decimal):
        return _DecimalKey(value)
    raise TypeError(f"Unsupported cursor key type: {type(value).__name__}")


def _unwrap(key: _Key) -> object:
    if isinstance(key, _DateTimeKey | _DateKey | _TimeKey | _UuidKey | _DecimalKey):
        return key.v
    return key


def _position(
    record: _Token,
    backend: str,
    model: str,
    *,
    sort_types: tuple[Any, ...],
    id_types: tuple[Any, ...],
) -> Cursor:
    """The position ``record`` holds, once it matches the sort and the primary key.

    A type of ``None`` stands for a column missing from ``columns``, which no
    value matches.
    """
    if record.b != backend:
        raise UnsupportedQuery(backend, model, "cursor token was issued by another backend")
    keys = tuple(_unwrap(key) for key in record.k)
    if not _typed_as(keys, sort_types):
        raise UnsupportedQuery(backend, model, "cursor token does not match the sort")
    composite = len(id_types) > 1
    carried = record.id
    ties = (
        tuple(_unwrap(key) for key in carried) if isinstance(carried, list) else (_unwrap(carried),)
    )
    if isinstance(carried, list) != composite or not _typed_as(ties, id_types):
        raise UnsupportedQuery(backend, model, "cursor token does not match the primary key")
    return Cursor(backend=record.b, keys=keys, tie_breaker=ties if composite else ties[0])


def _typed_as(values: tuple[object, ...], types: tuple[Any, ...]) -> bool:
    """Whether ``values`` has one value of each of ``types``, in order.

    msgspec checks each value strictly: an ``int`` is not a ``str``, a
    ``bool`` is not an ``int``, ``None`` only matches an optional type, and a
    string is never parsed into a datetime, date, time, UUID or decimal.
    """
    if len(values) != len(types) or None in types:
        return False
    try:
        msgspec.convert(values, tuple[types], builtin_types=_CARRIED_TYPES)  # type: ignore[valid-type]
    except msgspec.ValidationError:
        return False
    return True


def _b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes | None:
    """Strict unpadded base64url (RFC 4648 §5), or ``None``."""
    if _BASE64URL.fullmatch(text) is None or len(text) % 4 == 1:
        return None
    try:
        return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))
    except (binascii.Error, ValueError):
        return None


__all__ = ["MIN_KEY_BYTES", "Cursor", "CursorCodec", "CursorConfig", "CursorKeySource"]
