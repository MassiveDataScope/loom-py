"""What every engine's connection settings share: the standard keys' home and their guards.

Each engine declares one connection type in its package of
:mod:`loom.core.connections`, and every context using that engine declares the
same type or a subclass adding only its own keys. This module imports no driver.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping
from types import MappingProxyType
from typing import Any, ClassVar

from loom.core.model import LoomFrozenStruct
from loom.core.model.types import PositiveInt

# Everything up to the last "@" before the query: a raw "@" or "/" in a
# password belongs to the credentials. A path holding an "@" is masked too,
# which only hides more than needed.
_AUTHORITY_CREDENTIALS = re.compile(r"://[^?#]*@")
# Query parameters carrying a secret: their value is masked, their name kept.
_SECRET_QUERY_VALUE = re.compile(
    r"([?&](?:[^=&#]*(?:pass|pwd|secret|token|key)[^=&#]*|authMechanismProperties)=)[^&#]*",
    re.IGNORECASE,
)
_MASK = "***"


def redact_uri(uri: str) -> str:
    """Return *uri* with its credentials replaced by ``***``.

    The userinfo of the authority is masked whole, and so is the value of
    every query parameter whose name carries a secret (``password``,
    ``tlsCertificateKeyFilePassword``, ``token``, ``authMechanismProperties``...).

    Args:
        uri: A connection URI, with or without credentials.

    Returns:
        The URI, safe to log.
    """
    masked = _AUTHORITY_CREDENTIALS.sub(f"://{_MASK}@", uri)
    return _SECRET_QUERY_VALUE.sub(rf"\g<1>{_MASK}", masked)


class ConnectionSettings(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Base of every engine's connection settings.

    Wherever an engine has the concept, its type names it the same way and in
    the same unit: ``url``, ``username`` and ``password``, ``connect_timeout``
    and ``read_timeout`` in seconds, ``max_attempts`` counting the first one,
    and ``pool.size`` / ``pool.timeout`` under a :class:`ConnectionPool`. Each
    engine type translates them to its driver in ``client_options()``.

    ``forbid_unknown_fields`` is inherited; a subclass repeats
    ``frozen=True, kw_only=True``: msgspec applies ``kw_only`` per class only,
    and type checkers refuse a subclass of a frozen class not declared frozen.

    The repr masks the fields named in ``_SECRET_FIELDS`` and the credentials
    of the URIs named in ``_URI_FIELDS``, so a connection is safe in a log
    line, a traceback or a container's repr. ``_RETIRED_KEYS`` maps each key
    the engine's sections no longer take to its replacement, for
    :func:`reject_retired_keys`.
    """

    _SECRET_FIELDS: ClassVar[frozenset[str]] = frozenset({"password"})
    _URI_FIELDS: ClassVar[frozenset[str]] = frozenset({"url"})
    _RETIRED_KEYS: ClassVar[Mapping[str, str]] = MappingProxyType({})

    def __repr__(self) -> str:
        parts = []
        for name in self.__struct_fields__:
            value = getattr(self, name)
            if name in self._SECRET_FIELDS and value is not None:
                shown = repr(_MASK)
            elif name in self._URI_FIELDS and isinstance(value, str):
                shown = repr(redact_uri(value))
            else:
                shown = repr(value)
            parts.append(f"{name}={shown}")
        return f"{type(self).__name__}({', '.join(parts)})"

    @classmethod
    def retired_keys(cls) -> Mapping[str, str]:
        """Return each key this connection's sections no longer take, mapped to its replacement."""
        return cls._RETIRED_KEYS


class ConnectionPool(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The ``pool`` of a connection: what the client keeps open.

    Args:
        size: Connections the client keeps; ``None`` leaves the engine's default.
    """

    size: PositiveInt | None = None


def reject_retired_keys(data: object, path: str, connection: type[ConnectionSettings]) -> None:
    """Fail when *data*, read from *path*, holds a key *connection* no longer takes.

    msgspec would refuse the key as unknown; this names the replacement
    instead, at startup.

    Args:
        data: The section as written; anything but a mapping is left to the decoder.
        path: Dotted path the section was read from.
        connection: The connection type the section decodes into.

    Raises:
        ConfigError: Naming the retired key and its ``<path>.<key>`` replacement.
    """
    # Imported here, not at the top: loom.core.config's resolvers read
    # AwsClientConfig, so the config package imports this one first.
    from loom.core.config.errors import ConfigError

    if not isinstance(data, Mapping):
        return
    for retired, replacement in connection.retired_keys().items():
        if retired in data:
            raise ConfigError(f"{path}.{retired} was removed; use {path}.{replacement}")


def require_text(value: str, field: str) -> None:
    """Refuse an empty or blank *value*; msgspec reports the ``ValueError`` with its path.

    Raises:
        ValueError: When *value* holds no text, naming *field*.
    """
    if not value.strip():
        raise ValueError(f"{field} must not be empty")


def merge_extra(
    built: Mapping[str, Any],
    extra: Mapping[str, Any],
    connection: ConnectionSettings,
    *,
    reserved: Collection[str] = (),
) -> dict[str, Any]:
    """Merge a context's own client options into the options built from a connection.

    A context passes options its connection does not declare (``tz_aware`` for
    the Mongo repository, ``query_retries`` for the SQL service). It never
    replaces what the connection decides: the connection is the one place a
    timeout, a credential or an address is set.

    Args:
        built: Options the builder derived from *connection*: those of its
            ``client_options()`` and the driver objects the island made of them.
        extra: The context's options.
        connection: The connection the options were derived from.
        reserved: Further option names the island decides outside *built*
            (the boto3 session's ``region_name``...).

    Returns:
        *built* updated with *extra*.

    Raises:
        ValueError: When *extra* names a field of *connection*, an option of
            *built* or a reserved option, naming each.
    """
    taken = {*connection.__struct_fields__, *built, *reserved}
    clashes = sorted(name for name in extra if name in taken)
    if clashes:
        raise ValueError(
            f"{type(connection).__name__} already decides {', '.join(clashes)}; "
            "set it on the connection instead"
        )
    return {**built, **extra}


__all__ = [
    "ConnectionPool",
    "ConnectionSettings",
    "merge_extra",
    "redact_uri",
    "reject_retired_keys",
]
