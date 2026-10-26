"""What every engine's connection settings share: the standard keys' home and their guards.

Each engine declares one connection type in its package of
:mod:`loom.core.connections`, and every context using that engine declares the
same type or a subclass adding only its own keys. This module imports no driver.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from types import MappingProxyType
from typing import Any, ClassVar

from loom.core.model import LoomFrozenStruct
from loom.core.model.redaction import Dsn, mask_dsn
from loom.core.model.types import PositiveInt

redact_uri = mask_dsn


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

    A password is a :class:`~loom.core.model.redaction.Secret` and an address
    is a :class:`~loom.core.model.redaction.Dsn`, so the struct's own repr
    hides them. ``_RETIRED_KEYS`` maps each key the engine's sections no
    longer take to the key path replacing it, and ``_RETIRED_HINTS`` adds a
    note to the error of a retired key whose replacement needs one (a change
    of unit), for :func:`loom.core.config.reject_retired_keys`.
    """

    _RETIRED_KEYS: ClassVar[Mapping[str, str]] = MappingProxyType({})
    _RETIRED_HINTS: ClassVar[Mapping[str, str]] = MappingProxyType({})

    @classmethod
    def retired_keys(cls) -> Mapping[str, str]:
        """Return each key this connection's sections no longer take, mapped to its replacement."""
        return cls._RETIRED_KEYS

    @classmethod
    def retired_hints(cls) -> Mapping[str, str]:
        """Return the note added to the error of each retired key that needs one."""
        return cls._RETIRED_HINTS


class ConnectionPool(LoomFrozenStruct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """The ``pool`` of a connection: what the client keeps open.

    Args:
        size: Connections the client keeps; ``None`` leaves the engine's default.
    """

    size: PositiveInt | None = None


def require_text(value: str | Dsn, field: str) -> None:
    """Refuse an empty or blank *value*; msgspec reports the ``ValueError`` with its path.

    An address is judged on its display form, which is the address itself
    when it holds no secret.

    Raises:
        ValueError: When *value* holds no text, naming *field*.
    """
    if not str(value).strip():
        raise ValueError(f"{field} must not be empty")


def omit_none(options: Mapping[str, Any]) -> dict[str, Any]:
    """Return *options* without the ones left ``None``, which keep the driver's default."""
    return {name: value for name, value in options.items() if value is not None}


def merge_extra(
    built: Mapping[str, Any],
    extra: Mapping[str, Any],
    connection: ConnectionSettings,
    *,
    reserved: Collection[str] = (),
    case_insensitive: bool = False,
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
        case_insensitive: Compare option names ignoring case, for a driver
            that reads them so (pymongo: ``maxpoolsize`` is ``maxPoolSize``).

    Returns:
        *built* updated with *extra*.

    Raises:
        ValueError: When *extra* names a field of *connection*, an option of
            *built* or a reserved option, naming each.
    """
    fold = str.lower if case_insensitive else str
    taken = {fold(name) for name in (*connection.__struct_fields__, *built, *reserved)}
    clashes = sorted(name for name in extra if fold(name) in taken)
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
    "omit_none",
    "redact_uri",
    "require_text",
]
