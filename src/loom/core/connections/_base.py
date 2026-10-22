"""What every engine's connection settings share: a redacted repr and guarded extras.

Each engine declares one connection type here, in :mod:`loom.core.connections`,
and every context using that engine declares the same type or a subclass
adding only its own keys. This module imports no driver.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping
from typing import Any, ClassVar

from loom.core.model import LoomFrozenStruct

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

    ``forbid_unknown_fields`` is inherited; a subclass repeats
    ``frozen=True, kw_only=True``: msgspec applies ``kw_only`` per class only,
    and type checkers refuse a subclass of a frozen class not declared frozen.

    The repr masks the fields named in ``_SECRET_FIELDS`` and the credentials
    of the URIs named in ``_URI_FIELDS``, so a connection is safe in a log
    line, a traceback or a container's repr.
    """

    _SECRET_FIELDS: ClassVar[frozenset[str]] = frozenset()
    _URI_FIELDS: ClassVar[frozenset[str]] = frozenset()

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
        built: Options the builder derived from *connection*.
        extra: The context's options.
        connection: The connection the options were derived from.
        reserved: Driver option names the builder sets from *connection*
            under a name of the driver's own (``maxPoolSize``, ``config``...).

    Returns:
        *built* updated with *extra*.

    Raises:
        ValueError: When *extra* names a field of *connection* or a reserved
            option, naming each.
    """
    taken = {*connection.__struct_fields__, *reserved}
    clashes = sorted(name for name in extra if name in taken)
    if clashes:
        raise ValueError(
            f"{type(connection).__name__} already decides {', '.join(clashes)}; "
            "set it on the connection instead"
        )
    return {**built, **extra}


__all__ = ["ConnectionSettings", "merge_extra", "redact_uri", "require_text"]
