"""Startup errors for the keys a connection's sections no longer take."""

from __future__ import annotations

from collections.abc import Mapping

from loom.core.config.errors import ConfigError
from loom.core.connections import ConnectionSettings


def reject_retired_keys(data: object, path: str, connection: type[ConnectionSettings]) -> None:
    """Fail when *data*, read from *path*, holds a key *connection* no longer takes.

    msgspec would refuse the key as unknown; this names the replacement
    instead, at startup.

    Args:
        data: The section as written; anything but a mapping is left to the decoder.
        path: Dotted path the section was read from.
        connection: The connection type the section decodes into.

    Raises:
        ConfigError: Naming the retired key and its ``<path>.<key>`` replacement,
            with the connection's hint for that key when it has one.
    """
    if not isinstance(data, Mapping):
        return
    hints = connection.retired_hints()
    for retired, replacement in connection.retired_keys().items():
        if retired in data:
            hint = f" ({hints[retired]})" if retired in hints else ""
            raise ConfigError(f"{path}.{retired} was removed; use {path}.{replacement}{hint}")


__all__ = ["reject_retired_keys"]
