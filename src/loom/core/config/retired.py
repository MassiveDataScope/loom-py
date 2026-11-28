"""Startup errors for the keys a section, or the whole configuration, no longer takes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Final

from loom.core.config.errors import ConfigError
from loom.core.connections import ConnectionSettings

if TYPE_CHECKING:
    from loom.core.config.context import ConfigContext

_RETIRED_PERSISTENCE: Final = {
    "backend": "set stores.default.backend",
    "mongo": "move it to stores.default with backend: mongo",
    "dynamodb": "move it to stores.default with backend: dynamodb",
    "cursors": "move it to stores.settings.cursors",
    "rollback_timeout": "move it to stores.settings.rollback_timeout",
}
"""Keys of ``persistence`` that moved to ``stores``, each with where it went."""

_PERSISTENCE_REMEDY: Final = "the shared store settings live under stores.settings"

_OWN_DATABASE_MAPPINGS: Final = frozenset({"pool", "schema", "connect_args"})
"""Mapping keys of a relational ``database`` section, as opposed to a named database."""

_DATABASE_REMEDY: Final = "database was removed; move it to stores.default with backend: sqlalchemy"


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


def reject_retired_sections(ctx: ConfigContext) -> None:
    """Fail when the configuration *ctx* declares persistence outside ``stores``.

    ``database``, ``database.<name>`` and ``persistence`` were replaced by the
    ``stores.<name>`` sections and ``stores.settings``. Reading them next to
    ``stores`` would mix two contracts, so every load refuses them and names
    the replacement. The sections are read as written: no placeholder resolves.

    Args:
        ctx: The whole configuration.

    Raises:
        ConfigError: Naming every retired key found and its ``stores.``
            replacement.
    """
    problems = [
        *_retired_database(ctx.raw("database")),
        *_retired_persistence(ctx.raw("persistence")),
    ]
    if problems:
        raise ConfigError("; ".join(problems) + " (ADR 0008)")


def _retired_database(section: object) -> list[str]:
    """The retired ``database`` and ``database.<name>`` keys of *section*."""
    if section is None:
        return []
    if not isinstance(section, Mapping) or not section:
        return [_DATABASE_REMEDY]
    own = [key for key, value in section.items() if _is_own_database_key(key, value)]
    named = [
        f"database.{key} was removed; move it to stores.{key} with its backend"
        for key in section
        if key not in own
    ]
    return [_DATABASE_REMEDY, *named] if own else named


def _is_own_database_key(key: object, value: object) -> bool:
    """Whether *key* belongs to a relational section rather than naming a database."""
    return not isinstance(value, Mapping) or key in _OWN_DATABASE_MAPPINGS


def _retired_persistence(persistence: object) -> list[str]:
    """The retired ``persistence`` keys of *persistence*, each with its replacement."""
    if persistence is None:
        return []
    if not isinstance(persistence, Mapping) or not persistence:
        return [f"persistence was removed; {_PERSISTENCE_REMEDY}"]
    return [
        f"persistence.{key} was removed; {_RETIRED_PERSISTENCE.get(key, _PERSISTENCE_REMEDY)}"
        for key in persistence
    ]


__all__ = ["reject_retired_keys", "reject_retired_sections"]
