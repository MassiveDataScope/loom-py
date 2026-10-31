"""Startup errors for the keys a section, or the whole configuration, no longer takes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from loom.core.config.errors import ConfigError
from loom.core.connections import ConnectionSettings

_RETIRED_PERSISTENCE: Final = {
    "backend": "set stores.default.backend",
    "mongo": "move it to stores.default with backend: mongo",
    "dynamodb": "move it to stores.default with backend: dynamodb",
}
"""Keys of ``persistence`` that moved to ``stores``; ``persistence.cursors`` stays."""

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


def reject_retired_sections(document: object) -> None:
    """Fail when the configuration *document* declares persistence outside ``stores``.

    ``database``, ``database.<name>``, ``persistence.backend``,
    ``persistence.mongo`` and ``persistence.dynamodb`` were replaced by the
    ``stores.<name>`` sections (ADR 0008). Reading them next to ``stores``
    would mix two contracts, so every load refuses them and names the
    replacement. ``persistence.cursors`` stays.

    Args:
        document: The whole configuration as written; anything but a mapping
            is left alone.

    Raises:
        ConfigError: Naming every retired key found and its ``stores.``
            replacement.
    """
    if not isinstance(document, Mapping):
        return
    problems = [*_retired_database(document.get("database")), *_retired_persistence(document)]
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


def _retired_persistence(document: Mapping[object, object]) -> list[str]:
    """The retired keys of the ``persistence`` section of *document*."""
    persistence = document.get("persistence")
    if not isinstance(persistence, Mapping):
        return []
    return [
        f"persistence.{key} was removed; {remedy}"
        for key, remedy in _RETIRED_PERSISTENCE.items()
        if key in persistence
    ]


__all__ = ["reject_retired_keys", "reject_retired_sections"]
