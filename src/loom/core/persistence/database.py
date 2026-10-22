"""The single contract of a relational ``database`` section.

The API, the Celery worker and the streaming sinks read the same section shape
— at ``database``, at ``database.<name>`` for a shared streaming database, or
inline in a sink — and build their engine through the same constructor. The
pool keys live under ``database.pool``; each key left out takes the default of
the entry point's :class:`~loom.core.persistence.profile.RuntimeProfile`.

This module imports no driver: streaming resolves its configuration without
the SQLAlchemy extra installed.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final, Literal

import msgspec

from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.model.types import NonNegativeInt, PositiveInt
from loom.core.persistence.profile import PoolDefaults, RuntimeProfile

EXTERNAL: Final = "external"

_RETIRED_KEYS: Final = {
    "pool_pre_ping": "pre_ping",
    "pool_size": "size",
    "max_overflow": "max_overflow",
    "pool_timeout": "timeout",
    "pool_recycle": "recycle",
}
"""Flat pool keys a ``database`` section once took, mapped to their ``pool`` key."""


class SchemaRoles(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``database.schema.roles``: the roles owning and migrating the schema."""

    owner: str
    migrator: str


class SchemaUser(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """One entry of ``database.schema.database_users``."""

    login: bool
    access: str


class SchemaGroups(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``database.schema.groups``: the reader and writer group roles."""

    readers: str
    writers: str


class SchemaVersionTables(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``database.schema.version_tables``: where structure and data migrations are recorded."""

    structure: str
    data: str


class SchemaConfig(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``database.schema``: the one definition every reader of the section shares.

    Startup (``mode``, ``allow_unprotected_dialect``, ``scopes``, ``guard``),
    the migrations and the row-level-security tooling read it; every name is
    absent until the product writes it (``loom schema init``). An unknown key
    fails, naming it.

    ``naming_convention`` is handed to the application's ``MetaData`` as
    SQLAlchemy's ``naming_convention``, keyed by ``pk``, ``fk``, ``uq``, ``ck``
    and ``ix``; absent, SQLAlchemy's default applies.
    """

    mode: Literal["create_all", "external"] = "create_all"
    allow_unprotected_dialect: bool = False
    name: str | None = None
    roles: SchemaRoles | None = None
    database_users: dict[str, SchemaUser] | None = None
    scopes: dict[str, str] | None = None
    guard: str | None = None
    groups: SchemaGroups | None = None
    version_tables: SchemaVersionTables | None = None
    naming_convention: dict[str, str] | None = None


class PoolConfig(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``database.pool``: connection-pool keys; ``None`` means "not declared".

    Args:
        size: Connections kept open.
        max_overflow: Extra connections allowed under load.
        timeout: Seconds a caller waits for a free connection.
        recycle: Seconds after which a connection is replaced.
        pre_ping: Whether a connection is checked before it is handed out.
    """

    size: PositiveInt | None = None
    max_overflow: NonNegativeInt | None = None
    timeout: PositiveInt | None = None
    recycle: PositiveInt | None = None
    pre_ping: bool | None = None

    def resolve(self, profile: RuntimeProfile) -> PoolDefaults:
        """Return the effective pool: each declared key wins, the rest come from *profile*."""
        defaults = profile.pool
        return PoolDefaults(
            size=defaults.size if self.size is None else self.size,
            max_overflow=defaults.max_overflow if self.max_overflow is None else self.max_overflow,
            timeout_s=defaults.timeout_s if self.timeout is None else self.timeout,
            recycle_s=defaults.recycle_s if self.recycle is None else self.recycle,
            pre_ping=defaults.pre_ping if self.pre_ping is None else self.pre_ping,
        )


class DatabaseConfig(msgspec.Struct, kw_only=True, frozen=True):
    """A relational database section.

    Keys outside this contract are tolerated at the top level: the section
    also holds the named databases of streaming sinks (``database.<name>``).
    ``pool`` and ``schema``, each with a single definition, refuse them.

    Args:
        url: SQLAlchemy async URL; never empty.
        echo: Log every statement.
        pool: Connection-pool keys; see :class:`PoolConfig`.
        connect_args: Passed to the driver's ``connect()`` as written.
        schema: Startup schema handling; see :class:`SchemaConfig`.
    """

    url: str
    echo: bool = False
    pool: PoolConfig = msgspec.field(default_factory=PoolConfig)
    connect_args: dict[str, Any] = msgspec.field(default_factory=dict)
    schema: SchemaConfig = msgspec.field(default_factory=SchemaConfig)


def decode_database_config(data: Mapping[str, Any], *, path: str) -> DatabaseConfig:
    """Decode a resolved ``database`` mapping read from *path*.

    Args:
        data: The section as plain data, interpolations already resolved.
        path: Dotted path the section was read from; error messages name keys under it.

    Returns:
        The validated configuration.

    Raises:
        ConfigError: When a retired flat pool key is present (naming its
            ``pool`` replacement), a key is unknown, a value is out of range or
            the URL is empty.
    """
    reject_retired_pool_keys(data, path)
    try:
        config = msgspec.convert(dict(data), DatabaseConfig, strict=False)
    except msgspec.ValidationError as exc:
        raise ConfigError(f"Config section {path!r} failed validation: {exc}") from exc
    if not config.url.strip():
        raise ConfigError(f"Config section {path!r}: 'url' must not be empty")
    return config


def load_database_config(
    ctx: ConfigContext, key: str | ConfigKey = ConfigKey.DATABASE
) -> DatabaseConfig:
    """Read and decode the ``database`` section at *key* of *ctx*.

    Raises:
        ConfigError: When the section is absent or fails :func:`decode_database_config`.
    """
    return decode_database_config(ctx.section(key, dict), path=str(key))


def reject_retired_pool_keys(data: Mapping[str, Any], path: str) -> None:
    """Fail when *data*, read from *path*, holds a flat pool key that moved under ``pool``.

    Raises:
        ConfigError: Naming the retired key and its ``<path>.pool.<key>`` replacement.
    """
    for retired, replacement in _RETIRED_KEYS.items():
        if retired in data:
            raise ConfigError(f"{path}.{retired} was removed; use {path}.pool.{replacement}")


__all__ = [
    "EXTERNAL",
    "DatabaseConfig",
    "PoolConfig",
    "SchemaConfig",
    "SchemaGroups",
    "SchemaRoles",
    "SchemaUser",
    "SchemaVersionTables",
    "decode_database_config",
    "load_database_config",
    "reject_retired_pool_keys",
]
