"""The single contract of a relational ``database`` section.

The API, the Celery worker and the streaming sinks read the same section shape
— at ``database``, at ``database.<name>`` for a shared streaming database, or
inline in a sink — and build their engine through the same constructor. The
pool keys live under ``database.pool``; each key left out takes the default of
the entry point's :class:`~loom.core.persistence.profile.RuntimeProfile`.

The section follows the connection standard of :mod:`loom.core.connections`:
``url``, ``connect_timeout`` and ``read_timeout`` in seconds, ``pool.size``
and ``pool.timeout``. This module imports no driver: streaming resolves its
configuration without the SQLAlchemy extra installed.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, ClassVar, Final, Literal

import msgspec

from loom.core.config import ConfigContext, ConfigError, ConfigKey, reject_retired_keys
from loom.core.connections import ConnectionPool, ConnectionSettings, require_text
from loom.core.model.types import NonNegativeInt, PositiveInt
from loom.core.persistence.profile import PoolDefaults, RuntimeProfile

EXTERNAL: Final = "external"

_ASYNCPG: Final = "postgresql+asyncpg"
_ASYNCPG_TIMEOUTS: Final = {"connect_timeout": "timeout", "read_timeout": "command_timeout"}
"""The standard timeouts, mapped to the asyncpg ``connect()`` argument each one sets."""


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


class PoolConfig(ConnectionPool, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``database.pool``: connection-pool keys; ``None`` means "not declared".

    Args:
        size: Connections kept open.
        max_overflow: Extra connections allowed under load.
        timeout: Seconds a caller waits for a free connection.
        recycle: Seconds after which a connection is replaced.
        pre_ping: Whether a connection is checked before it is handed out.
    """

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


class DatabaseConfig(ConnectionSettings, frozen=True, kw_only=True, forbid_unknown_fields=False):
    """A relational database section.

    Keys outside this contract are tolerated at the top level: the section
    also holds the named databases of streaming sinks (``database.<name>``).
    ``pool`` and ``schema``, each with a single definition, refuse them. The
    repr hides the credentials of the URL and the whole ``connect_args``,
    which may carry a password or a TLS key.

    Args:
        url: SQLAlchemy async URL; never empty.
        echo: Log every statement.
        pool: Connection-pool keys; see :class:`PoolConfig`.
        connect_args: Passed to the driver's ``connect()`` as written.
        schema: Startup schema handling; see :class:`SchemaConfig`.
        connect_timeout: Seconds to establish a connection; ``postgresql+asyncpg``
            URLs only (asyncpg ``timeout``).
        read_timeout: Seconds a statement may run; ``postgresql+asyncpg`` URLs
            only (asyncpg ``command_timeout``).

    Raises:
        ValueError: When the URL is empty, a timeout is set for a URL that is
            not ``postgresql+asyncpg``, or ``connect_args`` sets the asyncpg
            argument a timeout decides. Surfaced as ``ConfigError``, naming
            the section, through :func:`decode_database_config`.
    """

    _SECRET_FIELDS: ClassVar[frozenset[str]] = ConnectionSettings._SECRET_FIELDS | {"connect_args"}
    _RETIRED_KEYS: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            "pool_pre_ping": "pool.pre_ping",
            "pool_size": "pool.size",
            "max_overflow": "pool.max_overflow",
            "pool_timeout": "pool.timeout",
            "pool_recycle": "pool.recycle",
        }
    )

    url: str
    echo: bool = False
    pool: PoolConfig = msgspec.field(default_factory=PoolConfig)
    connect_args: dict[str, Any] = msgspec.field(default_factory=dict)
    schema: SchemaConfig = msgspec.field(default_factory=SchemaConfig)
    connect_timeout: PositiveInt | None = None
    read_timeout: PositiveInt | None = None

    def __post_init__(self) -> None:
        require_text(self.url, "url")
        timeouts = self._asyncpg_timeouts()
        if not timeouts:
            return
        if self.url.split("://", 1)[0].lower() != _ASYNCPG:
            declared = ", ".join(
                field for field in _ASYNCPG_TIMEOUTS if getattr(self, field) is not None
            )
            raise ValueError(
                f"{declared} applies to {_ASYNCPG} URLs only; "
                "set the driver's own timeout in connect_args"
            )
        clashes = sorted(set(timeouts) & set(self.connect_args))
        if clashes:
            raise ValueError(
                f"connect_args sets {', '.join(clashes)}, which connect_timeout and "
                "read_timeout decide; set each timeout in one place"
            )

    def client_options(self) -> dict[str, Any]:
        """Return the driver's ``connect()`` arguments: ``connect_args`` plus the standard timeouts.

        Only asyncpg takes the timeouts, as ``timeout`` and ``command_timeout``;
        any other driver keeps its own option names, set in ``connect_args``.
        """
        return {**self.connect_args, **self._asyncpg_timeouts()}

    def _asyncpg_timeouts(self) -> dict[str, int]:
        """Return the declared standard timeouts under their asyncpg argument names."""
        return {
            argument: value
            for field, argument in _ASYNCPG_TIMEOUTS.items()
            if (value := getattr(self, field)) is not None
        }


def decode_database_config(data: Mapping[str, Any], *, path: str) -> DatabaseConfig:
    """Decode a resolved ``database`` mapping read from *path*.

    Args:
        data: The section as plain data, interpolations already resolved.
        path: Dotted path the section was read from; error messages name keys under it.

    Returns:
        The validated configuration.

    Raises:
        ConfigError: When a retired flat pool key is present (naming its
            ``pool`` replacement), a key is unknown, a value is out of range,
            the URL is empty or a timeout does not apply to the URL's driver.
    """
    reject_retired_pool_keys(data, path)
    try:
        config = msgspec.convert(dict(data), DatabaseConfig, strict=False)
    except msgspec.ValidationError as exc:
        raise ConfigError(f"Config section {path!r} failed validation: {exc}") from exc
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
    reject_retired_keys(data, path, DatabaseConfig)


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
