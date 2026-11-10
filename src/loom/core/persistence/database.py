"""The single contract of a relational store section.

The API, the Celery worker and the streaming sinks read the same section shape
— at ``stores.<name>`` for a store whose backend is ``sqlalchemy``, or inline
in a sink — and build their engine through the same constructor. The pool
keys live under ``stores.<name>.pool``; each key left out takes the default
of the entry point's :class:`~loom.core.persistence.profile.RuntimeProfile`.

The section follows the connection standard of :mod:`loom.core.connections`:
``url``, ``connect_timeout`` and ``read_timeout`` in seconds, ``pool.size``
and ``pool.timeout``. This module imports no driver: streaming resolves its
configuration without the SQLAlchemy extra installed.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, ClassVar, Final, Literal, TypeVar

import msgspec

from loom.core.config import ConfigContext, ConfigError, reject_retired_keys
from loom.core.config.loader import convert_section
from loom.core.connections import ConnectionPool, ConnectionSettings, require_text
from loom.core.model.redaction import Dsn, RedactedMap, Secret
from loom.core.model.types import NonNegativeInt, PositiveInt
from loom.core.persistence.profile import PoolDefaults, RuntimeProfile
from loom.core.persistence.stores import SQLALCHEMY_BACKEND, load_catalog

EXTERNAL: Final = "external"
_DEFAULT_MAX_SUBJECTS: Final = 1000

_ASYNCPG: Final = "postgresql+asyncpg"
_ASYNCPG_TIMEOUTS: Final = {"connect_timeout": "timeout", "read_timeout": "command_timeout"}
"""The standard timeouts, mapped to the asyncpg ``connect()`` argument each one sets."""


class SchemaRoles(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``stores.<name>.schema.roles``: the roles owning and migrating the schema."""

    owner: str
    migrator: str


class SchemaUser(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """One entry of ``stores.<name>.schema.database_users``."""

    login: bool
    access: str


class SchemaGroups(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``stores.<name>.schema.groups``: the reader and writer group roles."""

    readers: str
    writers: str


class SchemaVersionTables(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``stores.<name>.schema.version_tables``: where structure and data migrations are recorded."""

    structure: str
    data: str


class AuditReadingRoute(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``stores.<name>.schema.audit.reading.route``: the generated read route.

    ``requires_roles`` is required and must name at least one role. ``prefix``
    is optional; loom.rest uses ``/audit/<store>`` when it is absent.

    Raises:
        ValueError: When ``requires_roles`` is empty. Surfaced as
            ``ConfigError`` by the decode.
    """

    requires_roles: tuple[str, ...]
    prefix: str | None = None

    def __post_init__(self) -> None:
        if not self.requires_roles or any(not role.strip() for role in self.requires_roles):
            raise ValueError("requires_roles must contain non-blank roles")
        if self.prefix is not None and not _is_route_prefix(self.prefix):
            raise ValueError("prefix must be an absolute path without trailing slash or whitespace")


_PREFIX_SEGMENT = re.compile(r"[^/\s?#]+")


def _is_route_prefix(prefix: str) -> bool:
    """Return whether ``prefix`` is ``/seg[/seg...]`` with no empty, blank or query segment."""
    segments = prefix.split("/")
    return (
        segments[0] == ""
        and len(segments) > 1
        and all(_PREFIX_SEGMENT.fullmatch(segment) for segment in segments[1:])
    )


class AuditReadingConfig(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``stores.<name>.schema.audit.reading``: opt-in reading of the audit log.

    Absent on :class:`AuditSchemaConfig` until the product writes it.
    ``max_subjects`` is at least 1. A ``route`` needs ``enabled``.

    Raises:
        ValueError: When ``route`` is set and ``enabled`` is false. Surfaced
            as ``ConfigError`` by the decode.
    """

    enabled: bool = False
    max_subjects: PositiveInt = _DEFAULT_MAX_SUBJECTS
    route: AuditReadingRoute | None = None

    def __post_init__(self) -> None:
        if self.route is not None and not self.enabled:
            raise ValueError("schema.audit.reading.route requires enabled")


class AuditSchemaConfig(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``stores.<name>.schema.audit``: the store's audit log.

    ``enabled: false`` removes the audit trigger from every marked table at the
    next deploy and keeps the history. ``retention_months`` is how long a
    monthly partition is kept; without it nothing is ever dropped. ``reader``
    names the role that reads the log, ``<schema>_auditor`` when absent.
    ``reading`` is absent until the product opts in to reading the log.
    """

    enabled: bool = True
    retention_months: PositiveInt | None = None
    reader: str | None = None
    reading: AuditReadingConfig | None = None


class SchemaConfig(msgspec.Struct, kw_only=True, frozen=True, forbid_unknown_fields=True):
    """``stores.<name>.schema``: the one definition every reader of the section shares.

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
    audit: AuditSchemaConfig = msgspec.field(default_factory=AuditSchemaConfig)


class PoolConfig(ConnectionPool, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``stores.<name>.pool``: connection-pool keys; ``None`` means "not declared".

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


class DatabaseConfig(ConnectionSettings, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """A relational database section.

    A key outside this contract is refused, at the top level as in ``pool``
    and ``schema``: a store section holds one database, so a misspelt key
    fails at startup instead of being ignored. The address
    hides its password, and ``connect_args`` hides its secret-named values.
    ``username`` and ``password``, when set, win over credentials in the URL.

    Args:
        url: SQLAlchemy async URL; never empty.
        username: Driver username. Wins over the one in ``url``.
        password: Driver password. Wins over the one in ``url`` and is never
            shown.
        echo: Log every statement.
        pool: Connection-pool keys; see :class:`PoolConfig`.
        connect_args: Passed to the driver's ``connect()`` as written. Secret
            names in it are hidden.
        schema: Startup schema handling; see :class:`SchemaConfig`.
        connect_timeout: Seconds to establish a connection; ``postgresql+asyncpg``
            URLs only (asyncpg ``timeout``).
        read_timeout: Seconds a statement may run; ``postgresql+asyncpg`` URLs
            only (asyncpg ``command_timeout``).

    Raises:
        ValueError: When the URL is empty, a timeout is set for a URL that is
            not ``postgresql+asyncpg``, ``connect_args`` sets the asyncpg
            argument a timeout decides, ``schema.audit.reading.route`` is set
            while reading is disabled, or ``schema.audit.reading.enabled`` is
            set on a store that is not external Postgres with ``schema.name``
            and ``schema.guard``. Surfaced as ``ConfigError``, naming the
            section, through :func:`decode_database_config`.
    """

    _RETIRED_KEYS: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            "pool_pre_ping": "pool.pre_ping",
            "pool_size": "pool.size",
            "max_overflow": "pool.max_overflow",
            "pool_timeout": "pool.timeout",
            "pool_recycle": "pool.recycle",
        }
    )

    url: Dsn
    username: str | None = None
    password: Secret[str] | None = None
    echo: bool = False
    pool: PoolConfig = msgspec.field(default_factory=PoolConfig)
    connect_args: RedactedMap = msgspec.field(default_factory=RedactedMap.empty)
    schema: SchemaConfig = msgspec.field(default_factory=SchemaConfig)
    connect_timeout: PositiveInt | None = None
    read_timeout: PositiveInt | None = None

    def __post_init__(self) -> None:
        require_text(self.url, "url")
        # Before the timeout return: a store that declares no timeouts is still refused.
        self._require_audit_reading()
        timeouts = self._asyncpg_timeouts()
        if not timeouts:
            return
        if self.url.scheme.lower() != _ASYNCPG:
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
        Entries are returned as stored. The session manager reveals them.
        """
        options = {str(key): value for key, value in self.connect_args.items()}
        options.update(self._asyncpg_timeouts())
        return options

    def _require_audit_reading(self) -> None:
        """Refuse ``schema.audit.reading.enabled`` unless this store can host the reader."""
        reading = self.schema.audit.reading
        if reading is None or not reading.enabled:
            return
        dialect = self.url.scheme.split("+", 1)[0].lower()
        if (
            self.schema.mode == EXTERNAL
            and self.schema.name
            and self.schema.guard
            and dialect == "postgresql"
        ):
            return
        raise ValueError(
            "schema.audit.reading requires an external Postgres store "
            "with schema.name and schema.guard"
        )

    def _asyncpg_timeouts(self) -> dict[str, int]:
        """Return the declared standard timeouts under their asyncpg argument names."""
        return {
            argument: value
            for field, argument in _ASYNCPG_TIMEOUTS.items()
            if (value := getattr(self, field)) is not None
        }


def decode_database_config(data: Mapping[str, Any], *, path: str) -> DatabaseConfig:
    """Decode a resolved relational store mapping read from *path*.

    Args:
        data: The section as plain data, interpolations already resolved.
        path: Dotted path the section was read from; error messages name keys under it.

    Returns:
        The validated configuration.

    Raises:
        ConfigError: When a retired flat pool key is present (naming its
            ``pool`` replacement), a key is unknown, a value is out of range,
            the URL is empty, a timeout does not apply to the URL's driver,
            ``reading.route`` is set while reading is disabled, or
            ``reading.enabled`` is set on a store that is not external
            Postgres with ``schema.name`` and ``schema.guard``.
    """
    reject_retired_keys(data, path, DatabaseConfig)
    return convert_section(dict(data), path, DatabaseConfig)


def load_database_config(ctx: ConfigContext, store: str) -> DatabaseConfig:
    """Read and decode the relational section of the store *store* of *ctx*.

    For a caller outside a composition; a composition decodes the store of
    its own catalog with :meth:`~loom.core.persistence.stores.StoreSpec.decode`.

    Args:
        ctx: The application configuration.
        store: Key of the store, declared under ``stores.<store>``.

    Raises:
        ConfigError: When the catalog does not load, the store is not
            declared, its backend is not ``sqlalchemy``, or its section
            fails :func:`decode_database_config`.
    """
    spec = load_catalog(ctx).require(
        store, backend=SQLALCHEMY_BACKEND, purpose="a relational database"
    )
    return spec.decode(DatabaseConfig)


_T = TypeVar("_T")


def require_schema_key(value: _T | None, path: str, key: str) -> _T:
    """Return *value*, the ``schema.<key>`` of the store at *path*, which a row-scoped model needs.

    Raises:
        ConfigError: When it is not declared, naming the key and how to write it.
    """
    if value is None:
        raise ConfigError(
            f"{path}.schema.{key} is required when a model is RowScoped: "
            "run `loom schema init <schema>` to write the derived names"
        )
    return value


__all__ = [
    "EXTERNAL",
    "AuditReadingConfig",
    "AuditReadingRoute",
    "DatabaseConfig",
    "PoolConfig",
    "SchemaConfig",
    "SchemaGroups",
    "SchemaRoles",
    "SchemaUser",
    "SchemaVersionTables",
    "decode_database_config",
    "load_database_config",
    "require_schema_key",
]
