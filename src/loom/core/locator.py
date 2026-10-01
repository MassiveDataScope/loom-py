"""Locate one application from its configuration file.

``load_application`` is the single entry point the Alembic environment, the
command line and the schema tools share: it reads the file named by
``LOOM_CONFIG`` (or an explicit path), discovers the models the same way the
server does, compiles them into a ``MetaData`` owned by the returned
``Application`` and builds the bootstrap declaration from the ``database``
section. A missing key raises ``ConfigError`` naming it; nothing is defaulted.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeVar, get_args

import msgspec

from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.discovery.base import DiscoveryResult
from loom.core.discovery.interfaces import InterfacesDiscoveryEngine
from loom.core.discovery.manifest import ManifestDiscoveryEngine
from loom.core.discovery.modules import ModulesDiscoveryEngine
from loom.core.model.scoped import ScopedTable

if TYPE_CHECKING:
    from sqlalchemy import MetaData

    from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig, DatabaseUser

CONFIG_ENV_VAR = "LOOM_CONFIG"
SchemaMode = Literal["create_all", "external"]

_SCOPE_SOURCE = re.compile(r"^(identity|request)\.[A-Za-z_]\w*$")
_T = TypeVar("_T")


class _DiscoveryInterfaces(msgspec.Struct, kw_only=True):
    modules: list[str] = msgspec.field(default_factory=list)
    warn_recommended: bool = True


class _DiscoveryModules(msgspec.Struct, kw_only=True):
    include: list[str] = msgspec.field(default_factory=list)


class _DiscoveryManifest(msgspec.Struct, kw_only=True):
    module: str = ""


class _Discovery(msgspec.Struct, kw_only=True):
    mode: str = "interfaces"
    interfaces: _DiscoveryInterfaces = msgspec.field(default_factory=_DiscoveryInterfaces)
    modules: _DiscoveryModules = msgspec.field(default_factory=_DiscoveryModules)
    manifest: _DiscoveryManifest = msgspec.field(default_factory=_DiscoveryManifest)


class _AppSection(msgspec.Struct, kw_only=True):
    name: str
    code_path: str = "src"
    discovery: _Discovery = msgspec.field(default_factory=_Discovery)


class _RolesSection(msgspec.Struct, kw_only=True):
    owner: str
    migrator: str


class _UserSection(msgspec.Struct, kw_only=True):
    login: bool
    access: str


class _GroupsSection(msgspec.Struct, kw_only=True):
    readers: str
    writers: str


class _VersionTablesSection(msgspec.Struct, kw_only=True):
    structure: str
    data: str


class SchemaConfig(msgspec.Struct, kw_only=True):
    """The ``database.schema`` section; every name is absent until the product writes it."""

    mode: SchemaMode = "create_all"
    allow_unprotected_dialect: bool = False
    name: str | None = None
    roles: _RolesSection | None = None
    database_users: dict[str, _UserSection] | None = None
    scopes: dict[str, str] | None = None
    guard: str | None = None
    groups: _GroupsSection | None = None
    version_tables: _VersionTablesSection | None = None


class DatabaseConfig(msgspec.Struct, kw_only=True):
    """The ``database`` section as the locator reads it."""

    url: str
    schema: SchemaConfig = msgspec.field(default_factory=SchemaConfig)


@dataclass(frozen=True, slots=True)
class Application:
    """One application: its models compiled into their own metadata and its declarations."""

    models: tuple[type, ...]
    metadata: MetaData
    database: DatabaseConfig
    bootstrap: BootstrapConfig | None
    scoped: Mapping[tuple[str | None, str], ScopedTable]
    scope_sources: Mapping[str, str]


_ENGINES: dict[str, Callable[[_Discovery], DiscoveryResult]] = {
    "interfaces": lambda cfg: InterfacesDiscoveryEngine(
        cfg.interfaces.modules, warn_recommended=cfg.interfaces.warn_recommended
    ).discover(),
    "modules": lambda cfg: ModulesDiscoveryEngine(cfg.modules.include).discover(),
    "manifest": lambda cfg: ManifestDiscoveryEngine(cfg.manifest.module).discover(),
}


def load_application(config_path: str | None = None) -> Application:
    """Load, discover and compile the application described by ``config_path``.

    Raises:
        ConfigError: When the path is missing, a required key is absent or a
            value has the wrong shape; the message names the key.
    """
    from sqlalchemy import MetaData

    from loom.core.backend.scoped_ddl import SCHEMA_KEY
    from loom.core.backend.sqlalchemy import compile_all, scoped_tables

    path = _resolve_path(config_path)
    context = ConfigContext.from_yaml(path)
    app = context.section(ConfigKey.APP, _AppSection)
    database = context.section(ConfigKey.DATABASE, DatabaseConfig)
    _ensure_on_path(app.code_path, Path(path).resolve().parent)
    models = _discover(app.discovery).models
    metadata = MetaData()
    if database.schema.name is not None:
        metadata.info[SCHEMA_KEY] = _schema_name(database.schema.name)
    compile_all(*models, metadata=metadata)
    scoped = scoped_tables(metadata)
    bootstrap = _bootstrap(database.schema) if scoped else None
    scope_sources = _scope_sources(database.schema, scoped) if scoped else {}
    return Application(
        models=tuple(models),
        metadata=metadata,
        database=database,
        bootstrap=bootstrap,
        scoped=scoped,
        scope_sources=scope_sources,
    )


def _ensure_on_path(code_path: str, config_dir: Path) -> None:
    candidate = Path(code_path)
    if not candidate.is_absolute():
        candidate = config_dir / candidate
    resolved = str(candidate.resolve())
    if resolved not in sys.path:
        sys.path.insert(0, resolved)


def _resolve_path(config_path: str | None) -> str:
    if config_path:
        return config_path
    from_env = os.environ.get(CONFIG_ENV_VAR)
    if not from_env:
        raise ConfigError(f"{CONFIG_ENV_VAR} is not set and no config path was given")
    return from_env


def _discover(discovery: _Discovery) -> DiscoveryResult:
    engine = _ENGINES.get(discovery.mode)
    if engine is None:
        raise ConfigError(f"app.discovery.mode {discovery.mode!r} is not supported")
    return engine(discovery)


def _schema_name(name: str) -> str:
    from loom.core.backend.scoped_ddl import schema_identifier

    try:
        return schema_identifier(name)
    except ValueError as exc:
        raise ConfigError(f"database.schema.name: {exc}") from exc


def _bootstrap(schema: SchemaConfig) -> BootstrapConfig:
    from loom.core.repository.sqlalchemy.rls.config import (
        BootstrapConfig,
        DatabaseRoles,
        SchemaNames,
    )

    name = _required(schema.name, "name")
    roles = _required(schema.roles, "roles")
    users = _required(schema.database_users, "database_users")
    groups = _required(schema.groups, "groups")
    version_tables = _required(schema.version_tables, "version_tables")
    config = BootstrapConfig(
        schema=name,
        roles=DatabaseRoles(owner=roles.owner, migrator=roles.migrator),
        database_users={user: _database_user(user, section) for user, section in users.items()},
        names=SchemaNames(
            guard=_required(schema.guard, "guard"),
            readers=groups.readers,
            writers=groups.writers,
            version_table=version_tables.structure,
            data_version_table=version_tables.data,
        ),
    )
    try:
        return config.validated()
    except ValueError as exc:
        raise ConfigError(f"database.schema: {exc}") from exc


def _database_user(user: str, section: _UserSection) -> DatabaseUser:
    from loom.core.repository.sqlalchemy.rls.config import Access, DatabaseUser

    choices: tuple[Access, ...] = get_args(Access)
    access: Access | None = next((c for c in choices if c == section.access), None)
    if access is None:
        raise ConfigError(
            f"database.schema.database_users.{user}.access {section.access!r} "
            f"is not one of {', '.join(choices)}"
        )
    return DatabaseUser(login=section.login, access=access)


def _scope_sources(
    schema: SchemaConfig, scoped: Mapping[tuple[str | None, str], ScopedTable]
) -> dict[str, str]:
    sources = _required(schema.scopes, "scopes")
    declared = {scope.scope for table in scoped.values() for scope in table.scopes}
    for scope in sorted(declared):
        if scope not in sources:
            raise ConfigError(f"database.schema.scopes.{scope} is missing")
        if not _SCOPE_SOURCE.fullmatch(sources[scope]):
            raise ConfigError(
                f"database.schema.scopes.{scope} {sources[scope]!r} must be "
                "identity.<attribute> or request.<name>"
            )
    return {scope: sources[scope] for scope in sorted(declared)}


def _required(value: _T | None, key: str) -> _T:
    if value is None:
        raise ConfigError(
            f"database.schema.{key} is required when a model is RowScoped: "
            "run `loom schema init <schema>` to write the derived names"
        )
    return value
