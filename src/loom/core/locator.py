"""Locate one application from its configuration file.

``load_application`` is the single entry point the Alembic environment, the
command line and the schema tools share: it reads the file named by
``LOOM_CONFIG`` (or an explicit path), discovers the models the same way the
server does, compiles them into a ``MetaData`` owned by the returned
``Application`` and builds the bootstrap declaration from one store's
section, ``stores.default`` unless another is named. Only that store's models
are compiled. A missing key raises ``ConfigError`` naming it; nothing is defaulted.
"""

from __future__ import annotations

import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, get_args

import msgspec

from loom.core.config import ConfigError, ConfigKey
from loom.core.config.overrides import load_application_config
from loom.core.discovery.config import DISCOVERY_MODES, decode_discovery, discover
from loom.core.environment import (
    CONFIG_VARIABLE,
    config_source_paths,
    loom_variable,
    resolve_environment,
)
from loom.core.logger import get_logger
from loom.core.model.scoped import ScopedTables, declared_scopes
from loom.core.persistence.database import (
    DatabaseConfig,
    SchemaConfig,
    SchemaUser,
    require_schema_key,
)
from loom.core.persistence.stores import (
    DEFAULT_STORE,
    SQLALCHEMY_BACKEND,
    StoreKey,
    load_catalog,
)
from loom.core.schema_names import SchemaNames, schema_identifier

if TYPE_CHECKING:
    from sqlalchemy import MetaData

    from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig, DatabaseUser


_log = get_logger(__name__)

_SCOPE_SOURCE = re.compile(r"^(identity|request)\.[A-Za-z_]\w*$")


DEFAULT_CODE_PATH: Final = "src"
"""``app.code_path`` when the configuration names none."""


class _AppSection(msgspec.Struct, kw_only=True):
    name: str
    code_path: str = DEFAULT_CODE_PATH
    # Decoded by decode_discovery, which ignores the keys it does not declare.
    discovery: dict[str, Any] = msgspec.field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Application:
    """One application: its models compiled into their own metadata and its declarations.

    Attributes:
        store: The store whose models and declarations these are.
    """

    models: tuple[type, ...]
    metadata: MetaData
    database: DatabaseConfig
    bootstrap: BootstrapConfig | None
    scoped: ScopedTables
    scope_sources: Mapping[str, str]
    store: StoreKey = DEFAULT_STORE

    @property
    def path(self) -> str:
        """Dotted path of the store section the declarations were read from, which messages name."""
        return f"{ConfigKey.STORES}.{self.store}"

    def require_bootstrap(self, purpose: str) -> BootstrapConfig:
        """Return the bootstrap declaration, which *purpose* (``"migrations"``, ...) needs.

        Raises:
            ConfigError: When the store declares no schema name, roles or
                database users, naming its section.
        """
        if self.bootstrap is None:
            raise ConfigError(
                f"{purpose} need {self.path}.schema.name, roles and database_users "
                "in the configuration"
            )
        return self.bootstrap


def load_application(
    config_path: str | None = None, *, store: StoreKey = DEFAULT_STORE
) -> Application:
    """Load, discover and compile the models of one store of ``config_path``'s application.

    ``config_path``, else ``LOOM_CONFIG``, is a file or a config directory:
    a directory loads ``base.yaml`` then the overlay of the environment
    ``LOOM_ENV`` selects, as the application factories do, and the
    ``LOOM__*`` variables merge last. The models of other stores are left
    out, and a log line names those stores.

    Raises:
        ConfigError: When the path is missing, a required key is absent or a
            value has the wrong shape; the message names the key.
    """
    from sqlalchemy import MetaData

    from loom.core.backend.scoped_ddl import SCHEMA_KEY
    from loom.core.backend.sqlalchemy import (
        compile_all,
        configured_naming_convention,
        scoped_tables,
    )

    path = _resolve_path(config_path)
    environment = resolve_environment()
    paths = config_source_paths(path, environment)
    context = load_application_config(*paths, environment=environment)
    app = context.section(ConfigKey.APP, _AppSection)
    catalog = load_catalog(context)
    spec = catalog.require(store, backend=SQLALCHEMY_BACKEND, purpose="the application's schema")
    database = spec.decode(DatabaseConfig)
    add_code_path(app.code_path, Path(paths[0]).resolve().parent)
    stores = catalog.models_by_store(_discover(app.discovery))
    models = stores.get(store, ())
    skipped = sorted(key for key, held in stores.items() if held and key != store)
    if skipped:
        _log.info("StoresNotLoaded", store=store, skipped=skipped)
    schema = database.schema
    metadata = MetaData(
        naming_convention=configured_naming_convention(schema.naming_convention, path=spec.path)
    )
    if schema.name is not None:
        metadata.info[SCHEMA_KEY] = _schema_name(schema.name, spec.path)
    compile_all(*models, metadata=metadata)
    scoped = scoped_tables(metadata)
    bootstrap = _bootstrap(schema, spec.path) if scoped else None
    scope_sources = _scope_sources(schema, scoped, spec.path) if scoped else {}
    return Application(
        models=tuple(models),
        metadata=metadata,
        database=database,
        bootstrap=bootstrap,
        scoped=scoped,
        scope_sources=scope_sources,
        store=store,
    )


def add_code_path(code_path: str | os.PathLike[str], config_dir: Path) -> Path:
    """Put an application's code directory first on ``sys.path``, once.

    Args:
        code_path: ``app.code_path``; relative to *config_dir* unless absolute.
        config_dir: Directory of the application's first configuration file.

    Returns:
        The resolved directory. It is added only when it exists, so a
        configuration naming no local code leaves ``sys.path`` alone.
    """
    candidate = Path(code_path)
    if not candidate.is_absolute():
        candidate = config_dir / candidate
    resolved = candidate.resolve()
    if resolved.is_dir() and str(resolved) not in sys.path:
        sys.path.insert(0, str(resolved))
    return resolved


def _resolve_path(config_path: str | None) -> str:
    if config_path:
        return config_path
    from_env = loom_variable(CONFIG_VARIABLE)
    if not from_env:
        raise ConfigError(f"{CONFIG_VARIABLE} is not set and no config path was given")
    return from_env


def _discover(section: Mapping[str, Any]) -> tuple[type, ...]:
    """Discover the models ``app.discovery`` names; its unknown keys are ignored."""
    discovery = decode_discovery(section, strict=False)
    if discovery.mode not in DISCOVERY_MODES:
        raise ConfigError(f"app.discovery.mode {discovery.mode!r} is not supported")
    return discover(discovery).models


def _schema_name(name: str, path: str) -> str:
    try:
        return schema_identifier(name)
    except ValueError as exc:
        raise ConfigError(f"{path}.schema.name: {exc}") from exc


def _bootstrap(schema: SchemaConfig, path: str) -> BootstrapConfig:
    from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig, DatabaseRoles

    name = require_schema_key(schema.name, path, "name")
    roles = require_schema_key(schema.roles, path, "roles")
    users = require_schema_key(schema.database_users, path, "database_users")
    groups = require_schema_key(schema.groups, path, "groups")
    version_tables = require_schema_key(schema.version_tables, path, "version_tables")
    config = BootstrapConfig(
        schema=name,
        roles=DatabaseRoles(owner=roles.owner, migrator=roles.migrator),
        database_users={
            user: _database_user(user, section, path) for user, section in users.items()
        },
        names=SchemaNames(
            guard=require_schema_key(schema.guard, path, "guard"),
            readers=groups.readers,
            writers=groups.writers,
            version_table=version_tables.structure,
            data_version_table=version_tables.data,
        ),
    )
    try:
        return config.validated()
    except ValueError as exc:
        raise ConfigError(f"{path}.schema: {exc}") from exc


def _database_user(user: str, section: SchemaUser, path: str) -> DatabaseUser:
    from loom.core.repository.sqlalchemy.rls.config import Access, DatabaseUser

    choices: tuple[Access, ...] = get_args(Access)
    access: Access | None = next((c for c in choices if c == section.access), None)
    if access is None:
        raise ConfigError(
            f"{path}.schema.database_users.{user}.access {section.access!r} "
            f"is not one of {', '.join(choices)}"
        )
    return DatabaseUser(login=section.login, access=access)


def _scope_sources(schema: SchemaConfig, scoped: ScopedTables, path: str) -> dict[str, str]:
    sources = require_schema_key(schema.scopes, path, "scopes")
    for scope, binding in sorted(sources.items()):
        if not _SCOPE_SOURCE.fullmatch(binding):
            raise ConfigError(
                f"{path}.schema.scopes.{scope} {binding!r} must be "
                "identity.<attribute> or request.<name>"
            )
    declared = sorted(declared_scopes(scoped))
    for scope in declared:
        if scope not in sources:
            raise ConfigError(f"{path}.schema.scopes.{scope} is missing")
    return {scope: sources[scope] for scope in declared}
