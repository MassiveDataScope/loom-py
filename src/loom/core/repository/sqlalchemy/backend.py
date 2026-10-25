"""The ``sqlalchemy`` persistence backend."""

from __future__ import annotations

import functools
import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar, Final

from sqlalchemy import MetaData, Time, inspect, make_url, text
from sqlalchemy.exc import NoReferencedTableError
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.pool import QueuePool

from loom.core.authz.elevation import ElevationSink
from loom.core.authz.product import load_authz_product
from loom.core.backend.scoped_ddl import POSTGRES_DIALECT, check_dialect
from loom.core.backend.sqlalchemy import (
    compile_all,
    configured_naming_convention,
    get_metadata,
    reset_registry,
    scoped_tables,
)
from loom.core.config import ConfigContext, ConfigError
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.model.scoped import ScopedTable
from loom.core.persistence.abc import PersistenceWiring
from loom.core.persistence.database import (
    EXTERNAL,
    DatabaseConfig,
    SchemaConfig,
    load_database_config,
)
from loom.core.persistence.profile import BackendRuntime
from loom.core.persistence.routing import group_models
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.registry import (
    SQLAlchemyDefaultRepositoryBuilder,
    build_sqlalchemy_repository_registration_module,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.rls.elevate import (
    SQLAlchemyElevationSink,
    validate_elevations,
)
from loom.core.repository.sqlalchemy.rls.integrity import startup_problems
from loom.core.repository.sqlalchemy.rls.provider import DeferredScopedSettings, install_pool_reset
from loom.core.repository.sqlalchemy.session_manager import SessionManager, build_session_manager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory

_logger = logging.getLogger(__name__)


PROBE: Final = "SELECT 1"
PRIVILEGED_ROLE: Final = (
    "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
)
_PROBE: Final = text(PROBE)
_PRIVILEGED_ROLE: Final = text(PRIVILEGED_ROLE)


class SQLAlchemyBackend:
    """Backend for ``persistence.backend: sqlalchemy``.

    Reads the ``database`` and ``persistence.cursors`` sections, opens one
    engine shared by the unit of work and every repository, creates the
    compiled tables at startup and disposes the engine at shutdown.
    ``build`` is that single section. When a model sets ``__database__``,
    the host calls :func:`assemble_databases` instead.
    """

    name: ClassVar[str] = "sqlalchemy"
    repository_type: ClassVar[type[Any] | None] = RepositorySQLAlchemy
    assemble_databases: ClassVar[
        Callable[
            [ConfigContext, Sequence[type[BaseModel]], BackendRuntime],
            PersistenceWiring,
        ]
    ]

    def build(
        self,
        ctx: ConfigContext,
        models: Sequence[type[BaseModel]],
        runtime: BackendRuntime,
    ) -> PersistenceWiring:
        """Build the SQLAlchemy wiring for the discovered models.

        Args:
            ctx: Configuration context holding the ``database`` section.
            models: Models whose repositories the DI module registers.
            runtime: Entry-point pool defaults and the optional pool-metrics receiver.

        Returns:
            The SQLAlchemy wiring.

        Raises:
            ConfigError: When the ``database`` section is missing or invalid,
                or ``persistence.cursors`` cannot build a cursor codec.
        """
        db_cfg = load_database_config(ctx)
        postgres = make_url(str(db_cfg.url)).get_backend_name() == POSTGRES_DIALECT
        settings = (
            DeferredScopedSettings(db_cfg.schema.scopes or {})
            if db_cfg.schema.mode == EXTERNAL and postgres
            else None
        )
        convention = configured_naming_convention(db_cfg.schema.naming_convention)
        cursor_codec = CursorCodec.from_context(ctx)
        session_manager = build_session_manager(
            db_cfg,
            runtime.profile,
            pool_metrics=runtime.pool_metrics,
            session_settings=settings,
        )
        if settings is not None:
            install_pool_reset(session_manager.engine)
        return PersistenceWiring(
            uow_factory=SQLAlchemyUnitOfWorkFactory(session_manager),
            repo_registration_module=_with_elevation_sink(
                build_sqlalchemy_repository_registration_module(
                    session_manager, models, cursor_codec=cursor_codec
                ),
                enabled=settings is not None,
            ),
            lifespan_init=lambda: _lifespan(session_manager),
            prepare_models=functools.partial(
                _prepare_models, convention=convention, settings=settings
            ),
            startup_checks=lambda: _run_startup_checks(session_manager, db_cfg.schema),
            readiness=lambda: _readiness(session_manager),
        )


def _with_elevation_sink(
    module: Callable[[LoomContainer], None], *, enabled: bool
) -> Callable[[LoomContainer], None]:
    if not enabled:
        return module

    def register(container: LoomContainer) -> None:
        module(container)
        container.register_instance(ElevationSink, SQLAlchemyElevationSink())

    return register


def _prepare_models(
    models: Sequence[type[BaseModel]],
    *,
    convention: Mapping[str, str] | None = None,
    settings: DeferredScopedSettings | None = None,
) -> None:
    """Compile the discovered models and bind in-memory row-level security.

    The database is not contacted. A later ``startup_checks`` is what creates
    or verifies the schema.
    """
    _compile_and_bind(models, convention=convention, settings=settings)
    if settings is not None:
        _validate_elevations()


def _compile_and_bind(
    models: Sequence[type[BaseModel]],
    *,
    convention: Mapping[str, str] | None,
    settings: DeferredScopedSettings | None,
) -> None:
    if not models:
        _logger.warning(
            "no BaseModel classes discovered: the application starts with an empty "
            "relational schema. Declare your first model, or set "
            "persistence.backend: none if it never persists."
        )
    reset_registry(naming_convention=convention)
    compile_all(*models)
    if settings is None:
        return
    settings.bind(scoped_tables())


def _validate_elevations() -> None:
    product = load_authz_product()
    if product is None:
        return
    validate_elevations(product, scoped_tables())


async def _readiness(session_manager: SessionManager) -> bool:
    """Probe the database with ``SELECT 1``.

    Any failure is logged with its traceback and reported as not ready: the
    probe exists to be answered, never to raise.
    """
    try:
        async with session_manager.session() as session:
            await session.execute(_PROBE)
    except Exception:
        _logger.warning("sqlalchemy readiness probe failed", exc_info=True)
        return False
    return True


def startup_checks(
    config: SchemaConfig,
    dialect: str,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    *,
    has_session_settings: bool,
) -> tuple[str, ...]:
    """Decide what startup may do; return the scoped tables left unprotected on purpose.

    Raises:
        ConfigError: When ``create_all`` would run with the application's session
            settings, when scoped tables exist on a non-Postgres dialect without
            the opt-out, or when ``create_all`` meets scoped tables on Postgres.
    """
    if config.mode == EXTERNAL:
        return check_dialect(dialect, scoped, allow_unprotected=config.allow_unprotected_dialect)
    if has_session_settings:
        raise ConfigError(
            "the schema is never created with the application's session settings; "
            "use database.schema.mode: external and create_schema(migrator_url, application)"
        )
    unprotected = check_dialect(dialect, scoped, allow_unprotected=config.allow_unprotected_dialect)
    if scoped and dialect == "postgresql":
        names = ", ".join(sorted(table.name for table in scoped.values()))
        raise ConfigError(
            f"create_all cannot protect scoped tables {names}; run "
            "create_schema(migrator_url, application) and set database.schema.mode: external"
        )
    return unprotected


def check_time_zone_columns(
    dialect: str, metadata: MetaData, *, tables: frozenset[str] | None = None
) -> None:
    """Refuse a ``Time(timezone=True)`` column on a dialect that drops its offset.

    Postgres stores such a column as ``time with time zone`` and keeps the
    offset; SQLite ignores the zone and stores the time without it. ``tables``
    limits the check to one database's tables when several share the registry.

    Raises:
        ConfigError: When *metadata* holds such a column and *dialect* is not
            Postgres, naming each ``table.column``.
    """
    if dialect == POSTGRES_DIALECT:
        return
    columns = sorted(
        f"{table.name}.{column.name}"
        for table in metadata.tables.values()
        if tables is None or table.name in tables
        for column in table.columns
        if isinstance(column.type, Time) and column.type.timezone
    )
    if columns:
        raise ConfigError(
            f"dialect {dialect!r} would drop the UTC offset of the Time(timezone=True) "
            f"columns {', '.join(columns)}; declare a JSON column to keep the offset"
        )


async def _run_startup_checks(
    session_manager: SessionManager,
    config: SchemaConfig,
    tables: frozenset[str] | None = None,
) -> None:
    """Create or verify the schema, then drop queued connections.

    An in-memory SQLite pool is the database itself, so that connection stays.
    A failed check releases a queued pool too: the connection must not sit
    there after the error. ``tables`` is this engine's tables when another
    database shares the compiled registry; ``None`` means every compiled table.
    """
    scoped = _owned_scope(scoped_tables(), tables)
    dialect = session_manager.engine.dialect.name
    unprotected = startup_checks(
        config,
        dialect,
        scoped,
        has_session_settings=session_manager.has_session_settings,
    )
    check_time_zone_columns(dialect, get_metadata(), tables=tables)
    if unprotected:
        _logger.warning(
            "scoped tables left unprotected on this dialect: %s", ", ".join(unprotected)
        )
    selected = _owned_metadata(tables)

    def _create_owned(sync: Any) -> None:
        if selected is None:
            get_metadata().create_all(sync)
        else:
            get_metadata().create_all(sync, tables=selected)

    try:
        async with session_manager.engine.begin() as connection:
            if config.mode == EXTERNAL:
                await _check_external(connection, scoped, config.guard, tables)
            else:
                await connection.run_sync(_create_owned)
    finally:
        if isinstance(session_manager.engine.sync_engine.pool, QueuePool):
            await session_manager.dispose()


@asynccontextmanager
async def _lifespan(session_manager: SessionManager) -> AsyncIterator[None]:
    """Open nothing up front and release the engine on the way out.

    Schema checks belong to ``startup_checks``. The compiled registry stays:
    the next ``prepare_models`` is what resets it.
    """
    try:
        yield
    finally:
        await session_manager.dispose()


def _owned_scope(
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    tables: frozenset[str] | None,
) -> Mapping[tuple[str | None, str], ScopedTable]:
    if tables is None:
        return scoped
    return {key: table for key, table in scoped.items() if table.name in tables}


def _owned_metadata(tables: frozenset[str] | None) -> list[Any] | None:
    """Tables this engine creates. ``None`` means the whole compiled registry."""
    if tables is None:
        return None
    return [table for table in get_metadata().sorted_tables if table.name in tables]


async def _check_external(
    connection: AsyncConnection,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    guard: str | None,
    tables: frozenset[str] | None = None,
) -> None:
    names = [
        table.name
        for table in get_metadata().sorted_tables
        if tables is None or table.name in tables
    ]
    present = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
    missing = sorted(name for name in names if name not in present)
    if missing:
        raise ConfigError(
            f"database.schema.mode is external but tables are missing: {', '.join(missing)}"
        )
    if connection.dialect.name == POSTGRES_DIALECT:
        await _reject_privileged_connection(connection)
        if scoped:
            await _check_guard(connection, scoped, guard)


async def _check_guard(
    connection: AsyncConnection,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    guard: str | None,
) -> None:
    """Check, as the application user, the guard and every scoped table's protection.

    The guard's catalogue, owners, grants, function settings and event
    triggers, and each scoped table's forced row-level security, canonical
    permissive policies and owner trigger bound to this guard. The guard's
    configuration row is not readable here; ``verify`` checks it.
    """
    if guard is None:
        raise ConfigError(
            "database.schema.guard is required when a model is RowScoped: "
            "run `loom schema init <schema>` to write the derived names"
        )
    problems = await startup_problems(connection, guard, scoped)
    if problems:
        details = "; ".join(f"{p.check} {p.subject}: {p.actual}" for p in problems)
        raise ConfigError(
            f"the guard {guard} or the scoped tables differ from what this release of loom "
            f"ships: {details}"
        )


async def _reject_privileged_connection(connection: AsyncConnection) -> None:
    if (await connection.execute(_PRIVILEGED_ROLE)).scalar():
        raise ConfigError(
            "the application connects as a superuser or a bypass role, which ignores "
            "row-level security; use the URL of a read or write database user"
        )


def _section_name(name: str | None) -> str:
    """Return ``database`` or ``database.<name>``."""
    if name is None:
        return "database"
    return f"database.{name}"


def _table_names() -> set[str]:
    return {table.name for table in get_metadata().tables.values()}


def _checked_tables(part: _DatabasePart) -> frozenset[str] | None:
    """Tables this engine may create.

    ``None`` is the single-database path: the whole registry. A named engine
    that has not been prepared creates nothing, so a later group's tables
    cannot land on it. After preparation the recorded names are explicit,
    including the empty set.
    """
    if part.prepared:
        return frozenset(part.owned)
    if part.name is None:
        return None
    return frozenset()


@dataclass
class _DatabasePart:
    """One ``database`` section: its engine and the tables that section owns."""

    name: str | None
    manager: SessionManager
    schema_config: SchemaConfig
    convention: Mapping[str, str] | None
    settings: DeferredScopedSettings | None
    register: Callable[[LoomContainer], None]
    owned: set[str]
    prepared: bool = False

    async def startup(self) -> None:
        await _run_startup_checks(self.manager, self.schema_config, _checked_tables(self))


def _build_part(
    config: DatabaseConfig,
    models: Sequence[type[BaseModel]],
    runtime: BackendRuntime,
    *,
    name: str | None,
    codec: CursorCodec,
) -> _DatabasePart:
    """Build one section's engine. The engine has not connected yet."""
    postgres = make_url(str(config.url)).get_backend_name() == POSTGRES_DIALECT
    settings = (
        DeferredScopedSettings(config.schema.scopes or {})
        if config.schema.mode == EXTERNAL and postgres
        else None
    )
    convention = configured_naming_convention(config.schema.naming_convention)
    session_manager = build_session_manager(
        config,
        runtime.profile,
        pool_metrics=runtime.pool_metrics,
        database=name or "default",
        session_settings=settings,
    )
    try:
        if settings is not None:
            install_pool_reset(session_manager.engine)
        builder = None
        if name is not None:
            builder = SQLAlchemyDefaultRepositoryBuilder(session_manager, codec)
        register = build_sqlalchemy_repository_registration_module(
            session_manager,
            models,
            cursor_codec=codec,
            default_builder=builder,
        )
        if name is None:
            register = _with_elevation_sink(register, enabled=settings is not None)
        return _DatabasePart(
            name=name,
            manager=session_manager,
            schema_config=config.schema,
            convention=convention,
            settings=settings,
            register=register,
            owned=set(),
        )
    except Exception:
        session_manager.engine.sync_engine.dispose()
        raise


def _close_unopened(parts: Sequence[_DatabasePart]) -> None:
    """Drop engines that have not checked out a connection."""
    for part in parts:
        part.manager.engine.sync_engine.dispose()


def _load_sections(
    ctx: ConfigContext,
    groups: Sequence[tuple[str | None, Sequence[type[BaseModel]]]],
) -> dict[str | None, DatabaseConfig]:
    """Decode every used section before any engine exists."""
    return {name: load_database_config(ctx, _section_name(name)) for name, _models in groups}


def _open_parts(
    groups: Sequence[tuple[str | None, Sequence[type[BaseModel]]]],
    configs: Mapping[str | None, DatabaseConfig],
    runtime: BackendRuntime,
    codec: CursorCodec,
) -> tuple[_DatabasePart, ...]:
    opened: list[_DatabasePart] = []
    try:
        for name, models in groups:
            opened.append(_build_part(configs[name], models, runtime, name=name, codec=codec))
    except Exception:
        _close_unopened(opened)
        raise
    return tuple(opened)


def _register_nothing(_container: LoomContainer) -> None:
    """Register nothing: no model named a database."""


def assemble_databases(
    ctx: ConfigContext,
    models: Sequence[type[BaseModel]],
    runtime: BackendRuntime,
) -> PersistenceWiring:
    """Open one engine per database a model names, and none for the rest.

    The default group, models with no ``__database__``, is the kernel's unit
    of work. When every model names a section, that factory is ``None`` and
    the ``database`` section is not opened. Sections are validated before the
    first engine exists. The host still sees one wiring: its ``prepare_models``
    compiles the default group first and appends the others to the same
    registry, and each engine creates only the tables of its group.

    Raises:
        ConfigError: When a section is missing or invalid, two databases share
            a table name, a foreign key crosses databases, or a later section's
            naming convention disagrees with the shared metadata.
    """
    groups = group_models(models)
    if not groups:
        return PersistenceWiring(uow_factory=None, repo_registration_module=_register_nothing)
    configs = _load_sections(ctx, groups)
    codec = CursorCodec.from_context(ctx)
    return _facade(_open_parts(groups, configs, runtime, codec))


def _facade(parts: Sequence[_DatabasePart]) -> PersistenceWiring:
    default = next((part for part in parts if part.name is None), None)
    return PersistenceWiring(
        uow_factory=None if default is None else SQLAlchemyUnitOfWorkFactory(default.manager),
        repo_registration_module=lambda container: _register_parts(parts, container),
        lifespan_init=lambda: _lifespan_parts(parts),
        prepare_models=lambda models: _prepare_parts(parts, models),
        startup_checks=lambda: _startup_parts(parts),
        readiness=lambda: _readiness_parts(parts),
    )


def _register_parts(parts: Sequence[_DatabasePart], container: LoomContainer) -> None:
    for part in parts:
        part.register(container)


@asynccontextmanager
async def _lifespan_parts(parts: Sequence[_DatabasePart]) -> AsyncIterator[None]:
    async with AsyncExitStack() as stack:
        for part in parts:
            await stack.enter_async_context(_lifespan(part.manager))
        yield


async def _startup_parts(parts: Sequence[_DatabasePart]) -> None:
    for part in parts:
        await part.startup()


async def _readiness_parts(parts: Sequence[_DatabasePart]) -> bool:
    for part in parts:
        if not await _readiness(part.manager):
            return False
    return True


def _prepare_parts(parts: Sequence[_DatabasePart], models: Sequence[type[BaseModel]]) -> None:
    groups = group_models(models)
    by_name = {part.name: part for part in parts}
    _require_open_parts(groups, by_name)
    if groups:
        owners = _declared_owners(groups)
        primary_name, primary_models = groups[0]
        _prepare_part(by_name[primary_name], primary_models, owners)
        for name, grouped in groups[1:]:
            _append_part(by_name[name], grouped, owners)
        _reject_cross_database_foreign_keys(parts)
    opened = {name for name, _models in groups}
    for part in parts:
        if part.name not in opened:
            part.owned = set()
            part.prepared = True
    if groups and any(part.settings is not None for part in parts):
        _validate_elevations()


def _require_open_parts(
    groups: Sequence[tuple[str | None, Sequence[type[BaseModel]]]],
    by_name: Mapping[str | None, _DatabasePart],
) -> None:
    missing = [name for name, _models in groups if name not in by_name]
    if missing:
        raise ConfigError(f"{_section_name(missing[0])} is not open for these models")


def _declared_owners(
    groups: Sequence[tuple[str | None, Sequence[type[BaseModel]]]],
) -> dict[str, str | None]:
    owners: dict[str, str | None] = {}
    for name, models in groups:
        for model in models:
            table = getattr(model, "__tablename__", None)
            if isinstance(table, str):
                owners[table] = name
    return owners


def _prepare_part(
    part: _DatabasePart,
    models: Sequence[type[BaseModel]],
    owners: Mapping[str, str | None],
) -> None:
    _compile_and_bind(models, convention=part.convention, settings=part.settings)
    _record_owned(part, set(), owners)


def _append_part(
    part: _DatabasePart,
    models: Sequence[type[BaseModel]],
    owners: Mapping[str, str | None],
) -> None:
    _reject_other_naming_convention(part.convention)
    before = _table_names()
    scoped_before = set(scoped_tables())
    compile_all(*models)
    _bind_new_scoped(part.settings, scoped_before)
    _record_owned(part, before, owners)


def _record_owned(
    part: _DatabasePart,
    before: set[str],
    owners: Mapping[str, str | None],
) -> None:
    appeared = _table_names() - before
    _reject_pulled_tables(part.name, appeared, owners)
    part.owned = appeared
    part.prepared = True


def _reject_other_naming_convention(convention: Mapping[str, str] | None) -> None:
    """A later section may omit the convention; it may not declare another one."""
    if convention is None:
        return
    if dict(convention) != dict(get_metadata().naming_convention):
        raise ConfigError(
            "database sections share one metadata, so schema.naming_convention must match"
        )


def _reject_pulled_tables(
    compiling: str | None,
    appeared: set[str],
    owners: Mapping[str, str | None],
) -> None:
    for table in sorted(appeared):
        if table not in owners:
            continue
        owner = owners[table]
        if owner == compiling:
            continue
        raise ConfigError(
            f"table {table!r} belongs to {_section_name(owner)} and was compiled "
            f"with {_section_name(compiling)}"
        )


def _bind_new_scoped(
    settings: DeferredScopedSettings | None,
    before: set[tuple[str | None, str]],
) -> None:
    if settings is None:
        return
    added = {key: table for key, table in scoped_tables().items() if key not in before}
    settings.bind(added)


def _reject_cross_database_foreign_keys(parts: Sequence[_DatabasePart]) -> None:
    owners: dict[str, str | None] = {}
    for part in parts:
        for name in part.owned:
            owners[name] = part.name
    for compiled in get_metadata().sorted_tables:
        if compiled.name not in owners:
            continue
        source = owners[compiled.name]
        for fk in compiled.foreign_keys:
            target = _foreign_table(fk)
            if target is None or target not in owners:
                continue
            if owners[target] != source:
                raise ConfigError(
                    f"table {compiled.name} references {target}, which is on another database"
                )


def _foreign_table(fk: Any) -> str | None:
    try:
        name = str(fk.column.table.name)
    except NoReferencedTableError:
        full = str(fk.target_fullname)
        if "." not in full:
            return None
        return full.rsplit(".", 1)[0]
    return name


SQLAlchemyBackend.assemble_databases = assemble_databases

__all__ = ["SQLAlchemyBackend", "assemble_databases"]
