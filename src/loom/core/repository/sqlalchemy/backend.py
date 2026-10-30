"""The ``sqlalchemy`` persistence backend."""

from __future__ import annotations

import functools
import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any, ClassVar, Final

from sqlalchemy import MetaData, Time, inspect, make_url, text
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
from loom.core.di.scope import Scope
from loom.core.model import BaseModel
from loom.core.model.scoped import ScopedTable
from loom.core.persistence.abc import PersistenceWiring, no_model_preparation
from loom.core.persistence.database import EXTERNAL, SchemaConfig, load_database_config
from loom.core.persistence.profile import BackendRuntime
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.registry import (
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
    compiled tables at startup and disposes the engine at shutdown. Built
    connection-only, its startup probes the connection instead.
    """

    name: ClassVar[str] = "sqlalchemy"
    repository_type: ClassVar[type[Any] | None] = RepositorySQLAlchemy

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

        With ``runtime.connection_only`` no repository is served; see
        :class:`~loom.core.persistence.BackendRuntime`.

        Raises:
            ConfigError: When the ``database`` section is missing or invalid,
                or repositories are served and ``persistence.cursors`` cannot
                build a cursor codec.
        """
        db_cfg = load_database_config(ctx)
        postgres = make_url(str(db_cfg.url)).get_backend_name() == POSTGRES_DIALECT
        settings = (
            DeferredScopedSettings(db_cfg.schema.scopes or {})
            if db_cfg.schema.mode == EXTERNAL and postgres
            else None
        )
        # Before the engine: an invalid persistence.cursors fails with none built.
        register: Callable[[SessionManager], Callable[[LoomContainer], None]]
        prepare: Callable[[Sequence[type[BaseModel]]], None]
        check: Callable[[SessionManager, SchemaConfig], Awaitable[None]]
        if runtime.connection_only:
            prepare = no_model_preparation
            register = _session_manager_module
            check = _probe_connection
        else:
            check = _run_startup_checks
            prepare = functools.partial(
                _prepare_models,
                convention=configured_naming_convention(db_cfg.schema.naming_convention),
                settings=settings,
            )
            register = functools.partial(
                _repositories_module,
                models=models,
                cursor_codec=CursorCodec.from_context(ctx),
                elevation=settings is not None,
            )
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
            repo_registration_module=register(session_manager),
            lifespan_init=lambda: _lifespan(session_manager),
            prepare_models=prepare,
            startup_checks=functools.partial(check, session_manager, db_cfg.schema),
            readiness=lambda: _readiness(session_manager),
        )


def _session_manager_module(session_manager: SessionManager) -> Callable[[LoomContainer], None]:
    """The module of a backend serving no repository: the session manager only."""
    return functools.partial(_register_session_manager, session_manager)


def _repositories_module(
    session_manager: SessionManager,
    *,
    models: Sequence[type[BaseModel]],
    cursor_codec: CursorCodec,
    elevation: bool,
) -> Callable[[LoomContainer], None]:
    """The module registering a repository per model, and the elevation sink when scoped."""
    return _with_elevation_sink(
        build_sqlalchemy_repository_registration_module(
            session_manager, models, cursor_codec=cursor_codec
        ),
        enabled=elevation,
    )


def _register_session_manager(session_manager: SessionManager, container: LoomContainer) -> None:
    """Register the backend's session manager, the one every consumer shares.

    ``SessionManager`` is framework-owned: the composition refuses a caller's
    module binding it, so the unit of work, the pool reset and the startup
    checks hold the same instance the container serves.
    """
    container.register(SessionManager, lambda: session_manager, scope=Scope.APPLICATION)


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
    scoped = scoped_tables()
    settings.bind(scoped)
    product = load_authz_product()
    if product is not None:
        validate_elevations(product, scoped)


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


def check_time_zone_columns(dialect: str, metadata: MetaData) -> None:
    """Refuse a ``Time(timezone=True)`` column on a dialect that drops its offset.

    Postgres stores such a column as ``time with time zone`` and keeps the
    offset; SQLite ignores the zone and stores the time without it.

    Raises:
        ConfigError: When *metadata* holds such a column and *dialect* is not
            Postgres, naming each ``table.column``.
    """
    if dialect == POSTGRES_DIALECT:
        return
    columns = sorted(
        f"{table.name}.{column.name}"
        for table in metadata.tables.values()
        for column in table.columns
        if isinstance(column.type, Time) and column.type.timezone
    )
    if columns:
        raise ConfigError(
            f"dialect {dialect!r} would drop the UTC offset of the Time(timezone=True) "
            f"columns {', '.join(columns)}; declare a JSON column to keep the offset"
        )


async def _run_startup_checks(session_manager: SessionManager, config: SchemaConfig) -> None:
    """Create or verify the schema, then drop queued connections.

    An in-memory SQLite pool is the database itself, so that connection stays.
    A failed check releases a queued pool too: the connection must not sit
    there after the error.
    """
    scoped = scoped_tables()
    dialect = session_manager.engine.dialect.name
    unprotected = startup_checks(
        config,
        dialect,
        scoped,
        has_session_settings=session_manager.has_session_settings,
    )
    check_time_zone_columns(dialect, get_metadata())
    if unprotected:
        _logger.warning(
            "scoped tables left unprotected on this dialect: %s", ", ".join(unprotected)
        )
    try:
        async with session_manager.engine.begin() as connection:
            if config.mode == EXTERNAL:
                await _check_external(connection, scoped, config.guard)
            else:
                await connection.run_sync(get_metadata().create_all)
    finally:
        await _release_queued(session_manager)


async def _probe_connection(session_manager: SessionManager, config: SchemaConfig) -> None:
    """Open one connection and run ``SELECT 1``, then drop queued connections.

    The startup of a backend serving no repository: the process-wide metadata
    holds tables this host does not serve, so none is created or inspected and
    the schema settings in *config* are not consulted. The engine is used
    directly, not a session, so unbound row-level security settings do not
    refuse the probe. A failure to connect propagates as the driver raised it.
    """
    del config
    try:
        async with session_manager.engine.connect() as connection:
            await connection.execute(_PROBE)
    finally:
        await _release_queued(session_manager)


async def _release_queued(session_manager: SessionManager) -> None:
    """Dispose a queued pool, so a prefork parent forks with no connection."""
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


async def _check_external(
    connection: AsyncConnection,
    scoped: Mapping[tuple[str | None, str], ScopedTable],
    guard: str | None,
) -> None:
    names = [table.name for table in get_metadata().sorted_tables]
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


__all__ = ["SQLAlchemyBackend"]
