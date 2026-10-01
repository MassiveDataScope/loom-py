"""The ``sqlalchemy`` persistence backend."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import ClassVar, Literal

import msgspec
from sqlalchemy import inspect, make_url, text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.authz.elevation import ElevationSink
from loom.core.authz.product import load_authz_product
from loom.core.backend.scoped_ddl import check_dialect
from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry, scoped_tables
from loom.core.config import ConfigContext, ConfigError, ConfigKey
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.model.scoped import ScopedTable
from loom.core.persistence.abc import PersistenceWiring
from loom.core.repository.sqlalchemy.registry import (
    build_sqlalchemy_repository_registration_module,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.rls.elevate import (
    SQLAlchemyElevationSink,
    validate_elevations,
)
from loom.core.repository.sqlalchemy.rls.provider import DeferredScopedSettings, install_pool_reset
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory

_logger = logging.getLogger(__name__)


class _SchemaConfig(msgspec.Struct, kw_only=True, frozen=True):
    mode: Literal["create_all", "external"] = "create_all"
    allow_unprotected_dialect: bool = False
    scopes: dict[str, str] = msgspec.field(default_factory=dict)


class _DatabaseConfig(msgspec.Struct, kw_only=True):
    url: str
    echo: bool | None = None
    pool_pre_ping: bool = True
    schema: _SchemaConfig = msgspec.field(default_factory=_SchemaConfig)


class SQLAlchemyBackend:
    """Backend for ``persistence.backend: sqlalchemy``.

    Reads the ``database`` section, opens one engine shared by the unit of
    work and every repository, creates the compiled tables at startup and
    disposes the engine at shutdown.
    """

    name: ClassVar[str] = "sqlalchemy"

    def build(self, ctx: ConfigContext, models: Sequence[type[BaseModel]]) -> PersistenceWiring:
        """Build the SQLAlchemy wiring for the discovered models.

        Args:
            ctx: Configuration context holding the ``database`` section.
            models: Models whose repositories the DI module registers.

        Returns:
            The SQLAlchemy wiring.

        Raises:
            ConfigError: When the ``database`` section is missing or invalid.
        """
        db_cfg = ctx.section(ConfigKey.DATABASE, _DatabaseConfig)
        postgres = make_url(db_cfg.url).get_backend_name() == "postgresql"
        settings = (
            DeferredScopedSettings(db_cfg.schema.scopes)
            if db_cfg.schema.mode == "external" and postgres
            else None
        )
        session_manager = _build_session_manager(db_cfg, settings)
        return PersistenceWiring(
            uow_factory=SQLAlchemyUnitOfWorkFactory(session_manager),
            repo_registration_module=_with_elevation_sink(
                build_sqlalchemy_repository_registration_module(session_manager, models),
                enabled=settings is not None,
            ),
            lifespan_init=lambda: _lifespan(session_manager, db_cfg.schema, settings),
            default_repository_type=RepositorySQLAlchemy,
            prepare_models=_prepare_models,
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


def _build_session_manager(
    db_cfg: _DatabaseConfig, settings: DeferredScopedSettings | None
) -> SessionManager:
    echo = db_cfg.echo if db_cfg.echo is not None else False
    manager = SessionManager(
        db_cfg.url,
        echo=echo,
        pool_pre_ping=db_cfg.pool_pre_ping,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
        session_settings=settings,
    )
    if settings is not None:
        install_pool_reset(manager.engine)
    return manager


def _prepare_models(models: Sequence[type[BaseModel]]) -> None:
    """Compile the discovered models into the shared SQLAlchemy registry."""
    if not models:
        _logger.warning(
            "no BaseModel classes discovered: the application starts with an empty "
            "relational schema. Declare your first model, or set "
            "persistence.backend: none if it never persists."
        )
    reset_registry()
    compile_all(*models)


async def _readiness(session_manager: SessionManager) -> bool:
    """Probe the database with ``SELECT 1``.

    Any failure is logged with its traceback and reported as not ready: the
    probe exists to be answered, never to raise.
    """
    try:
        async with session_manager.session() as session:
            await session.execute(text("SELECT 1"))
    except Exception:
        _logger.warning("sqlalchemy readiness probe failed", exc_info=True)
        return False
    return True


def startup_checks(
    config: _SchemaConfig,
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
    if config.mode == "external":
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


@asynccontextmanager
async def _lifespan(
    session_manager: SessionManager,
    config: _SchemaConfig,
    settings: DeferredScopedSettings | None,
) -> AsyncIterator[None]:
    scoped = scoped_tables()
    if settings is not None:
        settings.bind(scoped)
        product = load_authz_product()
        if product is not None:
            validate_elevations(product, scoped)
    unprotected = startup_checks(
        config,
        session_manager.engine.dialect.name,
        scoped,
        has_session_settings=session_manager.has_session_settings,
    )
    if unprotected:
        _logger.warning(
            "scoped tables left unprotected on this dialect: %s", ", ".join(unprotected)
        )
    async with session_manager.engine.begin() as connection:
        if config.mode == "external":
            await _check_external(connection, scoped)
        else:
            await connection.run_sync(get_metadata().create_all)
    try:
        yield
    finally:
        await session_manager.dispose()
        reset_registry()


async def _check_external(
    connection: AsyncConnection, scoped: Mapping[tuple[str | None, str], ScopedTable]
) -> None:
    names = [table.name for table in get_metadata().sorted_tables]
    present = await connection.run_sync(lambda sync: set(inspect(sync).get_table_names()))
    missing = sorted(name for name in names if name not in present)
    if missing:
        raise ConfigError(
            f"database.schema.mode is external but tables are missing: {', '.join(missing)}"
        )
    if scoped and connection.dialect.name == "postgresql":
        await _check_forced_rls(connection, scoped)


async def _check_forced_rls(
    connection: AsyncConnection, scoped: Mapping[tuple[str | None, str], ScopedTable]
) -> None:
    role = text("SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user")
    if (await connection.execute(role)).scalar():
        raise ConfigError(
            "the application connects as a superuser or a bypass role, which ignores "
            "row-level security; use the URL of a read or write database user"
        )
    query = text(
        "SELECT relrowsecurity AND relforcerowsecurity FROM pg_class "
        "WHERE relname = :name AND relnamespace = current_schema()::regnamespace"
    )
    unforced = [
        table.name
        for table in scoped.values()
        if not (await connection.execute(query, {"name": table.name})).scalar()
    ]
    if unforced:
        raise ConfigError(f"scoped tables without forced row-level security: {', '.join(unforced)}")


__all__ = ["SQLAlchemyBackend"]
