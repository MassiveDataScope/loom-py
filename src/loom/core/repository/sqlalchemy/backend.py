"""The ``sqlalchemy`` persistence backend."""

from __future__ import annotations

import functools
import logging
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import ClassVar, Final, Literal

import msgspec
from sqlalchemy import inspect, make_url, text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.authz.elevation import ElevationSink
from loom.core.authz.product import load_authz_product
from loom.core.backend.scoped_ddl import POSTGRES_DIALECT, check_dialect
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
from loom.core.repository.sqlalchemy.rls.integrity import startup_problems
from loom.core.repository.sqlalchemy.rls.provider import DeferredScopedSettings, install_pool_reset
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWorkFactory
from loom.core.schema_names import naming_convention

_logger = logging.getLogger(__name__)

EXTERNAL: Final = "external"

PROBE: Final = "SELECT 1"
PRIVILEGED_ROLE: Final = (
    "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
)
_PROBE: Final = text(PROBE)
_PRIVILEGED_ROLE: Final = text(PRIVILEGED_ROLE)


class _SchemaConfig(msgspec.Struct, kw_only=True, frozen=True):
    mode: Literal["create_all", "external"] = "create_all"
    allow_unprotected_dialect: bool = False
    scopes: dict[str, str] = msgspec.field(default_factory=dict)
    guard: str | None = None
    naming_convention: dict[str, str] | None = None


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
        postgres = make_url(db_cfg.url).get_backend_name() == POSTGRES_DIALECT
        settings = (
            DeferredScopedSettings(db_cfg.schema.scopes)
            if db_cfg.schema.mode == EXTERNAL and postgres
            else None
        )
        convention = _naming_convention(db_cfg.schema)
        session_manager = _build_session_manager(db_cfg, settings)
        return PersistenceWiring(
            uow_factory=SQLAlchemyUnitOfWorkFactory(session_manager),
            repo_registration_module=_with_elevation_sink(
                build_sqlalchemy_repository_registration_module(session_manager, models),
                enabled=settings is not None,
            ),
            lifespan_init=lambda: _lifespan(session_manager, db_cfg.schema, settings),
            default_repository_type=RepositorySQLAlchemy,
            prepare_models=functools.partial(_prepare_models, convention=convention),
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


def _naming_convention(config: _SchemaConfig) -> dict[str, str] | None:
    try:
        return naming_convention(config.naming_convention)
    except ValueError as exc:
        raise ConfigError(f"database.schema.naming_convention: {exc}") from exc


def _prepare_models(
    models: Sequence[type[BaseModel]], *, convention: Mapping[str, str] | None = None
) -> None:
    """Compile the discovered models into the shared registry under the declared convention."""
    if not models:
        _logger.warning(
            "no BaseModel classes discovered: the application starts with an empty "
            "relational schema. Declare your first model, or set "
            "persistence.backend: none if it never persists."
        )
    reset_registry(naming_convention=convention)
    compile_all(*models)


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
        if config.mode == EXTERNAL:
            await _check_external(connection, scoped, config.guard)
        else:
            await connection.run_sync(get_metadata().create_all)
    try:
        yield
    finally:
        await session_manager.dispose()
        reset_registry()


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
