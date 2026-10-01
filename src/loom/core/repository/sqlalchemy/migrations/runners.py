"""Alembic runners for the structural and the data tree of a row-scoped schema.

Both runners are called from the environment loom ships, through
``AsyncConnection.run_sync``, so they receive a synchronous connection. The
structural tree acts as the owner through the migrator; the data tree acts as
a declared bypass user and never switches role. They share one advisory lock
per schema and every revision closes with the guard's assertion.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Collection, Iterator, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from alembic import command
from alembic.config import Config
from alembic.runtime.migration import MigrationContext, MigrationInfo
from alembic.script import Script, ScriptDirectory
from alembic.util.exc import CommandError
from sqlalchemy import Connection, event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.backend.scoped_ddl import ASSERT_SCHEMA
from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.migrations.hook import scope_protection_hook
from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig, SchemaNames

if TYPE_CHECKING:
    from loom.core.locator import Application

IncludeObject = Callable[[Any, "str | None", str, bool, Any], bool]

DATA_TREE = "data"
STRUCTURAL_TREE = "structural"
READ_ONLY = "loom.read_only"
GUARD_SQLSTATE = "LG002"
_TIMEOUT = re.compile(r"^\d+(ms|s|min)?$")

LOCK = "SELECT pg_advisory_lock(hashtextextended('loom.schema:' || :schema, 0))"
UNLOCK = "SELECT pg_advisory_unlock(hashtextextended('loom.schema:' || :schema, 0))"
LANDING = "SELECT current_user, current_schema()"
BYPASS_SELF = "SELECT current_user, rolbypassrls FROM pg_roles WHERE rolname = current_user"
PREPARE_VERSION_TABLES = "SELECT prepare_version_tables()"
REGISTERED_TABLES = "SELECT registered_tables()"
SET_TIMEOUTS = (
    "SELECT set_config('lock_timeout', :lock, true), "
    "set_config('statement_timeout', :statement, true)"
)
_ASSERT_SCHEMA = text(ASSERT_SCHEMA)
_SET_TIMEOUTS = text(SET_TIMEOUTS)


def alembic_config(script_location: str, url: str) -> Config:
    """Build a programmatic Alembic configuration for one tree.

    A script location whose last path element is ``data`` is the data tree;
    anything else is the structural tree. The URL may be ``postgresql+asyncpg``.
    """
    config = Config()
    config.set_main_option("script_location", script_location)
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    config.attributes["tree"] = (
        DATA_TREE if Path(script_location).name == DATA_TREE else STRUCTURAL_TREE
    )
    return config


def validate_timeout(value: str) -> str:
    """Accept a Postgres duration literal such as ``5s``, ``250ms`` or ``2min``."""
    if not _TIMEOUT.fullmatch(value):
        raise ValueError(f"timeout must be a Postgres duration like '5s', got {value!r}")
    return value


def run_migrations(
    connection: Connection,
    application: Application,
    *,
    lock_timeout: str = "5s",
    statement_timeout: str = "60s",
) -> None:
    """Run the structural tree as the owner, one transaction per revision."""
    from alembic import context

    bootstrap = _require_bootstrap(application)
    schema, owner, names = bootstrap.schema, bootstrap.roles.owner, bootstrap.names
    read_only = bool(context.config.attributes.get(READ_ONLY))
    _reject_revisions(context.config, data_tree=False)
    _require_landing(connection, owner, schema)
    if read_only:
        connection.commit()
    else:
        _lock(connection, schema)
    try:
        if not read_only:
            connection.execute(text(PREPARE_VERSION_TABLES))
        connection.commit()
        _install_timeouts(connection, lock_timeout, statement_timeout)
        context.configure(
            connection=connection,
            target_metadata=application.metadata,
            transaction_per_migration=True,
            compare_type=True,
            version_table=names.version_table,
            version_table_schema=schema,
            include_object=include_object_for(names),
            process_revision_directives=scope_protection_hook(application),
            on_version_apply=_assert_after_revision,
        )
        context.run_migrations()
    finally:
        if not read_only:
            _unlock(connection, schema)


def run_data_migrations(
    connection: Connection,
    application: Application,
    *,
    lock_timeout: str = "5s",
    statement_timeout: str = "60s",
) -> None:
    """Run the data tree as a declared bypass user; the structural tree must be at head.

    No closing assertion: the bypass user has no access to the guard and no
    ``CREATE`` on the schema, so a data revision cannot change the schema.
    """
    from alembic import context

    bootstrap = _require_bootstrap(application)
    schema, names = bootstrap.schema, bootstrap.names
    _reject_revisions(context.config, data_tree=True)
    _require_bypass(connection, application)
    _lock(connection, schema)
    try:
        _require_structural_head(connection, context.config, schema, names)
        connection.commit()
        _install_timeouts(connection, lock_timeout, statement_timeout)
        context.configure(
            connection=connection,
            transaction_per_migration=True,
            version_table=names.data_version_table,
            version_table_schema=schema,
        )
        context.run_migrations()
    finally:
        _unlock(connection, schema)


def check(config: Config, application: Application) -> None:
    """Fail when the models, the database or the guard registry disagree.

    Tables and columns are compared by Alembic's autogenerate; the registry is
    compared against the application's scoped tables. Policy and privilege
    drift belong to ``verify``. Call it from a thread without a running loop.

    Raises:
        ConfigError: Naming the first drift found.
    """
    config.attributes[READ_ONLY] = True
    try:
        command.check(config)
    except CommandError as exc:
        raise ConfigError(f"schema drift: {exc}") from exc
    except DBAPIError as exc:
        if _sqlstate(exc) != GUARD_SQLSTATE:
            raise
        raise ConfigError(f"guard assertion failed: {exc.orig}") from exc
    asyncio.run(_registry_drift(config, application))


def _sqlstate(exc: DBAPIError) -> str | None:
    original = exc.orig
    return getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)


async def _registry_drift(config: Config, application: Application) -> None:
    bootstrap = _require_bootstrap(application)
    url = str(config.get_main_option("sqlalchemy.url")).replace("%%", "%")
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(text(REGISTERED_TABLES))
            registered = {row[0] for row in rows}
    finally:
        await engine.dispose()
    expected = {table.name for table in application.scoped.values()}
    if registered != expected:
        missing = sorted(expected - registered)
        extra = sorted(registered - expected)
        raise ConfigError(
            f"registry drift in {bootstrap.names.guard}: missing {missing}, unexpected {extra}"
        )


def _require_bootstrap(application: Application) -> BootstrapConfig:
    bootstrap = application.bootstrap
    if bootstrap is None:
        raise ConfigError(
            "migrations need database.schema.name, roles and database_users in the configuration"
        )
    try:
        return bootstrap.validated()
    except ValueError as exc:
        raise ConfigError(f"database.schema: {exc}") from exc


def _reject_revisions(config: Config, *, data_tree: bool) -> None:
    for script in _revisions(config):
        flagged = bool(getattr(script.module, "data_migration", False))
        if flagged and not data_tree:
            raise ConfigError(
                f"revision {script.revision} sets data_migration = True but lives in the "
                "structural tree: move it to alembic/data"
            )
        if data_tree and not flagged:
            raise ConfigError(
                f"revision {script.revision} lives in the data tree without data_migration = True"
            )


def _revisions(config: Config) -> Iterator[Script]:
    yield from ScriptDirectory.from_config(config).walk_revisions()


def _require_landing(connection: Connection, owner: str, schema: str) -> None:
    row = connection.execute(text(LANDING)).one()
    if row[0] != owner or row[1] != schema:
        raise ConfigError(
            f"the migrator must act as {owner!r} inside schema {schema!r}, "
            f"found {row[0]!r} in {row[1]!r}: run apply_bootstrap (loom schema bootstrap)"
        )


def _require_bypass(connection: Connection, application: Application) -> None:
    user, bypass = connection.execute(text(BYPASS_SELF)).one()
    users = application.bootstrap.database_users if application.bootstrap else {}
    declared = users.get(str(user))
    if not bypass or declared is None or declared.access != "bypass":
        raise ConfigError(
            f"data migrations run as a declared bypass user; {user!r} is not one: "
            'connect with the URL of a database user declared with access="bypass"'
        )


def _require_structural_head(
    connection: Connection, config: Config, schema: str, names: SchemaNames
) -> None:
    location = Path(str(config.get_main_option("script_location")))
    structural = Config()
    structural.set_main_option("script_location", str(location.parent))
    heads = set(ScriptDirectory.from_config(structural).get_heads())
    applied = set(
        MigrationContext.configure(
            connection,
            opts={"version_table": names.version_table, "version_table_schema": schema},
        ).get_current_heads()
    )
    if heads != applied:
        raise ConfigError(
            "the structural tree must be at head before data migrations: "
            f"heads {sorted(heads)}, applied {sorted(applied)}"
        )


def _lock(connection: Connection, schema: str) -> None:
    connection.execute(text(LOCK), {"schema": schema})


def _unlock(connection: Connection, schema: str) -> None:
    connection.rollback()
    connection.execute(text(UNLOCK), {"schema": schema})
    connection.commit()


def _install_timeouts(connection: Connection, lock_timeout: str, statement_timeout: str) -> None:
    timeouts = {
        "lock": validate_timeout(lock_timeout),
        "statement": validate_timeout(statement_timeout),
    }

    def on_begin(conn: Connection) -> None:
        conn.execute(_SET_TIMEOUTS, timeouts)

    event.listen(connection, "begin", on_begin)


def _assert_after_revision(
    ctx: MigrationContext,
    step: MigrationInfo,
    heads: Collection[Any],
    run_args: Mapping[str, Any],
) -> None:
    del step, heads, run_args
    connection = ctx.connection
    if connection is not None:
        connection.execute(_ASSERT_SCHEMA)


def include_object_for(names: SchemaNames) -> IncludeObject:
    """Leave version tables alone and never autogenerate the drop of an undeclared table.

    A table the database has and the models do not may be one discovery
    missed; dropping it is written by hand, never generated.
    """
    version_tables = {names.version_table, names.data_version_table}

    def include_object(
        _obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any
    ) -> bool:
        if type_ != "table":
            return True
        if name in version_tables:
            return False
        return not (reflected and compare_to is None)

    return include_object
