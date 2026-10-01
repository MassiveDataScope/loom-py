"""Create an application's schema as the migrator, protected table by table."""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.backend.scoped_ddl import (
    ASSERT_SCHEMA,
    GUARD_FIRST,
    GUARD_PATH,
    LOCK_TIMEOUT,
    MISSING_EVENT_TRIGGERS,
    SCHEMA_LOCK,
    check_dialect,
    validate_timeout,
)
from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.rls.config import BootstrapConfig
from loom.core.repository.sqlalchemy.rls.integrity import require_guard_revision
from loom.core.repository.sqlalchemy.session_manager import SessionManager

if TYPE_CHECKING:
    from loom.core.locator import Application

PROTECT_SIGNATURE: Final = "protect_scoped_table(regclass,jsonb,text[])"
CURRENT_SCHEMA: Final = "SELECT current_schema()"
GUARD_FUNCTION: Final = (
    "SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
    "WHERE p.oid = to_regprocedure(:signature)"
)
_CURRENT_SCHEMA: Final = text(CURRENT_SCHEMA)
_GUARD_FUNCTION: Final = text(GUARD_FUNCTION)
_ASSERT_SCHEMA: Final = text(ASSERT_SCHEMA)
_GUARD_FIRST: Final = text(GUARD_FIRST)
_LOCK_TIMEOUT: Final = text(LOCK_TIMEOUT)
_SCHEMA_LOCK: Final = text(SCHEMA_LOCK)
_MISSING_EVENT_TRIGGERS: Final = text(MISSING_EVENT_TRIGGERS)


async def create_schema(
    migrator_url: str, application: Application, *, lock_timeout: str = "5s"
) -> None:
    """Run ``create_all`` on the application's metadata through the migrator.

    The schema's advisory lock is taken first, the guard's revision is checked
    before any guard call, and the listeners registered at compile time open
    the hatch and call the guard, guard first, for every scoped table, so each
    table is protected in the transaction that creates it; the guard's
    assertion closes the transaction.

    Raises:
        ConfigError: When scoped tables exist on a non-Postgres dialect without
            the opt-out, when the guard of the application schema is missing,
            unknown or below the minimum compatible revision, or when the
            migrator does not land in the application schema.
    """
    try:
        validate_timeout(lock_timeout)
    except ValueError as exc:
        raise ConfigError(f"create_schema: {exc}") from exc
    manager = SessionManager(migrator_url)
    try:
        check_dialect(
            manager.engine.dialect.name,
            application.scoped,
            allow_unprotected=application.database.schema.allow_unprotected_dialect,
        )
        bootstrap = application.bootstrap if application.scoped else None
        async with manager.engine.begin() as connection:
            if bootstrap is None:
                await connection.run_sync(application.metadata.create_all)
            else:
                await _create_protected(connection, application, bootstrap, lock_timeout)
    finally:
        await manager.dispose()


async def _create_protected(
    connection: AsyncConnection,
    application: Application,
    bootstrap: BootstrapConfig,
    lock_timeout: str,
) -> None:
    schema, guard = bootstrap.schema, bootstrap.names.guard
    await connection.execute(_LOCK_TIMEOUT, {"lock": lock_timeout})
    await connection.execute(_SCHEMA_LOCK, {"schema": schema})
    await _require_guard(connection, schema, guard)
    await require_guard_revision(connection, guard)
    path = {"schema": schema, "guard": guard}
    info = (await connection.get_raw_connection()).info
    info[GUARD_PATH] = path
    try:
        await connection.run_sync(application.metadata.create_all)
    finally:
        info.pop(GUARD_PATH, None)
    await connection.execute(_GUARD_FIRST, path)
    await connection.execute(_ASSERT_SCHEMA)


async def _require_guard(connection: AsyncConnection, schema: str, guard: str) -> None:
    current = (await connection.execute(_CURRENT_SCHEMA)).scalar()
    if current != schema:
        raise ConfigError(
            f"the migrator lands in schema {current!r}, not {schema!r}: run apply_bootstrap "
            "so its search_path points at the application schema"
        )
    resolved = (
        await connection.execute(_GUARD_FUNCTION, {"signature": PROTECT_SIGNATURE})
    ).scalar()
    if resolved != guard:
        raise ConfigError(
            f"protect_scoped_table resolves to schema {resolved!r}, not to the guard {guard!r}: "
            "run apply_bootstrap first"
        )
    result = await connection.execute(_MISSING_EVENT_TRIGGERS, {"guard": guard})
    names = sorted(str(row[0]) for row in result)
    if names:
        raise ConfigError(
            f"the guard of schema {schema!r} has missing or disabled event triggers {names}: "
            "run apply_bootstrap again"
        )
