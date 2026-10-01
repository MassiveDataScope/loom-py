"""Create an application's schema as the migrator, protected table by table."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.backend.scoped_ddl import (
    ASSERT_SCHEMA,
    MISSING_EVENT_TRIGGERS,
    check_dialect,
)
from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.session_manager import SessionManager

if TYPE_CHECKING:
    from loom.core.locator import Application

PROTECT_SIGNATURE = "protect_scoped_table(regclass,jsonb,text[])"
CURRENT_SCHEMA = "SELECT current_schema()"
GUARD_FUNCTION = (
    "SELECT n.nspname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
    "WHERE p.oid = to_regprocedure(:signature)"
)
_ASSERT_SCHEMA = text(ASSERT_SCHEMA)
_MISSING_EVENT_TRIGGERS = text(MISSING_EVENT_TRIGGERS)


async def create_schema(migrator_url: str, application: Application) -> None:
    """Run ``create_all`` on the application's metadata through the migrator.

    The listeners registered at compile time open the hatch and call the guard
    for every scoped table, so each table is protected in the transaction that
    creates it; the guard's assertion closes the transaction.

    Raises:
        ConfigError: When scoped tables exist on a non-Postgres dialect without
            the opt-out, when the guard of the application schema is missing,
            or when the migrator does not land in the application schema.
    """
    manager = SessionManager(migrator_url)
    try:
        check_dialect(
            manager.engine.dialect.name,
            application.scoped,
            allow_unprotected=application.database.schema.allow_unprotected_dialect,
        )
        bootstrap = application.bootstrap if application.scoped else None
        async with manager.engine.begin() as connection:
            if bootstrap is not None:
                await _require_guard(connection, bootstrap.schema, bootstrap.names.guard)
            await connection.run_sync(application.metadata.create_all)
            if bootstrap is not None:
                await connection.execute(_ASSERT_SCHEMA)
    finally:
        await manager.dispose()


async def _require_guard(connection: AsyncConnection, schema: str, guard: str) -> None:
    current = (await connection.execute(text(CURRENT_SCHEMA))).scalar()
    if current != schema:
        raise ConfigError(
            f"the migrator lands in schema {current!r}, not {schema!r}: run apply_bootstrap "
            "(loom schema bootstrap) so its search_path points at the application schema"
        )
    resolved = (
        await connection.execute(text(GUARD_FUNCTION), {"signature": PROTECT_SIGNATURE})
    ).scalar()
    if resolved != guard:
        raise ConfigError(
            f"protect_scoped_table resolves to schema {resolved!r}, not to the guard {guard!r}: "
            "run apply_bootstrap (loom schema bootstrap) first"
        )
    result = await connection.execute(_MISSING_EVENT_TRIGGERS, {"guard": guard})
    names = sorted(str(row[0]) for row in result)
    if names:
        raise ConfigError(
            f"the guard of schema {schema!r} has missing or disabled event triggers {names}: "
            "re-apply the bootstrap (loom schema bootstrap)"
        )
