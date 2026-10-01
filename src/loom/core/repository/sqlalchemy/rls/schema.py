"""Create an application's schema as the migrator, protected table by table."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from loom.core.backend.scoped_ddl import (
    assert_statement,
    check_dialect,
    guard_name,
    missing_event_triggers_statement,
)
from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.session_manager import SessionManager

if TYPE_CHECKING:
    from loom.core.locator import Application

PROTECT_SIGNATURE = "protect_scoped_table(regclass,jsonb,text[])"


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
        schema = application.database.schema
        check_dialect(
            manager.engine.dialect.name,
            application.scoped,
            allow_unprotected=schema.allow_unprotected_dialect,
        )
        async with manager.engine.begin() as connection:
            if application.scoped and schema.name is not None:
                await _require_guard(connection, schema.name)
            await connection.run_sync(application.metadata.create_all)
            if application.scoped and schema.name is not None:
                await connection.execute(text(assert_statement(schema.name)))
    finally:
        await manager.dispose()


async def _require_guard(connection: AsyncConnection, schema: str) -> None:
    current = (await connection.execute(text("SELECT current_schema()"))).scalar()
    if current != schema:
        raise ConfigError(
            f"the migrator lands in schema {current!r}, not {schema!r}: run apply_bootstrap "
            "(loom schema bootstrap) so its search_path points at the application schema"
        )
    guard = f"{guard_name(schema)}.{PROTECT_SIGNATURE}"
    found = (
        await connection.execute(text("SELECT to_regprocedure(:guard)"), {"guard": guard})
    ).scalar()
    if found is None:
        raise ConfigError(
            f"schema {schema!r} has no guard: run apply_bootstrap (loom schema bootstrap) first"
        )
    result = await connection.execute(text(missing_event_triggers_statement(schema)))
    missing = [str(row[0]) for row in result]
    names = sorted(missing)
    if names:
        raise ConfigError(
            f"the guard of schema {schema!r} has missing or disabled event triggers {names}: "
            "re-apply the bootstrap (loom schema bootstrap)"
        )
