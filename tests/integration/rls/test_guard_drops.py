"""Drops in the application schema keep the guard's invariants outside the hatch.

``DROP`` commands are not reported to ``ddl_command_end``, so the guard's
``sql_drop`` handler asserts the schema whenever an object of the application
schema is dropped outside the hatch.
"""

from __future__ import annotations

import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase, execute, scalar

pytestmark = pytest.mark.integration

SCOPES = '[{"col":"org","scope":"org","on":"both"}]'
PRIVILEGES = "ARRAY['SELECT','INSERT','UPDATE','DELETE']"


@asynccontextmanager
async def _connect(url: str) -> AsyncIterator[asyncpg.Connection]:
    raw = make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)
    conn = await asyncpg.connect(raw)
    try:
        yield conn
    finally:
        await conn.close()


async def _outcome(url: str, *statements: str) -> tuple[str, str]:
    """Run ``statements`` in one transaction and return its SQLSTATE and message."""
    async with _connect(url) as conn:
        try:
            async with conn.transaction():
                for statement in statements:
                    await conn.execute(statement)
        except asyncpg.PostgresError as exc:
            return str(exc.sqlstate), str(exc)
    return "none", ""


async def _guarded(scoped_database: BootstrapFactory) -> ScopedDatabase:
    """Bootstrap a schema whose table ``t`` is protected and carries a plain index."""
    schema = f"d{secrets.token_hex(3)}"
    database = await scoped_database(schema)
    table = f"{schema}.t"
    protect = (
        f"SELECT loom_guard_{schema}.protect_scoped_table('{table}', '{SCOPES}', {PRIVILEGES})"
    )
    outcome = await _outcome(
        database.migrator,
        f"SELECT loom_guard_{schema}.open_hatch()",
        f"CREATE TABLE {schema}.t (org text NOT NULL, note text, PRIMARY KEY (org))",
        f"CREATE INDEX t_note ON {schema}.t (note)",
        protect,
    )
    assert outcome == ("none", "")
    return database


async def _drop_event_triggers(database: ScopedDatabase) -> None:
    guard = f"loom_guard_{database.schema}"
    await execute(
        database.superuser,
        f"DROP EVENT TRIGGER IF EXISTS {guard}_ddl",
        f"DROP EVENT TRIGGER IF EXISTS {guard}_drop",
    )


async def test_dropping_a_policy_outside_the_hatch_is_refused(
    scoped_database: BootstrapFactory,
) -> None:
    database = await _guarded(scoped_database)
    schema = database.schema

    state, message = await _outcome(database.migrator, f"DROP POLICY loom_select ON {schema}.t")
    policies = await scalar(
        database.migrator,
        "SELECT count(*) FROM pg_policy WHERE polrelid = CAST(:t AS regclass) AND polname = :p",
        t=f"{schema}.t",
        p="loom_select",
    )
    await _drop_event_triggers(database)

    assert state == "LG002"
    assert "policy set differs from the registered canonical set" in message
    assert policies == 1


async def test_dropping_the_owner_trigger_outside_the_hatch_is_refused(
    scoped_database: BootstrapFactory,
) -> None:
    database = await _guarded(scoped_database)

    state, message = await _outcome(
        database.migrator, f"DROP TRIGGER loom_deny_owner_dml ON {database.schema}.t"
    )
    await _drop_event_triggers(database)

    assert state == "LG002"
    assert "owner trigger missing" in message


async def test_dropping_a_plain_index_outside_the_hatch_is_allowed(
    scoped_database: BootstrapFactory,
) -> None:
    database = await _guarded(scoped_database)

    outcome = await _outcome(database.migrator, f"DROP INDEX {database.schema}.t_note")
    await _drop_event_triggers(database)

    assert outcome == ("none", "")


async def test_dropping_a_policy_under_the_hatch_is_refused_at_commit(
    scoped_database: BootstrapFactory,
) -> None:
    database = await _guarded(scoped_database)
    schema = database.schema

    state, message = await _outcome(
        database.migrator,
        f"SELECT loom_guard_{schema}.open_hatch()",
        f"DROP POLICY loom_select ON {schema}.t",
    )
    await _drop_event_triggers(database)

    assert state == "LG002"
    assert "policy set differs from the registered canonical set" in message
