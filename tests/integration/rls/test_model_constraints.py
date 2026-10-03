"""Checks, partial unique indexes and a naming convention against a real Postgres.

The guard's rule that every unique index of a scoped table contains the
boundary column holds for a partial index too; the compiler refuses the same
index earlier, so the negative here is written in SQL.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from tests.integration.agnosticism import rosters
from tests.integration.rls.conftest import BootstrapFactory, application_for, scalar

pytestmark = pytest.mark.integration

_SCOPES = '[{"col":"org","scope":"org","on":"both"}]'
_TABLE = "CREATE TABLE {schema}.t (org text NOT NULL, id int, owner boolean, PRIMARY KEY (org, id))"


@asynccontextmanager
async def _connect(url: str) -> AsyncIterator[asyncpg.Connection]:
    conn = await asyncpg.connect(make_url(url).set(drivername="postgresql").render_as_string(False))
    try:
        yield conn
    finally:
        await conn.close()


async def _state(conn: asyncpg.Connection, *statements: str) -> str:
    try:
        async with conn.transaction():
            for statement in statements:
                await conn.execute(statement)
    except asyncpg.PostgresError as exc:
        return exc.sqlstate
    return "none"


def _as_owner(schema: str, *statements: str) -> tuple[str, ...]:
    return (
        f"SET LOCAL ROLE {schema}_owner",
        f"SELECT loom_guard_{schema}.open_hatch()",
        *statements,
    )


def _protect(schema: str) -> str:
    return (
        f"SELECT loom_guard_{schema}.protect_scoped_table('{schema}.t', '{_SCOPES}', "
        "ARRAY['SELECT','INSERT','UPDATE','DELETE'])"
    )


async def test_the_guard_accepts_a_partial_unique_index_that_contains_the_boundary(
    scoped_database: BootstrapFactory,
) -> None:
    schema = "pu_ok"
    database = await scoped_database(schema)
    index = f"CREATE UNIQUE INDEX uq_t_owner ON {schema}.t (org) WHERE owner"
    async with _connect(database.superuser) as conn:
        before_protect = await _state(
            conn, *_as_owner(schema, _TABLE.format(schema=schema), index, _protect(schema))
        )
        after_protect = await _state(
            conn,
            f"SET LOCAL ROLE {schema}_owner",
            f"CREATE UNIQUE INDEX uq_t_owner_id ON {schema}.t (org, id) WHERE owner",
        )

    assert before_protect == "none"
    assert after_protect == "none"


async def test_the_guard_rejects_a_partial_unique_index_without_the_boundary(
    scoped_database: BootstrapFactory,
) -> None:
    schema = "pu_bad"
    database = await scoped_database(schema)
    index = f"CREATE UNIQUE INDEX uq_t_owner ON {schema}.t (id) WHERE owner"
    async with _connect(database.superuser) as conn:
        before_protect = await _state(
            conn, *_as_owner(schema, _TABLE.format(schema=schema), index, _protect(schema))
        )
        protected = await _state(
            conn, *_as_owner(schema, _TABLE.format(schema=schema), _protect(schema))
        )
        after_protect = await _state(conn, f"SET LOCAL ROLE {schema}_owner", index)

    assert before_protect == "LG002"
    assert protected == "none"
    assert after_protect == "LG002"


async def test_create_schema_names_and_enforces_the_declared_constraints(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    from loom.core.repository.sqlalchemy.rls import create_schema, verify

    database = await scoped_database(rosters.SCHEMA)
    application = application_for(rosters, database, tmp_path)

    await create_schema(database.migrator, application)

    report = await verify(database.superuser, application)
    assert report.ok, report.findings
    constraints = (
        "SELECT string_agg(conname, ',' ORDER BY conname) FROM pg_constraint "
        "WHERE connamespace = 'rosters'::regnamespace"
    )
    assert await scalar(database.superuser, constraints) == (
        "ck_seats_status_code,fk_seats_tenant_id_roster_id,pk_rosters,pk_seats,uq_rosters_tenant_id_code"
    )
    indexes = (
        "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes "
        "WHERE schemaname = 'rosters' AND tablename = 'seats'"
    )
    assert await scalar(database.superuser, indexes) == (
        "ix_seats_tenant_id_status_code,pk_seats,uq_seats_owner"
    )
    async with _connect(database.write) as conn:
        scope = "SELECT set_config('loom.scope.tenant', 't1', true)"
        roster = "INSERT INTO rosters.rosters (tenant_id, id, code) VALUES ('t1', 1, 'a')"
        owner = (
            "INSERT INTO rosters.seats (tenant_id, roster_id, status_code, is_owner) "
            "VALUES ('t1', 1, 'active', true)"
        )
        member = (
            "INSERT INTO rosters.seats (tenant_id, roster_id, status_code, is_owner) "
            "VALUES ('t1', 1, 'active', false)"
        )
        unknown_status = (
            "INSERT INTO rosters.seats (tenant_id, roster_id, status_code, is_owner) "
            "VALUES ('t1', 1, 'paused', false)"
        )
        assert await _state(conn, scope, roster, owner, member, member) == "none"
        assert await _state(conn, scope, owner) == "23505"
        assert await _state(conn, scope, unknown_status) == "23514"
