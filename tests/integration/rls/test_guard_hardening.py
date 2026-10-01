"""Guard hardening from the T024 security review, each as a negative against a real Postgres."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from loom.core.repository.sqlalchemy.rls import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
    render_bootstrap,
)
from tests.integration.rls.conftest import BootstrapFactory

pytestmark = pytest.mark.integration


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
        f"SELECT set_config('loom_guard_{schema}.protecting', 'on', true)",
        *statements,
    )


def _protect(schema: str, table: str, scopes: str) -> str:
    return (
        f"SELECT loom_guard_{schema}.protect_scoped_table('{schema}.{table}', '{scopes}', "
        "ARRAY['SELECT','INSERT','UPDATE','DELETE'])"
    )


async def test_a_session_value_longer_than_a_varchar_boundary_never_matches_a_shorter_one(
    scoped_database: BootstrapFactory,
) -> None:
    schema = "hard_trunc"
    database = await scoped_database(schema)
    scopes = '[{"col":"org","scope":"org","on":"both"}]'
    async with _connect(database.superuser) as conn:
        created = await _state(
            conn,
            *_as_owner(
                schema,
                f"CREATE TABLE {schema}.t (org varchar(8) NOT NULL, v text, PRIMARY KEY (org))",
                _protect(schema, "t", scopes),
            ),
        )
        assert created == "none"
        await conn.execute(f"SET ROLE {schema}_ops")
        await conn.execute(f"INSERT INTO {schema}.t VALUES ('acme1234', 'secret')")
        await conn.execute(f"RESET ROLE; SET ROLE {schema}_rw")
        async with conn.transaction():
            await conn.execute("SELECT set_config('loom.scope.org', 'acme1234-x', true)")
            assert await conn.fetchval(f"SELECT count(*) FROM {schema}.t") == 0


async def test_a_blank_padded_boundary_is_refused(scoped_database: BootstrapFactory) -> None:
    schema = "hard_bpchar"
    database = await scoped_database(schema)
    scopes = '[{"col":"org","scope":"org","on":"both"}]'
    async with _connect(database.superuser) as conn:
        outcome = await _state(
            conn,
            *_as_owner(
                schema,
                f"CREATE TABLE {schema}.t (org char(8) NOT NULL, PRIMARY KEY (org))",
                _protect(schema, "t", scopes),
            ),
        )

    assert outcome == "22023"


async def test_a_boundary_only_in_include_does_not_satisfy_unique_containment(
    scoped_database: BootstrapFactory,
) -> None:
    schema = "hard_incl"
    database = await scoped_database(schema)
    scopes = '[{"col":"org","scope":"org","on":"both"}]'
    table = f"CREATE TABLE {schema}.t (org text NOT NULL, email text, PRIMARY KEY (org, email))"
    async with _connect(database.superuser) as conn:
        before_protect = await _state(
            conn,
            *_as_owner(
                schema,
                table,
                f"CREATE UNIQUE INDEX t_email ON {schema}.t (email) INCLUDE (org)",
                _protect(schema, "t", scopes),
            ),
        )
        protected = await _state(conn, *_as_owner(schema, table, _protect(schema, "t", scopes)))
        after_protect = await _state(
            conn,
            f"SET LOCAL ROLE {schema}_owner",
            f"CREATE UNIQUE INDEX t_email ON {schema}.t (email) INCLUDE (org)",
        )

    assert before_protect == "LG002"
    assert protected == "none"
    assert after_protect == "LG002"


async def test_an_application_user_that_is_a_member_of_the_owner_is_detected(
    scoped_database: BootstrapFactory,
) -> None:
    schema = "hard_member"
    database = await scoped_database(schema)
    scopes = '[{"col":"org","scope":"org","on":"both"}]'
    async with _connect(database.superuser) as conn:
        await _state(
            conn,
            *_as_owner(
                schema,
                f"CREATE TABLE {schema}.t (org text NOT NULL, PRIMARY KEY (org))",
                _protect(schema, "t", scopes),
            ),
        )
        detected = await _state(
            conn,
            f"GRANT {schema}_owner TO {schema}_rw",
            f"SELECT loom_guard_{schema}.assert_scoped_schema()",
        )
        truncated = await _state(
            conn,
            f"GRANT {schema}_owner TO {schema}_rw",
            f"SET LOCAL ROLE {schema}_rw",
            f"TRUNCATE {schema}.t",
        )

    assert detected == "LG002"
    assert truncated == "LG001"


async def test_unprotect_requires_the_hatch(scoped_database: BootstrapFactory) -> None:
    schema = "hard_unprot"
    database = await scoped_database(schema)
    scopes = '[{"col":"org","scope":"org","on":"both"}]'
    async with _connect(database.superuser) as conn:
        await _state(
            conn,
            *_as_owner(
                schema,
                f"CREATE TABLE {schema}.t (org text NOT NULL, PRIMARY KEY (org))",
                _protect(schema, "t", scopes),
            ),
        )
        without_hatch = await _state(
            conn,
            f"SET LOCAL ROLE {schema}_owner",
            f"SELECT loom_guard_{schema}.unprotect_scoped_table('{schema}.t')",
        )

    assert without_hatch == "LG002"


@pytest.mark.parametrize("schema", ["a" * 48, "Notes", "user", "current_user"])
def test_schema_names_that_postgres_would_truncate_fold_or_reserve_are_refused(
    schema: str,
) -> None:
    config = BootstrapConfig(
        schema=schema,
        roles=DatabaseRoles(owner="o", migrator="m"),
        database_users={"u": DatabaseUser(login=True, access="read")},
    )

    with pytest.raises(ValueError, match="identifier"):
        render_bootstrap(config)


@pytest.mark.parametrize("user", ["current_user", "session_user", "Public"])
def test_database_user_names_that_postgres_resolves_specially_are_refused(user: str) -> None:
    config = BootstrapConfig(
        schema="app",
        roles=DatabaseRoles(owner="o", migrator="m"),
        database_users={user: DatabaseUser(login=True, access="read")},
    )

    with pytest.raises(ValueError, match="identifier"):
        render_bootstrap(config)
