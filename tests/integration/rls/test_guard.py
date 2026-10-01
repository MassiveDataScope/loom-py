"""The per-schema guard against a real Postgres (T013).

Every scenario here is one line of the gate evidence reproduced through
loom's API: the bootstrap is applied with ``apply_bootstrap`` and the guard
functions are exercised exactly as a product would exercise them.
"""

from __future__ import annotations

import asyncpg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.repository.sqlalchemy.rls import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
    SchemaNames,
    apply_bootstrap,
)
from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase, scalar

pytestmark = pytest.mark.integration


async def test_the_bootstrap_applies_and_every_user_logs_in_with_its_scram_password(
    scoped_database: BootstrapFactory,
) -> None:
    database: ScopedDatabase = await scoped_database("guard_a")

    for url in (database.migrator, database.read, database.write, database.bypass):
        assert await scalar(url, "SELECT 1") == 1

    protect = "loom_guard_guard_a.protect_scoped_table(regclass,jsonb,text[])"
    assert await scalar(database.superuser, f"SELECT to_regprocedure('{protect}') IS NOT NULL")
    triggers = "SELECT count(*) FROM pg_event_trigger WHERE evtname LIKE 'loom_guard_guard_a_%'"
    assert await scalar(database.superuser, triggers) == 2


async def test_the_bootstrap_is_idempotent(
    scoped_database: BootstrapFactory, module_database_uri: str
) -> None:
    database = await scoped_database("guard_b")
    roles = "SELECT count(*) FROM pg_roles WHERE rolname LIKE 'guard_b%'"
    before = await scalar(database.superuser, roles)

    await apply_bootstrap(
        module_database_uri,
        BootstrapConfig(
            schema="guard_b",
            roles=DatabaseRoles(owner="guard_b_owner", migrator="guard_b_migrator"),
            database_users={
                "guard_b_ro": DatabaseUser(login=True, access="read"),
                "guard_b_rw": DatabaseUser(login=True, access="write"),
                "guard_b_ops": DatabaseUser(login=True, access="bypass"),
            },
            names=SchemaNames.derived("guard_b"),
        ),
        passwords={},
    )

    assert await scalar(database.superuser, roles) == before
    assert await scalar(database.read, "SELECT 1") == 1


async def test_an_existing_role_with_other_attributes_is_refused(
    module_database_uri: str, created_roles: set[str]
) -> None:
    created_roles.add("guard_c_ro")
    engine = create_async_engine(
        module_database_uri, poolclass=NullPool, isolation_level="AUTOCOMMIT"
    )
    try:
        async with engine.connect() as conn:
            await conn.execute(text("DROP ROLE IF EXISTS guard_c_ro"))
            await conn.execute(text("CREATE ROLE guard_c_ro LOGIN BYPASSRLS"))
    finally:
        await engine.dispose()

    bootstrap_config = BootstrapConfig(
        schema="guard_c",
        roles=DatabaseRoles(owner="guard_c_owner", migrator="guard_c_migrator"),
        database_users={"guard_c_ro": DatabaseUser(login=True, access="read")},
        names=SchemaNames.derived("guard_c"),
    )

    with pytest.raises(asyncpg.PostgresError) as failure:
        await apply_bootstrap(module_database_uri, bootstrap_config, passwords={})

    assert failure.value.sqlstate == "42501"
    assert "different attributes" in str(failure.value)
    guard = "SELECT to_regnamespace('loom_guard_guard_c')"
    assert await scalar(module_database_uri, guard) is None
