"""The guard is static, identical for every schema, and its tampering is detected.

Every test installs its own copy of the synthetic ``notes`` product under a
fresh schema name and drops that copy's event triggers afterwards, so a
deliberately broken guard never blocks the other tests of the module.
"""

from __future__ import annotations

import secrets
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.locator import Application
from loom.core.repository.sqlalchemy.rls import create_schema, verify
from loom.core.repository.sqlalchemy.rls.integrity import guard_problems
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase, application_for

pytestmark = pytest.mark.integration

PROTECT = "protect_scoped_table(regclass, jsonb, text[])"


@dataclass(frozen=True, slots=True)
class Installed:
    schema: str
    guard: str
    database: ScopedDatabase
    application: Application


def _raw(url: str) -> str:
    return make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)


async def _run(url: str, *statements: str) -> None:
    connection = await asyncpg.connect(_raw(url))
    try:
        async with connection.transaction():
            for statement in statements:
                await connection.execute(statement)
    finally:
        await connection.close()


async def _fetch(url: str, query: str) -> list[asyncpg.Record]:
    connection = await asyncpg.connect(_raw(url))
    try:
        return await connection.fetch(query)
    finally:
        await connection.close()


async def _install(
    scoped_database: BootstrapFactory, tmp_path: Path, schema: str | None = None
) -> Installed:
    name = schema or f"n{secrets.token_hex(3)}"
    database = await scoped_database(name)
    application = application_for(notes, database, tmp_path, schema=name)
    await create_schema(database.migrator, application)
    return Installed(name, f"loom_guard_{name}", database, application)


@pytest.fixture
async def installed(scoped_database: BootstrapFactory, tmp_path: Path) -> AsyncIterator[Installed]:
    product = await _install(scoped_database, tmp_path)
    yield product
    await _run(
        product.database.superuser,
        f"DROP EVENT TRIGGER IF EXISTS {product.guard}_ddl",
        f"DROP EVENT TRIGGER IF EXISTS {product.guard}_drop",
    )


async def _checks(product: Installed) -> set[str]:
    report = await verify(product.database.superuser, product.application)
    return {finding.check for finding in report.findings}


async def _startup_problems(product: Installed) -> set[str]:
    engine = create_async_engine(product.database.write, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            problems = await guard_problems(connection, product.guard)
    finally:
        await engine.dispose()
    return {problem.check for problem in problems}


def _installing(guard: str) -> str:
    return f"SELECT set_config('{guard}.installing', 'on', true)"


async def test_every_schema_gets_a_byte_identical_guard(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    first = await _install(scoped_database, tmp_path)
    second = await _install(scoped_database, tmp_path)
    try:
        sources = [
            await _fetch(
                first.database.superuser,
                "SELECT p.proname, p.prosrc FROM pg_proc p JOIN pg_namespace n "
                f"ON n.oid = p.pronamespace WHERE n.nspname = '{product.guard}' ORDER BY 1, 2",
            )
            for product in (first, second)
        ]
        assert [tuple(row) for row in sources[0]] == [tuple(row) for row in sources[1]]
        assert await _checks(first) == set()
        assert await _checks(second) == set()
    finally:
        for product in (first, second):
            await _run(
                product.database.superuser,
                f"DROP EVENT TRIGGER IF EXISTS {product.guard}_ddl",
                f"DROP EVENT TRIGGER IF EXISTS {product.guard}_drop",
            )


async def test_the_owner_of_one_schema_cannot_reach_another_guard(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    first = await _install(scoped_database, tmp_path)
    second = await _install(scoped_database, tmp_path)
    try:
        reach = await _fetch(
            first.database.migrator,
            f"SELECT has_schema_privilege(current_user, '{second.guard}', 'USAGE')",
        )
        assert reach[0][0] is False
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await _run(
                first.database.migrator,
                f"SELECT {second.guard}.protect_scoped_table("
                f"'{first.schema}.notes'::regclass, '[]'::jsonb, ARRAY[]::text[])",
            )
    finally:
        for product in (first, second):
            await _run(
                product.database.superuser,
                f"DROP EVENT TRIGGER IF EXISTS {product.guard}_ddl",
                f"DROP EVENT TRIGGER IF EXISTS {product.guard}_drop",
            )


@pytest.mark.parametrize(
    "statement",
    [
        "CREATE OR REPLACE FUNCTION {guard}.hatch_open() RETURNS boolean "
        "LANGUAGE sql STABLE AS $$ SELECT true $$",
        "ALTER FUNCTION {guard}." + PROTECT + " SECURITY INVOKER",
        "CREATE TABLE {guard}.extra (x int)",
    ],
    ids=["replace body", "alter function", "extra table"],
)
async def test_ddl_on_the_guard_outside_the_installer_fails_in_the_same_statement(
    installed: Installed, statement: str
) -> None:
    with pytest.raises(asyncpg.PostgresError, match="outside the loom installer"):
        await _run(installed.database.superuser, statement.format(guard=installed.guard))


@pytest.mark.parametrize(
    ("statements", "check", "visible_at_startup"),
    [
        (
            [
                "{installing}",
                "CREATE OR REPLACE FUNCTION {guard}.hatch_open() RETURNS boolean LANGUAGE sql "
                "STABLE SET search_path FROM CURRENT AS $$ SELECT true $$",
            ],
            "guard.functions",
            True,
        ),
        (
            ["{installing}", "ALTER FUNCTION {guard}." + PROTECT + " SECURITY INVOKER"],
            "guard.functions",
            True,
        ),
        (
            ["{installing}", "ALTER FUNCTION {guard}." + PROTECT + " SET search_path = public"],
            "guard.function_config",
            True,
        ),
        (
            ["{installing}", "ALTER FUNCTION {guard}." + PROTECT + " OWNER TO {schema}_owner"],
            "guard.function_owner",
            True,
        ),
        (
            ["GRANT EXECUTE ON FUNCTION {guard}." + PROTECT + " TO PUBLIC"],
            "guard.function_grants",
            True,
        ),
        (
            ["REVOKE EXECUTE ON FUNCTION {guard}." + PROTECT + " FROM {schema}_owner"],
            "guard.function_grants",
            False,
        ),
        (
            [
                "{installing}",
                "CREATE FUNCTION {guard}.extra() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$",
            ],
            "guard.functions",
            True,
        ),
        (["{installing}", "CREATE TABLE {guard}.extra (x int)"], "guard.relations", False),
        (["GRANT CREATE ON SCHEMA {guard} TO PUBLIC"], "guard.schema_grants", False),
        (
            [
                "{installing}",
                "CREATE FUNCTION {guard}.noop() RETURNS trigger LANGUAGE plpgsql "
                "AS $$ BEGIN RETURN NULL; END $$",
                "CREATE TRIGGER t AFTER INSERT ON {guard}.config FOR EACH STATEMENT "
                "EXECUTE FUNCTION {guard}.noop()",
            ],
            "guard.relation",
            False,
        ),
        (
            ["UPDATE {guard}.config SET bypass_roles = array_append(bypass_roles, '{schema}_rw')"],
            "guard.config",
            False,
        ),
        (["ALTER EVENT TRIGGER {guard}_ddl DISABLE"], "guard.event_triggers", True),
        (["ALTER EVENT TRIGGER {guard}_ddl ENABLE REPLICA"], "guard.event_triggers", True),
        (
            [
                "DROP EVENT TRIGGER {guard}_ddl",
                "CREATE EVENT TRIGGER {guard}_ddl ON ddl_command_end WHEN TAG IN ('CREATE INDEX') "
                "EXECUTE FUNCTION {guard}.on_ddl_end()",
                "ALTER EVENT TRIGGER {guard}_ddl ENABLE ALWAYS",
            ],
            "guard.event_triggers",
            True,
        ),
    ],
    ids=[
        "replaced body",
        "security invoker",
        "search path",
        "owner",
        "execute to public",
        "execute revoked from the owner",
        "extra function",
        "extra table",
        "create on the guard schema",
        "trigger on the registry",
        "configuration row",
        "event trigger disabled",
        "event trigger replica only",
        "event trigger filtered by tag",
    ],
)
async def test_tampering_with_the_guard_is_reported(
    installed: Installed, statements: Sequence[str], check: str, visible_at_startup: bool
) -> None:
    values = {
        "guard": installed.guard,
        "schema": installed.schema,
        "installing": _installing(installed.guard),
    }
    await _run(installed.database.superuser, *(s.format(**values) for s in statements))

    assert check in await _checks(installed)
    if visible_at_startup:
        assert check in await _startup_problems(installed)


async def test_the_bootstrap_never_adopts_a_guard_schema_it_did_not_create(
    scoped_database: BootstrapFactory, module_database_uri: str, created_roles: set[str]
) -> None:
    name = f"n{secrets.token_hex(3)}"
    squatter = f"{name}_squatter"
    created_roles.add(squatter)
    await _run(
        module_database_uri,
        f"CREATE ROLE {squatter} NOLOGIN",
        f"CREATE SCHEMA loom_guard_{name} AUTHORIZATION {squatter}",
    )

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="never adopts"):
        await scoped_database(name)


async def test_the_bootstrap_never_adopts_an_application_schema_owned_by_someone_else(
    scoped_database: BootstrapFactory, module_database_uri: str, created_roles: set[str]
) -> None:
    name = f"n{secrets.token_hex(3)}"
    squatter = f"{name}_squatter"
    created_roles.add(squatter)
    await _run(
        module_database_uri,
        f"CREATE ROLE {squatter} NOLOGIN",
        f"CREATE SCHEMA {name} AUTHORIZATION {squatter}",
    )

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="never adopts"):
        await scoped_database(name)
    await _run(
        module_database_uri,
        f"DROP EVENT TRIGGER IF EXISTS loom_guard_{name}_ddl",
        f"DROP EVENT TRIGGER IF EXISTS loom_guard_{name}_drop",
    )


async def test_a_password_must_be_a_verifier_and_is_never_echoed(installed: Installed) -> None:
    with pytest.raises(asyncpg.InvalidParameterValueError) as raised:
        await _run(
            installed.database.superuser,
            f"SELECT {installed.guard}.set_password_verifier("
            f"'{installed.schema}_rw', 'plain-secret')",
        )

    assert "plain-secret" not in str(raised.value)


async def test_a_password_is_set_only_for_a_login_user_of_the_schema(
    installed: Installed,
) -> None:
    verifier = "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy"

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="not a login user"):
        await _run(
            installed.database.superuser,
            f"SELECT {installed.guard}.set_password_verifier("
            f"'{installed.schema}_owner', '{verifier}')",
        )


async def test_the_owner_cannot_configure_the_guard_or_set_passwords(
    installed: Installed,
) -> None:
    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        await _run(
            installed.database.migrator, f"SELECT {installed.guard}.configure('{{}}'::jsonb)"
        )
