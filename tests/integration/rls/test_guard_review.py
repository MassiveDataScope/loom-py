"""Negatives for the decisions taken after the static guard review, against a real Postgres."""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from sqlalchemy import Engine, event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.config import ConfigError
from loom.core.locator import Application
from loom.core.repository.sqlalchemy.migrations.runners import LOCK, UNLOCK
from loom.core.repository.sqlalchemy.rls import (
    BootstrapConfig,
    DatabaseRoles,
    DatabaseUser,
    apply_bootstrap,
    create_schema,
    integrity,
    verify,
)
from loom.core.repository.sqlalchemy.rls.bootstrap import QUIET_LOGS
from loom.core.repository.sqlalchemy.rls.guard_manifest import GUARD_REVISIONS
from loom.core.schema_names import SchemaNames
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
    scalar,
)

pytestmark = pytest.mark.integration

_UNRELEASED = GUARD_REVISIONS[-1].number + 1

SCOPES = '[{"col":"org","scope":"org","on":"both"}]'
PRIVILEGES = "ARRAY['SELECT','INSERT','UPDATE','DELETE']"


def _name() -> str:
    return f"r{secrets.token_hex(3)}"


@asynccontextmanager
async def _connect(url: str) -> AsyncIterator[asyncpg.Connection]:
    raw = make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)
    conn = await asyncpg.connect(raw)
    try:
        yield conn
    finally:
        await conn.close()


async def _state(url: str, *statements: str) -> str:
    async with _connect(url) as conn:
        try:
            async with conn.transaction():
                for statement in statements:
                    await conn.execute(statement)
        except asyncpg.PostgresError as exc:
            return str(exc.sqlstate)
    return "none"


async def _drop_event_triggers(url: str, *schemas: str) -> None:
    await execute(
        url,
        *(
            f"DROP EVENT TRIGGER IF EXISTS loom_guard_{schema}{suffix}"
            for schema in schemas
            for suffix in ("_ddl", "_drop")
        ),
    )


def _users(schema: str, **extra: DatabaseUser) -> dict[str, DatabaseUser]:
    return {
        f"{schema}_ro": DatabaseUser(login=True, access="read"),
        f"{schema}_rw": DatabaseUser(login=True, access="write"),
        f"{schema}_ops": DatabaseUser(login=True, access="bypass"),
        **extra,
    }


def _config(schema: str, **overrides: Any) -> BootstrapConfig:
    values: dict[str, Any] = {
        "schema": schema,
        "roles": DatabaseRoles(owner=f"{schema}_owner", migrator=f"{schema}_migrator"),
        "database_users": _users(schema),
        "names": SchemaNames.derived(schema),
        **overrides,
    }
    return BootstrapConfig(**values)


def _as_owner(schema: str, *statements: str) -> tuple[str, ...]:
    hatch = f"SELECT loom_guard_{schema}.open_hatch()"
    return (f"SET LOCAL ROLE {schema}_owner", hatch, *statements)


def _protected_table(schema: str) -> tuple[str, ...]:
    return _as_owner(
        schema,
        f"CREATE TABLE {schema}.t (org text NOT NULL, PRIMARY KEY (org))",
        f"SELECT loom_guard_{schema}.protect_scoped_table('{schema}.t', '{SCOPES}', {PRIVILEGES})",
    )


async def _guarded_table(scoped_database: BootstrapFactory) -> ScopedDatabase:
    database = await scoped_database(_name())
    assert await _state(database.superuser, *_protected_table(database.schema)) == "none"
    return database


async def _installed(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> tuple[ScopedDatabase, Application]:
    schema = _name()
    database = await scoped_database(schema)
    application = application_for(notes, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    return database, application


async def _startup_checks(url: str, application: Application) -> set[str]:
    bootstrap = application.bootstrap
    assert bootstrap is not None
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            problems = await integrity.startup_problems(
                connection, bootstrap.names.guard, application.scoped
            )
    finally:
        await engine.dispose()
    return {problem.check for problem in problems}


async def _verify_checks(url: str, application: Application) -> set[str]:
    report = await verify(url, application)
    return {finding.check for finding in report.findings}


@pytest.mark.parametrize("stolen", ["migrator", "login user"])
async def test_n1_a_bootstrap_never_adopts_a_role_of_another_schema(
    scoped_database: BootstrapFactory,
    module_database_uri: str,
    created_roles: set[str],
    stolen: str,
) -> None:
    victim = await scoped_database(_name())
    schema = _name()
    created_roles.update({f"{schema}_{suffix}" for suffix in ("owner", "migrator", "readers")})
    created_roles.update({f"{schema}_{suffix}" for suffix in ("writers", "ro", "rw", "ops")})
    if stolen == "migrator":
        foreign = f"{victim.schema}_migrator"
        config = _config(schema, roles=DatabaseRoles(owner=f"{schema}_owner", migrator=foreign))
        victim_url = victim.migrator
    else:
        foreign = f"{victim.schema}_rw"
        config = _config(
            schema, database_users=_users(schema, **{foreign: DatabaseUser(True, "write")})
        )
        victim_url = victim.write

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="never adopts"):
        await apply_bootstrap(module_database_uri, config, {foreign: "taken-over"})

    assert await scalar(victim_url, "SELECT 1") == 1


async def test_n1_an_owner_granted_a_predefined_role_is_refused_on_rerun(
    scoped_database: BootstrapFactory,
) -> None:
    database = await scoped_database(_name())
    await execute(database.superuser, f"GRANT pg_write_server_files TO {database.schema}_owner")

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="pg_write_server_files"):
        await scoped_database(database.schema)


async def test_n1_a_rerun_never_adopts_a_foreign_existing_role(
    scoped_database: BootstrapFactory, module_database_uri: str, created_roles: set[str]
) -> None:
    database = await scoped_database(_name())
    foreign = f"{database.schema}_foreign"
    created_roles.add(foreign)
    await execute(database.superuser, f"CREATE ROLE {foreign} LOGIN")
    config = _config(
        database.schema,
        database_users=_users(database.schema, **{foreign: DatabaseUser(True, "read")}),
    )

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="never adopts"):
        await apply_bootstrap(module_database_uri, config, {})


async def test_n2_unprotect_and_a_hand_policy_fail_at_commit(
    scoped_database: BootstrapFactory,
) -> None:
    database = await _guarded_table(scoped_database)
    schema = database.schema

    outcome = await _state(
        database.superuser,
        *_as_owner(
            schema,
            f"SELECT loom_guard_{schema}.unprotect_scoped_table('{schema}.t')",
            f"CREATE POLICY loose ON {schema}.t USING (true)",
        ),
    )

    assert outcome == "LG002"


async def test_n2_verify_and_startup_report_a_deregistered_table_with_a_hand_policy(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _installed(scoped_database, tmp_path)
    schema, guard, admin = database.schema, f"loom_guard_{database.schema}", database.superuser
    await execute(
        admin,
        f"ALTER EVENT TRIGGER {guard}_ddl DISABLE",
        f"ALTER EVENT TRIGGER {guard}_drop DISABLE",
        f"DELETE FROM {guard}.scoped_policy WHERE rel = '{schema}.notes'::regclass",
        f"DELETE FROM {guard}.scoped_table WHERE rel = '{schema}.notes'::regclass",
        f"DROP POLICY loom_select ON {schema}.notes",
        f"CREATE POLICY loose ON {schema}.notes USING (true)",
        f"ALTER EVENT TRIGGER {guard}_drop ENABLE ALWAYS",
        f"ALTER EVENT TRIGGER {guard}_ddl ENABLE ALWAYS",
    )
    try:
        verified = await _verify_checks(admin, application)
        started = await _startup_checks(database.write, application)
    finally:
        await _drop_event_triggers(admin, schema)

    assert {"assertion", "registry"} <= verified
    assert "table.policies" in started


async def test_n4_a_session_setting_alone_opens_no_hatch(
    scoped_database: BootstrapFactory,
) -> None:
    database = await _guarded_table(scoped_database)
    schema = database.schema

    outcome = await _state(
        database.superuser,
        f"SET LOCAL ROLE {schema}_owner",
        f"SELECT set_config('loom_guard_{schema}.protecting', 'on', true)",
        f"SELECT loom_guard_{schema}.unprotect_scoped_table('{schema}.t')",
    )

    assert outcome == "LG002"


async def test_n4_a_violation_left_under_an_open_hatch_fails_at_commit(
    scoped_database: BootstrapFactory,
) -> None:
    database = await _guarded_table(scoped_database)
    schema = database.schema

    outcome = await _state(
        database.superuser,
        *_as_owner(schema, f"ALTER TABLE {schema}.t NO FORCE ROW LEVEL SECURITY"),
    )

    assert outcome == "LG002"
    forced = f"SELECT relforcerowsecurity FROM pg_class WHERE oid = '{schema}.t'::regclass"
    assert await scalar(database.superuser, forced) is True


async def test_n5_a_broken_schema_blocks_only_its_own_ddl_and_every_grant(
    scoped_database: BootstrapFactory,
) -> None:
    broken = await _guarded_table(scoped_database)
    healthy = await scoped_database(_name())
    a, b = broken.schema, healthy.schema
    await execute(
        broken.superuser,
        f"ALTER EVENT TRIGGER loom_guard_{a}_ddl DISABLE",
        f"ALTER TABLE {a}.t NO FORCE ROW LEVEL SECURITY",
        f"ALTER EVENT TRIGGER loom_guard_{a}_ddl ENABLE ALWAYS",
    )
    try:
        created = await _state(
            healthy.superuser, f"SET LOCAL ROLE {b}_owner", f"CREATE TABLE {b}.plain (id int)"
        )
        granted = await _state(
            healthy.superuser,
            f"SET LOCAL ROLE {b}_owner",
            f"CREATE TABLE {b}.other (id int)",
            f"GRANT SELECT ON {b}.other TO {b}_readers",
        )
    finally:
        await _drop_event_triggers(broken.superuser, a)

    assert created == "none"
    assert granted == "LG002"


async def test_arch3_a_bootstrap_waits_for_a_migration_of_the_same_schema(
    scoped_database: BootstrapFactory, admin_connection: Any
) -> None:
    database = await scoped_database(_name())
    await admin_connection.execute(text(LOCK), {"schema": database.schema})
    rerun = asyncio.create_task(scoped_database(database.schema))
    try:
        await asyncio.sleep(0.5)
        waited = not rerun.done()
    finally:
        await admin_connection.execute(text(UNLOCK), {"schema": database.schema})
    await asyncio.wait_for(rerun, timeout=10)

    assert waited


async def test_arch4_removing_a_declared_user_revokes_what_it_held(
    module_database_uri: str, created_roles: set[str]
) -> None:
    schema = _name()
    writer, bypass = f"{schema}_gone_rw", f"{schema}_gone_ops"
    created_roles.update({writer, bypass, *(f"{schema}_{s}" for s in ("owner", "migrator"))})
    created_roles.update({f"{schema}_{s}" for s in ("readers", "writers", "ro", "rw", "ops")})
    extra = {writer: DatabaseUser(True, "write"), bypass: DatabaseUser(True, "bypass")}
    await apply_bootstrap(
        module_database_uri, _config(schema, database_users=_users(schema, **extra)), {}
    )

    await apply_bootstrap(module_database_uri, _config(schema), {})

    held = (
        f"SELECT pg_has_role('{writer}', '{schema}_readers', 'MEMBER') "
        f"OR pg_has_role('{writer}', '{schema}_writers', 'MEMBER') "
        f"OR has_schema_privilege('{bypass}', '{schema}', 'USAGE')"
    )
    assert await scalar(module_database_uri, held) is False
    await _drop_event_triggers(module_database_uri, schema)


async def test_arch8_startup_refuses_an_owner_trigger_outside_the_guard(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, application = await _installed(scoped_database, tmp_path)
    schema, guard, admin = database.schema, f"loom_guard_{database.schema}", database.superuser
    await execute(
        admin,
        f"ALTER EVENT TRIGGER {guard}_ddl DISABLE",
        f"ALTER EVENT TRIGGER {guard}_drop DISABLE",
        f"CREATE FUNCTION {schema}_owner_dml() RETURNS trigger LANGUAGE plpgsql "
        "AS $$ BEGIN RETURN NULL; END $$",
        f"DROP TRIGGER loom_deny_owner_dml ON {schema}.notes",
        f"CREATE TRIGGER loom_deny_owner_dml BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE "
        f"ON {schema}.notes FOR EACH STATEMENT EXECUTE FUNCTION public.{schema}_owner_dml()",
        f"ALTER TABLE {schema}.notes ENABLE ALWAYS TRIGGER loom_deny_owner_dml",
        f"ALTER EVENT TRIGGER {guard}_drop ENABLE ALWAYS",
        f"ALTER EVENT TRIGGER {guard}_ddl ENABLE ALWAYS",
    )
    try:
        started = await _startup_checks(database.write, application)
    finally:
        await _drop_event_triggers(admin, schema)

    assert "table.owner_trigger" in started


async def test_arch1_a_guard_below_the_minimum_revision_stops_create_schema(
    scoped_database: BootstrapFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    schema = _name()
    database = await scoped_database(schema)
    application = application_for(notes, database, tmp_path, schema=schema)
    monkeypatch.setattr(integrity, "MIN_COMPATIBLE_GUARD_REVISION", _UNRELEASED)

    with pytest.raises(ConfigError, match="guard revision pending"):
        await create_schema(database.migrator, application)


async def test_arch1_startup_reports_a_guard_below_the_minimum_revision(
    scoped_database: BootstrapFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database, application = await _installed(scoped_database, tmp_path)
    monkeypatch.setattr(integrity, "MIN_COMPATIBLE_GUARD_REVISION", _UNRELEASED)

    assert "guard.revision" in await _startup_checks(database.write, application)


async def test_n7_scram_iterations_reach_the_stored_verifier(
    module_database_uri: str, created_roles: set[str]
) -> None:
    schema = _name()
    created_roles.update({f"{schema}_{s}" for s in ("owner", "migrator", "readers", "writers")})
    created_roles.update({f"{schema}_{s}" for s in ("ro", "rw", "ops")})
    password = secrets.token_urlsafe(12)

    await apply_bootstrap(
        module_database_uri, _config(schema, scram_iterations=8192), {f"{schema}_ro": password}
    )

    stored = f"SELECT rolpassword FROM pg_authid WHERE rolname = '{schema}_ro'"
    assert str(await scalar(module_database_uri, stored)).startswith("SCRAM-SHA-256$8192:")
    login = make_url(module_database_uri).set(username=f"{schema}_ro", password=password)
    assert await scalar(login.render_as_string(hide_password=False), "SELECT 1") == 1
    await _drop_event_triggers(module_database_uri, schema)


async def test_n7_the_bootstrap_quiets_logging_before_anything_else(
    scoped_database: BootstrapFactory,
) -> None:
    statements: list[str] = []

    def record(_conn: Any, _cursor: Any, statement: str, *_: Any) -> None:
        statements.append(statement)

    event.listen(Engine, "before_cursor_execute", record)
    try:
        await scoped_database(_name())
    finally:
        event.remove(Engine, "before_cursor_execute", record)

    assert statements[0] == QUIET_LOGS


async def test_n10_a_rerun_with_another_version_table_is_refused(
    scoped_database: BootstrapFactory, module_database_uri: str
) -> None:
    database = await scoped_database(_name())
    names = SchemaNames.derived(database.schema)
    config = _config(
        database.schema,
        names=SchemaNames(
            guard=names.guard,
            readers=names.readers,
            writers=names.writers,
            version_table="other_version",
            data_version_table=names.data_version_table,
        ),
    )

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="configured for another"):
        await apply_bootstrap(module_database_uri, config, {})
