"""The guard installs, pins and grants the audit reader (spec 009, US1, FR-002 to FR-004, FR-012).

Revision 4 records the ``audit_reading`` flag and the boundary ``B`` in the
guard config, and the owner step's ``apply_audit_reading()`` installs
``<schema>.read_audit_log`` locked down, granting it to the readers group only
while the flag is on. The bootstrap document's reading keys reach
``configure`` through the fixture's *document* option; the owner step is
called by hand after ``create_schema``, so these tests depend on the guard
revision only. ``read_audit_log`` is called positionally, as the adapter does.

US3 #15 and #16 (the auditor keeps its full read, the application has no
``SELECT`` on the log) are not rewritten: ``test_audit_reader.py`` runs as
their regression (review N21).
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import ModuleType

import asyncpg
import msgspec
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.config import ConfigError
from loom.core.locator import Application
from loom.core.model import Audited, BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer, String, Text
from loom.core.repository.sqlalchemy.rls import create_schema, guard_manifest
from loom.core.repository.sqlalchemy.rls.guard_manifest import (
    AUDIT_GUARD_REVISION,
    AUDIT_LOG_SHAPE,
)
from loom.core.repository.sqlalchemy.rls.integrity import AUDIT_SHAPE
from tests.integration.agnosticism import bookkeeping, devices
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    scalar,
)

pytestmark = pytest.mark.integration

# The multi-boundary product (FR-012) is this module: two boundary scopes, one audited table.
SCOPE_BINDINGS = {"tenant": "identity.subject", "region": "identity.region"}


class _Ticket(BaseModel, RowScoped, Audited):
    __tablename__ = "audit_read_guard_tickets"

    tenant: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True)
    body: str = ColumnField(Text, default="")


class _Site(BaseModel, RowScoped):
    __tablename__ = "audit_read_guard_sites"

    region: int = ScopedField(Integer, primary_key=True, scope="region")
    id: int = ColumnField(Integer, primary_key=True)


MODELS = (_Ticket, _Site)
MIXED = sys.modules[__name__]

TENANT = {"boundary_scope": "tenant", "boundary_type": "character varying"}
READING_ON = {"audit_reading": True, **TENANT}
READING_OFF = {"audit_reading": False}
ARGUMENTS = (
    "p_all boolean, p_subjects text[], p_cap integer, "
    "p_from timestamp with time zone, p_to timestamp with time zone, "
    "p_subject text, p_table text, p_tables text[], p_row_key jsonb, p_trace_id text, "
    "p_op text, p_after_at timestamp with time zone, p_after_id bigint, "
    "p_ascending boolean, p_limit integer"
)
RESULT = (
    "TABLE(id bigint, at timestamp with time zone, op text, schema_name text, "
    "table_name text, row_key text, changed jsonb, subject text, mechanism text, "
    "on_behalf_of text, trace_id text, elevated text[], origin text)"
)
READ_ALL = (
    "SELECT op, table_name FROM {schema}.read_audit_log(true, NULL::text[], 1, "
    "NULL::timestamptz, NULL::timestamptz, NULL::text, NULL::text, NULL::text[], NULL::jsonb, "
    "NULL::text, NULL::text, NULL::timestamptz, NULL::bigint, false, 101)"
)
VIOLATION = "LG002"
INVALID = "22023"
REFUSED = "42501"


@asynccontextmanager
async def _connect(url: str) -> AsyncIterator[asyncpg.Connection]:
    raw = make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)
    connection = await asyncpg.connect(raw)
    try:
        yield connection
    finally:
        await connection.close()


async def _run(url: str, *statements: str) -> None:
    """Run *statements* in one transaction as the user in *url*."""
    async with _connect(url) as connection, connection.transaction():
        for statement in statements:
            await connection.execute(statement)


async def _refusal(url: str, *statements: str) -> tuple[str, str]:
    """The SQLSTATE and message *statements* fail with, or ``("none", "")``."""
    try:
        await _run(url, *statements)
    except asyncpg.PostgresError as exc:
        return str(getattr(exc, "sqlstate", "")), str(exc)
    return "none", ""


async def _apply_reading(database: ScopedDatabase) -> None:
    """The owner step: ``apply_audit_reading()`` in one deploy transaction, as the migrator."""
    await _run(database.migrator, f"SELECT loom_guard_{database.schema}.apply_audit_reading()")


async def _deployed(
    factory: BootstrapFactory,
    tmp_path: Path,
    schema: str,
    *,
    document: dict[str, object],
    product: ModuleType = bookkeeping,
) -> tuple[ScopedDatabase, Application]:
    """Bootstrap *schema* with *document*, create *product*'s tables and run the owner step."""
    database = await factory(schema, document=document)
    application = application_for(product, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    await _apply_reading(database)
    return database, application


def _reader(schema: str) -> str:
    """The reader's signature by argument types, the form ``to_regprocedure`` accepts."""
    types = ", ".join(argument.split(" ", 1)[1] for argument in ARGUMENTS.split(", "))
    return f"{schema}.read_audit_log({types})"


async def _config(database: ScopedDatabase, column: str) -> object:
    return await scalar(
        database.superuser, f"SELECT {column} FROM loom_guard_{database.schema}.config"
    )


async def _acl(database: ScopedDatabase) -> list[str]:
    """``grantee:privilege:grantable`` of every ACL entry of the reader, sorted."""
    query = (
        "SELECT coalesce(array_agg(coalesce(g.rolname, 'PUBLIC') || ':' || a.privilege_type "
        "|| ':' || a.is_grantable::text ORDER BY 1), '{}') "
        "FROM pg_proc p CROSS JOIN LATERAL aclexplode(p.proacl) a "
        "LEFT JOIN pg_roles g ON g.oid = a.grantee "
        f"WHERE p.oid = to_regprocedure('{_reader(database.schema)}')"
    )
    return list(await scalar(database.superuser, query))


async def _readers(database: ScopedDatabase) -> int:
    query = (
        "SELECT count(*) FROM pg_proc "
        f"WHERE proname = 'read_audit_log' AND pronamespace = '{database.schema}'::regnamespace"
    )
    return int(await scalar(database.superuser, query))


async def _can_execute(database: ScopedDatabase, user: str) -> bool:
    query = (
        f"SELECT has_function_privilege('{user}', "
        f"to_regprocedure('{_reader(database.schema)}'), 'EXECUTE')"
    )
    return bool(await scalar(database.superuser, query))


async def _read_as(url: str, schema: str, tenant: str) -> list[tuple[str, str]]:
    async with _connect(url) as connection, connection.transaction():
        await connection.execute(f"SELECT set_config('loom.scope.tenant', '{tenant}', true)")
        rows = await connection.fetch(READ_ALL.format(schema=schema))
    return [(row["op"], row["table_name"]) for row in rows]


def _insert_account(schema: str, account: int) -> str:
    return (
        f"INSERT INTO {schema}.accounts (tenant, id, name, email, balance, currency, active, "
        f"internal_note, api_token) VALUES ('acme', {account}, 'Ana', 'a', 0, 'EUR', true, '', '')"
    )


def _users(schema: str) -> dict[str, str]:
    return {"read": f"{schema}_ro", "write": f"{schema}_rw", "bypass": f"{schema}_ops"}


@pytest.fixture(scope="module")
def anyio_backend() -> str:
    return "asyncio"


async def test_without_reading_no_application_user_may_execute_the_reader(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US1 #1, FR-003: the owner step installs the reader for every log, granted to nobody."""
    database, _ = await _deployed(scoped_database, tmp_path, "rg_off", document={})

    assert await _config(database, "audit_reading") is False
    assert await _readers(database) == 1
    assert {who: await _can_execute(database, user) for who, user in _users("rg_off").items()} == {
        "read": False,
        "write": False,
        "bypass": False,
    }
    assert await _acl(database) == ["rg_off_owner:EXECUTE:false"]
    assert (await _refusal(database.write, READ_ALL.format(schema="rg_off")))[0] == REFUSED


async def test_with_reading_the_owner_step_installs_locked_down_then_grants_the_readers(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US1 #2, FR-002: one call installs, revokes PUBLIC, and grants only the readers group."""
    database, _ = await _deployed(scoped_database, tmp_path, "rg_on", document=READING_ON)

    assert await _config(database, "audit_reading") is True
    assert await _acl(database) == ["rg_on_owner:EXECUTE:false", "rg_on_readers:EXECUTE:false"]
    assert {who: await _can_execute(database, user) for who, user in _users("rg_on").items()} == {
        "read": True,
        "write": True,
        "bypass": False,
    }
    assert await _refusal(database.write, READ_ALL.format(schema="rg_on")) == ("none", "")


async def test_revision_three_installs_no_reader(
    scoped_database: BootstrapFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """US1 #3: this release ships a later revision; a database left at 3 has no reader."""
    assert guard_manifest.GUARD_REVISIONS[-1].number > AUDIT_GUARD_REVISION
    revisions = guard_manifest.GUARD_REVISIONS[:AUDIT_GUARD_REVISION]
    monkeypatch.setattr(guard_manifest, "GUARD_REVISIONS", revisions)
    database = await scoped_database("rg_rev3")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema="rg_rev3")
    )

    guard_function = "SELECT to_regproc('loom_guard_rg_rev3.apply_audit_reading')::text"
    applied = "SELECT string_agg(n::text, ',' ORDER BY n) FROM loom_guard_rg_rev3.revision"
    assert await scalar(database.superuser, applied) == "1,2,3"
    assert await _readers(database) == 0
    assert await scalar(database.superuser, guard_function) is None


async def test_turning_reading_off_revokes_and_the_deploy_completes(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US1 #4, FR-002, review M1: ``configure`` revokes before it writes the flag."""
    database, application = await _deployed(
        scoped_database, tmp_path, "rg_revoke", document=READING_ON
    )
    assert await _can_execute(database, "rg_revoke_rw")

    database = await scoped_database("rg_revoke", document=READING_OFF)
    assert await _config(database, "audit_reading") is False
    assert await _acl(database) == ["rg_revoke_owner:EXECUTE:false"]

    await create_schema(database.migrator, application)
    await _apply_reading(database)
    assert await _readers(database) == 1
    assert await _acl(database) == ["rg_revoke_owner:EXECUTE:false"]


async def test_the_reader_is_pinned_as_released(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """FR-002, FR-003: definer, stable, pinned search_path, owner, arguments, source and ACL."""
    database, _ = await _deployed(scoped_database, tmp_path, "rg_pins", document=READING_ON)
    reader = _reader("rg_pins")
    pins = (
        "SELECT p.prosecdef, p.provolatile::text, p.proconfig, r.rolname::text, "
        "pg_get_function_identity_arguments(p.oid), pg_get_function_result(p.oid), "
        "p.prosrc = loom_guard_rg_pins.audit_reader_source(), p.proacl IS NULL "
        "FROM pg_proc p JOIN pg_roles r ON r.oid = p.proowner "
        f"WHERE p.oid = to_regprocedure('{reader}')"
    )

    async with _connect(database.superuser) as connection:
        row = await connection.fetchrow(pins)

    assert row is not None, f"{reader} is not installed"
    assert tuple(row) == (
        True,
        "s",
        ["search_path=pg_catalog, rg_pins, pg_temp"],
        "rg_pins_owner",
        ARGUMENTS,
        RESULT,
        True,
        False,
    )
    assert "PUBLIC:EXECUTE:false" not in await _acl(database)


async def test_the_log_keeps_its_contracted_shape_and_grants(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """FR-004, NFR-002: revision 4 and the reader leave ``AUDIT_LOG_SHAPE`` and the log ACL."""
    database, _ = await _deployed(scoped_database, tmp_path, "rg_shape", document=READING_ON)
    acl = (
        "SELECT array_agg(DISTINCT coalesce(g.rolname, 'PUBLIC') || ':' || a.privilege_type) "
        "FROM pg_class c CROSS JOIN LATERAL aclexplode(c.relacl) a "
        "LEFT JOIN pg_roles g ON g.oid = a.grantee "
        "WHERE c.oid = 'rg_shape.audit_log'::regclass AND a.grantee <> c.relowner"
    )

    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(text(AUDIT_SHAPE), {"schema": "rg_shape"})
            shape = tuple(str(row[0]) for row in rows)
    finally:
        await engine.dispose()

    assert shape == AUDIT_LOG_SHAPE
    assert await scalar(database.superuser, acl) == ["rg_shape_auditor:SELECT"]


@pytest.mark.parametrize("grantee", ["PUBLIC", "rg_grantee_ops", "rg_grantee_writers"])
async def test_an_extra_execute_grantee_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path, grantee: str
) -> None:
    """SC-003, FR-002: only the readers group may hold ``EXECUTE`` on the reader."""
    database, _ = await _deployed(scoped_database, tmp_path, "rg_grantee", document=READING_ON)

    state, _ = await _refusal(
        database.migrator, f"GRANT EXECUTE ON FUNCTION {_reader('rg_grantee')} TO {grantee}"
    )

    assert state == VIOLATION


async def test_a_grant_with_grant_option_to_the_readers_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """SC-003, FR-002: the readers group holds ``EXECUTE`` without the grant option."""
    database, _ = await _deployed(scoped_database, tmp_path, "rg_option", document=READING_ON)

    state, _ = await _refusal(
        database.migrator,
        f"GRANT EXECUTE ON FUNCTION {_reader('rg_option')} TO rg_option_readers WITH GRANT OPTION",
    )

    assert state == VIOLATION


async def test_the_flag_without_a_grant_is_allowed(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """FR-002, M1: the flag never requires a grant; before the owner step nothing is refused."""
    await _deployed(scoped_database, tmp_path, "rg_flag", document={})

    await scoped_database("rg_flag", document=READING_ON)
    again = await scoped_database("rg_flag", document=READING_ON)

    assert await _config(again, "audit_reading") is True
    assert await _acl(again) == ["rg_flag_owner:EXECUTE:false"]


async def test_a_grant_without_the_reading_flag_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """FR-002: a grant implies the flag; the readers group gets nothing while reading is off."""
    database, _ = await _deployed(scoped_database, tmp_path, "rg_noflag", document={})

    state, _ = await _refusal(
        database.migrator,
        f"GRANT EXECUTE ON FUNCTION {_reader('rg_noflag')} TO rg_noflag_readers",
    )

    assert state == VIOLATION


@pytest.mark.parametrize(
    "drift",
    [
        "body",
        "ALTER FUNCTION {reader} SECURITY INVOKER",
        "ALTER FUNCTION {reader} VOLATILE",
        "ALTER FUNCTION {reader} SET search_path = pg_catalog, public, pg_temp",
        "CREATE FUNCTION {schema}.read_audit_log(p integer) RETURNS void LANGUAGE sql AS ''",
    ],
)
async def test_drift_in_the_reader_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path, drift: str
) -> None:
    """SC-003, FR-003: the owner cannot change the reader's body, settings or overloads."""
    database, _ = await _deployed(scoped_database, tmp_path, "rg_drift", document=READING_ON)
    reader = _reader("rg_drift")
    if drift == "body":
        definition = str(
            await scalar(
                database.superuser, f"SELECT pg_get_functiondef(to_regprocedure('{reader}'))"
            )
        )
        statement = definition.replace("AS $function$", "AS $function$\n-- drift", 1)
    else:
        statement = drift.format(reader=reader, schema="rg_drift")

    state, _ = await _refusal(database.migrator, statement)

    assert state == VIOLATION


async def test_the_boundary_is_recorded_only_with_reading_on(
    scoped_database: BootstrapFactory,
) -> None:
    """FR-002, M2: with reading off ``configure`` records no ``B``; with it on, ``B``."""
    off = await scoped_database("rg_b_record", document={**READING_OFF, **TENANT})
    assert (await _config(off, "boundary_scope"), await _config(off, "boundary_type")) == (
        None,
        None,
    )

    on = await scoped_database("rg_b_record", document=READING_ON)
    assert (await _config(on, "boundary_scope"), await _config(on, "boundary_type")) == (
        "tenant",
        "character varying",
    )


@pytest.mark.parametrize(
    ("schema", "scope", "type_name"),
    [("rg_b_name", "region", "integer"), ("rg_b_type", "tenant", "text")],
    ids=["other-name", "same-name-other-type"],
)
async def test_the_recorded_boundary_never_changes(
    scoped_database: BootstrapFactory, schema: str, scope: str, type_name: str
) -> None:
    """SC-003, M2: once recorded, ``B`` (name and type) changes only by disabling and clearing."""
    recorded = await scoped_database(schema, document=READING_ON)
    assert await _config(recorded, "boundary_scope") == "tenant"
    changed = {"audit_reading": True, "boundary_scope": scope, "boundary_type": type_name}

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="changes only by disabling"):
        await scoped_database(schema, document=changed)

    kept = await scoped_database(schema, document=READING_OFF)
    assert (await _config(kept, "boundary_scope"), await _config(kept, "boundary_type")) == (
        "tenant",
        "character varying",
    )


async def test_the_boundary_is_cleared_only_with_reading_off(
    scoped_database: BootstrapFactory,
) -> None:
    """M2: ``clear_audit_boundary`` is refused while reading is on and clears ``B`` when off."""
    recorded = await scoped_database("rg_b_clear", document=READING_ON)
    assert await _config(recorded, "boundary_scope") == "tenant"

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="before clearing its boundary"):
        await scoped_database("rg_b_clear", document={**READING_ON, "clear_audit_boundary": True})

    cleared = await scoped_database(
        "rg_b_clear", document={**READING_OFF, "clear_audit_boundary": True}
    )
    assert (await _config(cleared, "boundary_scope"), await _config(cleared, "boundary_type")) == (
        None,
        None,
    )


async def test_the_bootstrap_refuses_a_multi_boundary_store_with_reading_on(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """FR-012: the bootstrap derives one ``B`` and refuses two boundary scope names."""
    database = await scoped_database("rg_mixed_boot")

    with pytest.raises(ConfigError, match="audit_read_guard_sites"):
        application_for(
            MIXED, database, tmp_path, schema="rg_mixed_boot", audit={"reading": {"enabled": True}}
        )


async def test_a_multi_boundary_store_deploys_with_reading_off(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """FR-012, M2: with reading off nothing is derived and nothing is refused."""
    database, _ = await _deployed(
        scoped_database, tmp_path, "rg_mixed_off", document={}, product=MIXED
    )

    assert await _config(database, "boundary_scope") is None
    assert await _acl(database) == ["rg_mixed_off_owner:EXECUTE:false"]


async def test_the_owner_step_refuses_a_multi_boundary_store_with_reading_on(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """FR-012: ``apply_audit_reading`` rejects tables whose boundary is not the recorded ``B``."""
    await _deployed(scoped_database, tmp_path, "rg_mixed_on", document={}, product=MIXED)
    database = await scoped_database("rg_mixed_on", document=READING_ON)

    state, message = await _refusal(
        database.migrator, "SELECT loom_guard_rg_mixed_on.apply_audit_reading()"
    )

    assert (state, "audit_read_guard_sites" in message) == (INVALID, True)


async def test_reading_without_an_audit_log_installs_nothing(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """N19: a store with reading on but no audit log records the flag; there is no reader."""
    database, _ = await _deployed(
        scoped_database,
        tmp_path,
        "rg_nolog",
        document={**READING_ON, "boundary_scope": "owner"},
        product=devices,
    )

    assert await _config(database, "audit_reading") is True
    assert await _readers(database) == 0


async def test_disabling_the_audit_keeps_the_history_readable(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Review M5: with ``audit.enabled: false`` the trigger goes, the reader serves the history."""
    database, application = await _deployed(
        scoped_database, tmp_path, "rg_history", document=READING_ON
    )
    await _run(database.superuser, _insert_account("rg_history", 1))
    schema_config = msgspec.structs.replace(
        application.database.schema,
        audit=msgspec.structs.replace(application.database.schema.audit, enabled=False),
    )
    disabled = msgspec.structs.replace(application.database, schema=schema_config)
    await create_schema(database.migrator, replace(application, database=disabled))
    await _apply_reading(database)
    await _run(database.superuser, _insert_account("rg_history", 2))

    assert await _read_as(database.write, "rg_history", "acme") == [("I", "accounts")]


async def test_revision_three_upgrades_to_four_and_reads_the_existing_log(
    scoped_database: BootstrapFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rollout: a revision 3 store upgrades, records reading and reads its earlier rows."""
    revisions = guard_manifest.GUARD_REVISIONS[:AUDIT_GUARD_REVISION]
    monkeypatch.setattr(guard_manifest, "GUARD_REVISIONS", revisions)
    database = await scoped_database("rg_upgrade")
    application = application_for(bookkeeping, database, tmp_path, schema="rg_upgrade")
    await create_schema(database.migrator, application)
    await _run(database.superuser, _insert_account("rg_upgrade", 1))
    monkeypatch.undo()

    database = await scoped_database("rg_upgrade", document=READING_ON)
    applied = "SELECT string_agg(n::text, ',' ORDER BY n) FROM loom_guard_rg_upgrade.revision"
    assert await scalar(database.superuser, applied) == "1,2,3,4"
    await create_schema(database.migrator, application)
    await _apply_reading(database)

    assert await _config(database, "audit_reading") is True
    assert await _read_as(database.write, "rg_upgrade", "acme") == [("I", "accounts")]


FORGED_TREE = (
    "SELECT open_hatch()",
    "CREATE TABLE {schema}.forged_events (tenant varchar(36) NOT NULL, at timestamptz NOT NULL, "
    "PRIMARY KEY (tenant, at)) PARTITION BY RANGE (at)",
    "SELECT protect_scoped_table('{schema}.forged_events'::regclass, "
    """'[{{"col": "tenant", "scope": "tenant"}}]'::jsonb, ARRAY['SELECT', 'INSERT'])""",
    "SELECT protect_audited_table('{schema}.forged_events'::regclass, ARRAY[]::text[])",
    "SELECT open_hatch()",
    "CREATE TABLE {schema}.forged_events_all PARTITION OF {schema}.forged_events "
    "FOR VALUES FROM ('2000-01-01') TO ('2100-01-01')",
    "SELECT protect_scoped_table('{schema}.forged_events_all'::regclass, "
    """'[{{"col": "tenant", "scope": "forged"}}]'::jsonb, ARRAY['SELECT', 'INSERT'])""",
)
FORGED_TREE_ROW = "INSERT INTO {schema}.forged_events (tenant, at) VALUES ('acme', now())"
TWIN_SCOPES = (
    "CREATE TABLE {schema}.twin_scopes (tenant varchar(36) NOT NULL, alias text NOT NULL, "
    "id integer NOT NULL, PRIMARY KEY (tenant, id))",
    "SELECT protect_scoped_table('{schema}.twin_scopes'::regclass, "
    """'[{{"col": "tenant", "scope": "tenant"}}, """
    """{{"col": "alias", "scope": "tenant", "on": "write"}}]'::jsonb, ARRAY['SELECT', 'INSERT'])""",
    "SELECT protect_audited_table('{schema}.twin_scopes'::regclass, ARRAY[]::text[])",
)
TWIN_SCOPES_ROW = "INSERT INTO {schema}.twin_scopes (tenant, alias, id) VALUES ('acme', 'x', 1)"


@pytest.mark.parametrize(
    ("schema", "registration", "row", "table"),
    [
        ("rg_forged_tree", FORGED_TREE, FORGED_TREE_ROW, "forged_events_all"),
        ("rg_forged_twin", TWIN_SCOPES, TWIN_SCOPES_ROW, "twin_scopes"),
    ],
    ids=["partition-scopes-differ-from-root", "duplicate-scope-name"],
)
async def test_the_upgrade_refuses_a_registration_that_forged_history(
    scoped_database: BootstrapFactory,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    schema: str,
    registration: tuple[str, ...],
    row: str,
    table: str,
) -> None:
    """Security review (medium): a revision 3 registration 0004 forbids, with audited rows
    already written under it, stops the upgrade to 4 so reading never serves that history.

    The schema assertion fails closed (LG002) naming the table; the revision stays at 3."""
    revisions = guard_manifest.GUARD_REVISIONS[:AUDIT_GUARD_REVISION]
    monkeypatch.setattr(guard_manifest, "GUARD_REVISIONS", revisions)
    database = await scoped_database(schema)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=schema)
    )
    await _run(database.migrator, *(statement.format(schema=schema) for statement in registration))
    await _run(database.superuser, row.format(schema=schema))
    monkeypatch.undo()

    with pytest.raises(asyncpg.PostgresError, match=rf"{schema}\.{table}\b") as refused:
        await scoped_database(schema, document=READING_ON)

    assert getattr(refused.value, "sqlstate", "") == VIOLATION

    applied = f"SELECT string_agg(n::text, ',' ORDER BY n) FROM loom_guard_{schema}.revision"
    assert await scalar(database.superuser, applied) == "1,2,3"


@pytest.mark.parametrize("grantee", ["PUBLIC", "stray"])
async def test_turning_reading_off_refuses_while_another_grantee_holds_execute(
    scoped_database: BootstrapFactory, tmp_path: Path, created_roles: set[str], grantee: str
) -> None:
    """FR-002, security review (low): ``configure`` revokes only the readers group, so any
    other ``EXECUTE`` holder, ``PUBLIC`` included, makes turning reading off refuse (the
    assertion's "no other grantee", LG002)."""
    schema = f"rg_stray_{grantee.lower()}"
    database, _ = await _deployed(scoped_database, tmp_path, schema, document=READING_ON)
    role = "PUBLIC" if grantee == "PUBLIC" else f"{schema}_role"
    if role != "PUBLIC":
        created_roles.add(role)
        await _run(database.superuser, f"CREATE ROLE {role} NOLOGIN")
    await _behind_the_guard(database, f"GRANT EXECUTE ON FUNCTION {_reader(schema)} TO {role}")
    try:
        with pytest.raises(asyncpg.PostgresError, match="audit reader grant") as refused:
            await scoped_database(schema, document=READING_OFF)
    finally:
        # Another guard's drifted grant would refuse every later DDL in this database.
        await _behind_the_guard(
            database, f"REVOKE EXECUTE ON FUNCTION {_reader(schema)} FROM {role}"
        )

    assert getattr(refused.value, "sqlstate", "") == VIOLATION


async def _behind_the_guard(database: ScopedDatabase, statement: str) -> None:
    """Run *statement* as the superuser with the guard's event triggers off, then on again."""
    ddl, drop = (f"loom_guard_{database.schema}_{kind}" for kind in ("ddl", "drop"))
    await _run(
        database.superuser,
        f"ALTER EVENT TRIGGER {ddl} DISABLE",
        f"ALTER EVENT TRIGGER {drop} DISABLE",
        statement,
        f"ALTER EVENT TRIGGER {drop} ENABLE ALWAYS",
        f"ALTER EVENT TRIGGER {ddl} ENABLE ALWAYS",
    )
