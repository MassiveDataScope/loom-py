"""Postgres keeps every reader inside its boundary and read scopes (spec 009, US3, FR-005, FR-006).

Every test calls ``<schema>.read_audit_log`` positionally, as the adapter does,
as the application's write user, with the caller's ``loom.scope.*`` keys set
by ``set_config(..., true)`` inside the read's transaction. Two boundaries
(``acme``, ``globex``) and two regions (``north``, ``south``) are seeded
through real audited writes of the application user; a superuser writes the
log only where a scenario needs a past timestamp (the window tests), and says
so where it does.

The bootstrap document carries the reading keys (``audit_reading``,
``boundary_scope``, ``boundary_type``), as in ``test_audit_read_guard.py``,
and the owner step ``apply_audit_reading()`` is called by hand, as the last
step of each deploy, so these tests depend on guard revision 4 only.

Each write transaction is tagged with a ``trace_id`` that names the boundary,
the region and the change, so assertions read as sets of traces.
"""

from __future__ import annotations

import datetime as dt
import json
import secrets
import sys
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import astuple, dataclass, replace
from pathlib import Path
from types import ModuleType
from typing import Any

import asyncpg
import pytest
from sqlalchemy.engine import make_url

from loom.core.model import READ_WRITE, Audited, BaseModel, ColumnField, RowScoped, ScopedField
from loom.core.model.types import Integer, String, Text, Uuid
from loom.core.repository.sqlalchemy.rls import create_schema
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    scalar,
)

pytestmark = pytest.mark.integration

SCHEMA = "audit_read_visibility"
SCOPE_BINDINGS = {
    "tenant": "identity.subject",
    "region": "identity.region",
    "owner": "identity.owner",
    "editor": "identity.editor",
}


class _Ticket(BaseModel, RowScoped, Audited):
    """Boundary ``tenant`` and the read scope ``region``: a row can move between regions."""

    __tablename__ = "rv_tickets"

    tenant: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True)
    region: str = ScopedField(Text, scope="region", on="read")
    body: str = ColumnField(Text, default="")


class _Device(BaseModel, RowScoped, Audited):
    """A uuid read scope (US3 #9)."""

    __tablename__ = "rv_devices"

    tenant: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True)
    owner: str = ScopedField(Uuid, scope="owner", on="read")


class _Task(BaseModel, RowScoped, Audited):
    """Boundary only for reads; ``editor`` is a write-only scope (US3 #7, #13)."""

    __tablename__ = "rv_tasks"

    tenant: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True)
    editor: str = ScopedField(Text, scope="editor", on="write")
    body: str = ColumnField(Text, default="")


class _Note(BaseModel, Audited):
    """A global audited table of the scoped store (US3 #4, #4b)."""

    __tablename__ = "rv_notes"
    __privileges__ = {"writers": READ_WRITE}

    id: int = ColumnField(Integer, primary_key=True)
    body: str = ColumnField(Text, default="")


MODELS = (_Ticket, _Device, _Task, _Note)
PRODUCT = sys.modules[__name__]

# A second product, with no scoped table (N19, US3 #11). Discovery collects the
# classes whose ``__module__`` is the module it imports, so the class names this one.
GLOBALS = ModuleType(f"{__name__}_globals")
GLOBALS.SCHEMA = "audit_read_visibility_globals"  # type: ignore[attr-defined]
GLOBALS.SCOPE_BINDINGS = {}  # type: ignore[attr-defined]


class _Bulletin(BaseModel, Audited):
    __module__ = GLOBALS.__name__
    __tablename__ = "rv_bulletins"
    __privileges__ = {"writers": READ_WRITE}

    id: int = ColumnField(Integer, primary_key=True)
    body: str = ColumnField(Text, default="")


GLOBALS._Bulletin = _Bulletin  # type: ignore[attr-defined]
GLOBALS.MODELS = (_Bulletin,)  # type: ignore[attr-defined]
sys.modules[GLOBALS.__name__] = GLOBALS

TENANT_B = {"boundary_scope": "tenant", "boundary_type": "character varying"}
READING_ON = {"audit_reading": True, **TENANT_B}
READING_NO_B = {"audit_reading": True}

ACME_NORTH = {"tenant": "acme", "region": "north"}
ACME_SOUTH = {"tenant": "acme", "region": "south"}
GLOBEX_NORTH = {"tenant": "globex", "region": "north"}
GLOBEX_SOUTH = {"tenant": "globex", "region": "south"}
OWNER = "6f1c2a9e-4b7d-4e2a-9c3f-0a1b2c3d4e5f"

# What an acme caller of each region reads with ALL_SUBJECTS: rv_tickets and rv_tasks rows;
# rv_devices needs the ``owner`` key, rv_notes is global.
SEEN_ACME_NORTH = ["an-before-move", "an-insert", "an-insert", "an-update", "task-acme"]
SEEN_ACME_SOUTH = ["as-after-move", "as-insert", "task-acme"]

READ = (
    "SELECT * FROM {schema}.read_audit_log($1::boolean, $2::text[], $3::integer, "
    "$4::timestamptz, $5::timestamptz, $6::text, $7::text, $8::text[], $9::jsonb, $10::text, "
    "$11::text, $12::timestamptz, $13::bigint, $14::boolean, $15::integer)"
)
INVALID_ARGUMENTS = "LG005"
OVER_CAP = "LG004"


@dataclass(frozen=True, slots=True)
class Call:
    """The positional arguments of ``read_audit_log``, in the pinned order."""

    all: bool | None = True
    subjects: Sequence[str | None] | None = None
    cap: int | None = 1000
    from_: dt.datetime | None = None
    to: dt.datetime | None = None
    subject: str | None = None
    table: str | None = None
    tables: Sequence[str] | None = None
    row_key: str | None = None
    trace_id: str | None = None
    op: str | None = None
    after_at: dt.datetime | None = None
    after_id: int | None = None
    ascending: bool = False
    limit: int = 101


ALL = Call()


def _subjects(*subjects: str | None, cap: int = 1000) -> Call:
    return replace(ALL, all=False, subjects=list(subjects), cap=cap)


def _key(tenant: str, id_: int) -> str:
    return json.dumps({"id": id_, "tenant": tenant})


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


@pytest.fixture(scope="module")
def anyio_backend() -> str:
    return "asyncio"


@asynccontextmanager
async def _connect(url: str) -> AsyncIterator[asyncpg.Connection]:
    raw = make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)
    connection = await asyncpg.connect(raw)
    try:
        yield connection
    finally:
        await connection.close()


async def _settings(connection: asyncpg.Connection, settings: Mapping[str, str]) -> None:
    for name, value in settings.items():
        await connection.execute("SELECT set_config($1, $2, true)", name, value)


def _scope_settings(scopes: Mapping[str, str]) -> dict[str, str]:
    return {f"loom.scope.{name}": value for name, value in scopes.items()}


async def _run(url: str, *statements: str) -> None:
    async with _connect(url) as connection, connection.transaction():
        for statement in statements:
            await connection.execute(statement)


async def _write(
    url: str, scopes: Mapping[str, str], actor: str, trace: str, *statements: str
) -> None:
    """One audited transaction as the user in *url*, acting as *actor* within *scopes*."""
    settings = {
        **_scope_settings(scopes),
        "loom.actor.subject": actor,
        "loom.trace_id": trace,
    }
    async with _connect(url) as connection, connection.transaction():
        await _settings(connection, settings)
        for statement in statements:
            await connection.execute(statement)


async def _read(
    database: ScopedDatabase, scopes: Mapping[str, str], call: Call = ALL
) -> list[asyncpg.Record]:
    """``read_audit_log`` as the application's write user, with *scopes* as session keys."""
    async with _connect(database.write) as connection, connection.transaction():
        await _settings(connection, _scope_settings(scopes))
        return list(await connection.fetch(READ.format(schema=database.schema), *astuple(call)))


async def _refusal(database: ScopedDatabase, scopes: Mapping[str, str], call: Call) -> str:
    """The SQLSTATE the read fails with, or ``"none"``."""
    try:
        await _read(database, scopes, call)
    except asyncpg.PostgresError as exc:
        return str(getattr(exc, "sqlstate", ""))
    return "none"


def _traces(rows: Sequence[asyncpg.Record]) -> list[str]:
    return sorted(row["trace_id"] for row in rows)


def _tenants(rows: Sequence[asyncpg.Record]) -> set[str]:
    return {json.loads(row["row_key"]).get("tenant", "") for row in rows}


async def _bootstrap(
    factory: BootstrapFactory,
    tmp_path: Path,
    schema: str,
    *,
    document: Mapping[str, object],
    product: ModuleType = PRODUCT,
) -> ScopedDatabase:
    """Bootstrap *schema* with *document* and create *product*'s tables: no owner step."""
    database = await factory(schema, document=dict(document))
    await create_schema(
        database.migrator, application_for(product, database, tmp_path, schema=schema)
    )
    return database


async def _apply_reading(database: ScopedDatabase) -> None:
    """The deploy's owner step: ``apply_audit_reading()`` as the migrator."""
    await _run(database.migrator, f"SELECT loom_guard_{database.schema}.apply_audit_reading()")


def _schema() -> str:
    return f"rv{secrets.token_hex(3)}"


async def _seed(database: ScopedDatabase) -> None:
    """Rows in both boundaries and both regions; ticket 2 of acme moves north to south."""
    s = database.schema
    w = database.write
    await _write(
        w,
        ACME_NORTH,
        "carla",
        "an-insert",
        f"INSERT INTO {s}.rv_tickets (tenant, id, region, body) "
        "VALUES ('acme', 1, 'north', 'a1'), ('acme', 2, 'north', 'a2')",
    )
    await _write(
        w,
        ACME_SOUTH,
        "bob",
        "as-insert",
        f"INSERT INTO {s}.rv_tickets (tenant, id, region, body) VALUES ('acme', 3, 'south', 'b3')",
    )
    await _write(
        w,
        GLOBEX_NORTH,
        "dave",
        "gn-insert",
        f"INSERT INTO {s}.rv_tickets (tenant, id, region, body) "
        "VALUES ('globex', 1, 'north', 'g1')",
    )
    await _write(
        w,
        GLOBEX_SOUTH,
        "dave",
        "gs-insert",
        f"INSERT INTO {s}.rv_tickets (tenant, id, region, body) "
        "VALUES ('globex', 2, 'south', 'g2')",
    )
    await _write(
        w,
        ACME_NORTH,
        "carla",
        "an-update",
        f"UPDATE {s}.rv_tickets SET body = 'a1x' WHERE tenant = 'acme' AND id = 1",
    )
    await _write(
        w,
        ACME_NORTH,
        "carla",
        "an-before-move",
        f"UPDATE {s}.rv_tickets SET body = 'a2x' WHERE tenant = 'acme' AND id = 2",
    )
    # The application user cannot move a row out of its own read scope (Postgres checks
    # the new row against ``loom_select``), so the move is the bypass user's, still audited.
    await _write(
        database.bypass,
        {},
        "ops",
        "move",
        f"UPDATE {s}.rv_tickets SET region = 'south' WHERE tenant = 'acme' AND id = 2",
    )
    await _write(
        w,
        ACME_SOUTH,
        "bob",
        "as-after-move",
        f"UPDATE {s}.rv_tickets SET body = 'a2y' WHERE tenant = 'acme' AND id = 2",
    )
    await _write(
        w,
        {"tenant": "acme", "editor": "ana"},
        "carla",
        "task-acme",
        f"INSERT INTO {s}.rv_tasks (tenant, id, editor, body) VALUES ('acme', 1, 'ana', 't')",
    )
    await _write(
        w,
        {"tenant": "globex", "editor": "ana"},
        "dave",
        "task-globex",
        f"INSERT INTO {s}.rv_tasks (tenant, id, editor, body) VALUES ('globex', 1, 'ana', 't')",
    )
    await _write(
        w,
        {"tenant": "acme"},
        "carla",
        "device-acme",
        f"INSERT INTO {s}.rv_devices (tenant, id, owner) VALUES ('acme', 1, '{OWNER}')",
    )
    await _write(
        w, {}, "carla", "note-acme", f"INSERT INTO {s}.rv_notes (id, body) VALUES (1, 'n')"
    )
    await _write(
        w, {}, "dave", "note-globex", f"INSERT INTO {s}.rv_notes (id, body) VALUES (2, 'n')"
    )


async def _seeded(
    factory: BootstrapFactory, tmp_path: Path, *, document: Mapping[str, object] = READING_ON
) -> ScopedDatabase:
    """A deployed store with the seed and the owner step applied."""
    database = await _bootstrap(factory, tmp_path, _schema(), document=document)
    await _seed(database)
    await _apply_reading(database)
    return database


# Every guard's event triggers fire on every DDL of the module database, so each
# bootstrap slows the next. Tests that only read share one store per module
# database; a failed build is kept and raised again, so a red run builds it once.
_SHARED: dict[tuple[str, str], ScopedDatabase | BaseException] = {}


async def _shared(
    name: str, database_uri: str, build: Callable[[], Awaitable[ScopedDatabase]]
) -> ScopedDatabase:
    key = (database_uri, name)
    if key not in _SHARED:
        try:
            _SHARED[key] = await build()
        except Exception as exc:
            _SHARED[key] = exc
    shared = _SHARED[key]
    if isinstance(shared, BaseException):
        raise shared
    return shared


@pytest.fixture
async def seeded(
    scoped_database: BootstrapFactory, module_database_uri: str, tmp_path: Path
) -> ScopedDatabase:
    """The seeded store, read-only: tests must not write to it."""
    return await _shared("seeded", module_database_uri, lambda: _seeded(scoped_database, tmp_path))


@pytest.fixture
async def windowed(
    scoped_database: BootstrapFactory, module_database_uri: str, tmp_path: Path
) -> ScopedDatabase:
    """The store of the window tests, read-only."""
    return await _shared(
        "windowed", module_database_uri, lambda: _windowed(scoped_database, tmp_path)
    )


async def _registered_scopes(database: ScopedDatabase, table: str) -> list[dict[str, Any]]:
    s = database.schema
    raw = await scalar(
        database.superuser,
        f"SELECT scopes::text FROM loom_guard_{s}.scoped_table WHERE rel = '{s}.{table}'::regclass",
    )
    return list(json.loads(str(raw)))


async def _reprotect(
    database: ScopedDatabase, table: str, scopes: Sequence[Mapping[str, Any]], *ddl: str
) -> None:
    """Unprotect *table*, run *ddl*, protect it again with *scopes*: the owner, in one hatch."""
    s = database.schema
    guard = f"loom_guard_{s}"
    privileges = "ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE']"
    await _run(
        database.migrator,
        f"SELECT {guard}.open_hatch()",
        f"SELECT {guard}.unprotect_scoped_table('{s}.{table}')",
        *ddl,
        f"SELECT {guard}.protect_scoped_table('{s}.{table}', '{json.dumps(list(scopes))}', "
        f"{privileges})",
    )


# --- US3: boundary and read scopes -------------------------------------------------------------


async def test_an_acme_caller_reads_no_globex_row(seeded: ScopedDatabase) -> None:
    """US3 #1: ALL_SUBJECTS within acme returns acme rows and no globex row."""
    database = seeded

    rows = await _read(database, ACME_NORTH)

    assert _tenants(rows) == {"acme"}
    assert _traces(rows) == SEEN_ACME_NORTH


async def test_a_north_caller_reads_no_south_row(seeded: ScopedDatabase) -> None:
    """US3 #2: the read scope ``region`` bounds an ALL_SUBJECTS caller of north."""
    database = seeded

    rows = await _read(database, ACME_NORTH, replace(ALL, table="rv_tickets"))

    assert not {"as-insert", "as-after-move", "move", "gs-insert"} & set(_traces(rows))
    assert _traces(rows) == ["an-before-move", "an-insert", "an-insert", "an-update"]


async def test_a_south_caller_does_not_read_the_update_that_moved_a_row_in(
    seeded: ScopedDatabase,
) -> None:
    """US3 #3, term 4: the north-to-south update is hidden from south (its old value is north)."""
    database = seeded

    rows = await _read(database, ACME_SOUTH, replace(ALL, row_key=_key("acme", 2)))

    assert _traces(rows) == ["as-after-move"]


async def test_no_caller_reads_a_global_table_through_the_function(seeded: ScopedDatabase) -> None:
    """US3 #4: rows of a global table carry no ``B`` and are never returned."""
    database = seeded

    callers = (ACME_NORTH, ACME_SOUTH, GLOBEX_NORTH, {}, {"tenant": ""})
    rows = [row for scopes in callers for row in await _read(database, scopes)]

    assert [row for row in rows if row["table_name"] == "rv_notes"] == []
    assert await _read(database, ACME_NORTH, replace(ALL, table="rv_notes")) == []


async def test_the_auditor_reads_every_row_of_the_global_table(seeded: ScopedDatabase) -> None:
    """US3 #4b (review N13): the auditor role reads the global table's rows in the log."""
    database = seeded
    s = database.schema

    async with _connect(database.superuser) as connection, connection.transaction():
        await connection.execute(f"SET LOCAL ROLE {s}_auditor")
        rows = await connection.fetch(
            f"SELECT trace_id FROM {s}.audit_log WHERE table_name = 'rv_notes'"
        )

    assert _traces(rows) == ["note-acme", "note-globex"]


async def test_rows_of_a_renamed_table_are_not_returned(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US3 #5, term 2: rows logged under the old name no longer resolve; new ones do."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _seed(database)
    s = database.schema
    await _run(
        database.migrator,
        f"SELECT loom_guard_{s}.open_hatch()",
        f"ALTER TABLE {s}.rv_tickets RENAME TO rv_cases",
    )
    await _write(
        database.write,
        ACME_NORTH,
        "carla",
        "an-renamed",
        f"INSERT INTO {s}.rv_cases (tenant, id, region, body) VALUES ('acme', 9, 'north', 'r')",
    )
    await _apply_reading(database)

    rows = await _read(database, ACME_NORTH)

    assert [row for row in rows if row["table_name"] == "rv_tickets"] == []
    assert _traces(rows) == ["an-renamed", "task-acme"]


async def test_rows_of_a_dropped_table_are_not_returned(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US3 #6, term 2: a dropped table's rows stay in the log and are not returned."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _seed(database)
    s = database.schema
    guard = f"loom_guard_{s}"
    await _run(
        database.migrator,
        f"SELECT {guard}.open_hatch()",
        f"SELECT {guard}.unprotect_scoped_table('{s}.rv_tickets')",
        f"SELECT {guard}.unprotect_audited_table('{s}.rv_tickets')",
        f"SELECT {guard}.open_hatch()",  # unprotect_audited_table leaves the hatch
        f"DROP TABLE {s}.rv_tickets",
    )
    await _apply_reading(database)

    rows = await _read(database, ACME_NORTH)

    assert _traces(rows) == ["task-acme"]


async def test_update_rows_are_hidden_after_their_scope_column_is_renamed(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US3 #7, term 4: an update whose ``changed`` names a column that no longer exists is hidden.

    ``editor`` is a write-only scope, so before the rename the update is visible
    to acme (term 3 does not check it). The update is written by the bypass user,
    the only user that may change a non-elevable write scope; the trigger audits it.
    """
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _seed(database)
    s = database.schema
    await _write(
        database.bypass,
        {},
        "ops",
        "task-reassigned",
        f"UPDATE {s}.rv_tasks SET editor = 'bob' WHERE tenant = 'acme' AND id = 1",
    )
    await _apply_reading(database)
    tasks = replace(ALL, table="rv_tasks")
    assert _traces(await _read(database, {"tenant": "acme"}, tasks)) == [
        "task-acme",
        "task-reassigned",
    ]

    scopes = [
        {**x, "col": "author"} if x["scope"] == "editor" else x
        for x in await _registered_scopes(database, "rv_tasks")
    ]
    await _reprotect(
        database, "rv_tasks", scopes, f"ALTER TABLE {s}.rv_tasks RENAME COLUMN editor TO author"
    )
    await _apply_reading(database)

    rows = await _read(database, {"tenant": "acme"}, tasks)

    assert [row["op"] for row in rows] == ["I"]
    assert _traces(rows) == ["task-acme"]


async def test_a_move_is_hidden_from_both_regions_after_its_read_scope_column_is_renamed(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Term 4 (review M3): ``changed`` of the move holds ``region``, no longer a current column.

    After ``region`` becomes ``area`` (re-protected, scope name kept), the registry maps
    the scope to ``area``, so only the rename rule still hides the move from south: its
    old value sits under a key the old-value check no longer reads. The seed's move is
    the bypass user's (see ``_seed``); rows that changed only ``body`` stay visible.
    """
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _seed(database)
    s = database.schema
    scopes = [
        {**x, "col": "area"} if x["scope"] == "region" else x
        for x in await _registered_scopes(database, "rv_tickets")
    ]
    await _reprotect(
        database, "rv_tickets", scopes, f"ALTER TABLE {s}.rv_tickets RENAME COLUMN region TO area"
    )
    await _apply_reading(database)
    tickets = replace(ALL, table="rv_tickets")

    north = await _read(database, ACME_NORTH, tickets)
    south = await _read(database, ACME_SOUTH, tickets)

    assert "move" not in _traces(north) + _traces(south)
    assert _traces(north) == ["an-before-move", "an-insert", "an-insert", "an-update"]
    assert _traces(south) == ["as-after-move", "as-insert"]


async def test_nothing_is_returned_when_every_scoped_table_is_unprotected(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US3 #8: the store keeps its recorded ``B``; no row resolves to a registered table."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _seed(database)
    s = database.schema
    guard = f"loom_guard_{s}"
    await _run(
        database.migrator,
        f"SELECT {guard}.open_hatch()",
        *(
            f"SELECT {guard}.unprotect_scoped_table('{s}.{table}')"
            for table in ("rv_tickets", "rv_devices", "rv_tasks")
        ),
    )
    await _apply_reading(database)

    assert await scalar(database.superuser, f"SELECT boundary_scope FROM {guard}.config") == (
        "tenant"
    )
    for scopes in (ACME_NORTH, ACME_SOUTH, GLOBEX_NORTH, {}):
        assert await _read(database, scopes) == []


async def test_a_uuid_scope_matches_whatever_the_case_of_the_session_value(
    seeded: ScopedDatabase,
) -> None:
    """US3 #9, term 5: the session value is cast to uuid, as ``loom_select`` does."""
    database = seeded
    caller = {"tenant": "acme", "owner": OWNER.upper()}

    rows = await _read(database, caller, replace(ALL, table="rv_devices"))
    async with _connect(database.write) as connection, connection.transaction():
        await _settings(connection, _scope_settings(caller))
        on_the_table = await connection.fetchval(
            f"SELECT count(*) FROM {database.schema}.rv_devices"
        )

    assert on_the_table == 1
    assert _traces(rows) == ["device-acme"]
    uncastable = {"tenant": "acme", "owner": "not-a-uuid"}
    assert await _read(database, uncastable, replace(ALL, table="rv_devices")) == []


@pytest.mark.parametrize(
    "scopes",
    [ACME_NORTH, ACME_SOUTH, GLOBEX_NORTH, GLOBEX_SOUTH, {"tenant": "acme"}],
    ids=["acme-north", "acme-south", "globex-north", "globex-south", "acme-no-region"],
)
async def test_the_function_returns_no_live_key_the_table_hides(
    seeded: ScopedDatabase, scopes: Mapping[str, str]
) -> None:
    """US3 #10: for keys that never moved, every live key read is visible under ``loom_select``."""
    database = seeded
    s = database.schema
    moved = {_key("acme", 2)}

    async with _connect(database.write) as connection, connection.transaction():
        await _settings(connection, _scope_settings(scopes))
        logged = await connection.fetch(
            READ.format(schema=s), *astuple(replace(ALL, table="rv_tickets"))
        )
        visible = await connection.fetch(
            f"SELECT jsonb_build_object('tenant', tenant, 'id', id)::text AS k FROM {s}.rv_tickets"
        )

    read_keys = {json.dumps(json.loads(row["row_key"])) for row in logged} - moved
    table_keys = {json.dumps(json.loads(row["k"])) for row in visible}
    assert read_keys <= table_keys
    if "region" in scopes:
        assert read_keys, "the comparison needs at least one live key"


async def test_a_scoped_table_added_after_reading_was_enabled_keeps_the_boundary(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US3 #11: no scoped table at first (no ``B``); the deploy that adds one records it."""
    schema = _schema()
    first = await _bootstrap(
        scoped_database, tmp_path, schema, document=READING_NO_B, product=GLOBALS
    )
    await _apply_reading(first)
    assert (
        await scalar(first.superuser, f"SELECT boundary_scope FROM loom_guard_{schema}.config")
        is None
    )

    database = await _bootstrap(scoped_database, tmp_path, schema, document=READING_ON)
    await _seed(database)
    await _apply_reading(database)

    rows = await _read(database, ACME_NORTH)

    assert _tenants(rows) == {"acme"}
    assert _traces(rows) == SEEN_ACME_NORTH


async def test_a_scoped_store_without_a_recorded_boundary_returns_nothing(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US3 #12: scoped tables registered and no ``B`` in the guard config: nothing, for anyone."""
    database = await _seeded(scoped_database, tmp_path, document=READING_NO_B)

    assert (
        await scalar(
            database.superuser, f"SELECT boundary_scope FROM loom_guard_{database.schema}.config"
        )
        is None
    )
    for scopes in (ACME_NORTH, ACME_SOUTH, GLOBEX_NORTH, {}):
        assert await _read(database, scopes) == []


async def test_rows_logged_before_a_read_scope_was_added_are_not_returned(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """US3 #13, term 2: old rows lack the new scope key; rows written after it carry it."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _seed(database)
    s = database.schema
    scopes = [
        *await _registered_scopes(database, "rv_tasks"),
        {"col": "zone", "scope": "zone", "on": "read"},
    ]
    await _reprotect(
        database,
        "rv_tasks",
        scopes,
        f"ALTER TABLE {s}.rv_tasks ADD COLUMN zone text NOT NULL DEFAULT 'z1'",
    )
    await _write(
        database.write,
        {"tenant": "acme", "editor": "ana"},
        "carla",
        "task-zoned",
        f"INSERT INTO {s}.rv_tasks (tenant, id, editor, body, zone) "
        "VALUES ('acme', 2, 'ana', 't', 'z1')",
    )
    await _apply_reading(database)

    rows = await _read(database, {"tenant": "acme", "zone": "z1"}, replace(ALL, table="rv_tasks"))

    assert _traces(rows) == ["task-zoned"]


async def test_a_moved_row_keeps_its_earlier_history_in_its_earlier_scope(
    seeded: ScopedDatabase,
) -> None:
    """US3 #14: north reads the changes made in north, not the move, nothing after it."""
    database = seeded
    ticket = replace(ALL, row_key=_key("acme", 2))

    north = await _read(database, ACME_NORTH, ticket)
    south = await _read(database, ACME_SOUTH, ticket)

    assert _traces(north) == ["an-before-move", "an-insert"]
    assert _traces(south) == ["as-after-move"]


# --- Tables registered outside the owner step (security review of revision 4) -----------------

# Either outcome closes a leak: the guard refuses to register the table, or the reader
# returns none of its rows. ``LG002`` is a guard violation, ``22023`` a rejection.
GUARD_REFUSALS = {"LG002", "22023"}
VIOLATION = "LG002"
REJECTED = "22023"
PRIVILEGES = "ARRAY['SELECT', 'INSERT', 'UPDATE', 'DELETE']"


async def _register(
    database: ScopedDatabase, table: str, columns: str, scopes: Sequence[Mapping[str, str]]
) -> str:
    """Create, protect and audit *table* as the owner; the SQLSTATE of a refusal, or ``"none"``.

    This is a later migration's work: it runs after the deploy's owner step.
    """
    s = database.schema
    guard = f"loom_guard_{s}"
    return await _outcome(
        database.migrator,
        f"SELECT {guard}.open_hatch()",
        f"CREATE TABLE {s}.{table} ({columns})",
        f"SELECT {guard}.protect_scoped_table('{s}.{table}', '{json.dumps(list(scopes))}', "
        f"{PRIVILEGES})",
        f"SELECT {guard}.protect_audited_table('{s}.{table}', '{{}}')",
    )


async def test_a_table_whose_read_boundary_is_not_b_leaks_no_row(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Term 1: B must be the read boundary of every table whose rows the reader returns.

    ``rv_orgs`` is bounded by ``org`` and carries ``tenant`` (B) as a write-only scope,
    so a row can move from acme to globex. The move's ``changed`` holds acme's old values.
    """
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _apply_reading(database)
    s = database.schema
    refusal = await _register(
        database,
        "rv_orgs",
        "org text NOT NULL, tenant varchar(36) NOT NULL, id integer NOT NULL, "
        "body text NOT NULL DEFAULT '', PRIMARY KEY (org, id)",
        [
            {"col": "org", "scope": "org", "on": "both"},
            {"col": "tenant", "scope": "tenant", "on": "write"},
        ],
    )
    if refusal != "none":
        assert refusal in GUARD_REFUSALS
        return
    await _write(
        database.write,
        {"org": "o1", "tenant": "acme"},
        "carla",
        "org-acme",
        f"INSERT INTO {s}.rv_orgs (org, tenant, id, body) VALUES ('o1', 'acme', 1, 'secret')",
    )
    # Moving a row across B needs a user outside its write scopes: the superuser, audited.
    await _write(
        database.superuser,
        {},
        "ops",
        "org-moved",
        f"UPDATE {s}.rv_orgs SET tenant = 'globex', body = 'moved' WHERE org = 'o1' AND id = 1",
    )
    orgs = replace(ALL, table="rv_orgs")

    globex = await _read(database, {"tenant": "globex", "org": "o1"}, orgs)
    acme = await _read(database, {"tenant": "acme", "org": "o1"}, orgs)

    assert "org-moved" not in _traces(globex), "globex reads acme's old values"
    assert _traces(globex) == []
    assert _traces(acme) == []


ORGS_COLUMNS = (
    "org text NOT NULL, tenant varchar(36) NOT NULL, id integer NOT NULL, "
    "body text NOT NULL DEFAULT '', PRIMARY KEY (tenant, id)"
)


@asynccontextmanager
async def _forged_registry(
    database: ScopedDatabase, table: str, scopes: Sequence[Mapping[str, str]]
) -> AsyncIterator[None]:
    """Rewrite *table*'s registered scopes as the superuser, past every guard check.

    The table keeps the policies of its legitimate registration; only the registry,
    which the audit writer and the reader both read, changes. No DDL runs, so no
    guard assertion fires. Every guard asserts its schema on any DDL of the database,
    so the legitimate scopes are restored before other tests run DDL.
    """
    s = database.schema
    registry = f"loom_guard_{s}.scoped_table"
    where = f"WHERE rel = '{s}.{table}'::regclass"
    original = await scalar(database.superuser, f"SELECT scopes::text FROM {registry} {where}")
    await _run(
        database.superuser,
        f"UPDATE {registry} SET scopes = '{json.dumps(list(scopes))}' {where}",
    )
    try:
        yield
    finally:
        await _run(database.superuser, f"UPDATE {registry} SET scopes = '{original}' {where}")


async def test_the_reader_skips_a_registered_table_whose_read_boundary_is_not_b(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Term 1: a table in the registry without B as its read boundary returns no row.

    The guard refuses such a registration, so the superuser forges it: ``rv_orgs``
    is registered with boundary ``tenant`` and then rewritten as bounded by ``org``
    with ``tenant`` write-only. A move from acme to globex, by the superuser (outside
    any write scope), leaves acme's old values in ``changed``.
    """
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _apply_reading(database)
    s = database.schema
    legitimate = [
        {"col": "tenant", "scope": "tenant", "on": "both"},
        {"col": "org", "scope": "org", "on": "read"},
    ]
    assert await _register(database, "rv_orgs", ORGS_COLUMNS, legitimate) == "none"
    unbounded = [
        {"col": "org", "scope": "org", "on": "both"},
        {"col": "tenant", "scope": "tenant", "on": "write"},
    ]
    orgs = replace(ALL, table="rv_orgs")
    async with _forged_registry(database, "rv_orgs", unbounded):
        await _write(
            database.write,
            {"org": "o1", "tenant": "acme"},
            "carla",
            "org-acme",
            f"INSERT INTO {s}.rv_orgs (org, tenant, id, body) VALUES ('o1', 'acme', 1, 'secret')",
        )
        await _write(
            database.superuser,
            {},
            "ops",
            "org-moved",
            f"UPDATE {s}.rv_orgs SET tenant = 'globex', body = 'moved' WHERE tenant = 'acme'",
        )
        globex = await _read(database, {"tenant": "globex", "org": "o1"}, orgs)
        acme = await _read(database, {"tenant": "acme", "org": "o1"}, orgs)

    assert "org-moved" not in _traces(globex), "globex reads acme's old values"
    assert _traces(globex) == []
    assert _traces(acme) == []


DUPLICATE_TENANT = [
    {"col": "tenant_id", "scope": "tenant", "on": "both"},
    {"col": "other_col", "scope": "tenant", "on": "read"},
]
DUPES_COLUMNS = (
    "tenant_id varchar(36) NOT NULL, other_col varchar(36) NOT NULL, id integer NOT NULL, "
    "PRIMARY KEY (tenant_id, id)"
)


async def test_protecting_a_table_with_a_duplicate_scope_name_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Security review: two entries named ``tenant`` let the audit writer log the wrong one."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)

    refusal = await _register(database, "rv_dupes", DUPES_COLUMNS, DUPLICATE_TENANT)

    assert refusal == REJECTED


async def test_a_duplicate_scope_name_cannot_forge_another_boundary(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Security review: the writer keeps the last ``tenant`` (``other_col``) in ``scopes``.

    Acme writes a row whose ``other_col`` is ``'globex'``; the log then says
    ``tenant = globex``. If the guard refuses the duplicate registration, the
    superuser forges it in the registry, so the reader is tested either way.
    """
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _apply_reading(database)
    s = database.schema
    dupes = replace(ALL, table="rv_dupes")
    plant = f"INSERT INTO {s}.rv_dupes (tenant_id, other_col, id) VALUES ('acme', 'globex', 1)"
    if await _register(database, "rv_dupes", DUPES_COLUMNS, DUPLICATE_TENANT) == "none":
        await _write(database.write, {"tenant": "acme"}, "carla", "dupe-acme", plant)
        globex = await _read(database, {"tenant": "globex"}, dupes)
    else:
        legitimate = DUPLICATE_TENANT[:1]
        assert await _register(database, "rv_dupes", DUPES_COLUMNS, legitimate) == "none"
        async with _forged_registry(database, "rv_dupes", DUPLICATE_TENANT):
            await _write(database.write, {"tenant": "acme"}, "carla", "dupe-acme", plant)
            globex = await _read(database, {"tenant": "globex"}, dupes)

    assert _traces(globex) == [], "globex reads acme's row"


async def _outcome(url: str, *statements: str) -> str:
    """The SQLSTATE *statements* fail with in one transaction, or ``"none"``."""
    try:
        await _run(url, *statements)
    except asyncpg.PostgresError as exc:
        return str(getattr(exc, "sqlstate", ""))
    return "none"


async def _assert_schema(database: ScopedDatabase) -> str:
    """The SQLSTATE of the guard's schema assertion, as the owner runs it, or ``"none"``.

    It is the assertion every DDL of the application schema ends with; it is called
    directly so no other check of the DDL path can answer for it.
    """
    return await _outcome(
        database.migrator, f"SELECT loom_guard_{database.schema}.assert_scoped_schema()"
    )


async def test_the_schema_assertion_refuses_a_duplicate_scope_name_in_the_registry(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Security review: ``assert_scoped_schema`` refuses a registry ``protect`` would refuse."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    legitimate = DUPLICATE_TENANT[:1]
    assert await _register(database, "rv_dupes", DUPES_COLUMNS, legitimate) == "none"

    async with _forged_registry(database, "rv_dupes", DUPLICATE_TENANT):
        state = await _assert_schema(database)

    assert state == VIOLATION


PARTS_COLUMNS = (
    "tenant_id varchar(36) NOT NULL, col2 varchar(36) NOT NULL, at date NOT NULL, "
    "id integer NOT NULL, PRIMARY KEY (tenant_id, col2, at, id)"
)
ROOT_SCOPES = [{"col": "tenant_id", "scope": "tenant", "on": "both"}]
# ``col2`` is in every unique index, so only the scope mismatch can refuse this.
PARTITION_SCOPES = [{"col": "col2", "scope": "tenant", "on": "both"}]


async def _partitioned(database: ScopedDatabase) -> None:
    """``rv_parts``: a range-partitioned root, protected with ``ROOT_SCOPES`` and audited."""
    s = database.schema
    guard = f"loom_guard_{s}"
    state = await _outcome(
        database.migrator,
        f"SELECT {guard}.open_hatch()",
        f"CREATE TABLE {s}.rv_parts ({PARTS_COLUMNS}) PARTITION BY RANGE (at)",
        f"SELECT {guard}.protect_scoped_table('{s}.rv_parts', '{json.dumps(ROOT_SCOPES)}', "
        f"{PRIVILEGES})",
        f"SELECT {guard}.protect_audited_table('{s}.rv_parts', '{{}}')",
    )
    assert state == "none"


async def _mismatched_partition(database: ScopedDatabase) -> str:
    """Create ``rv_parts_p`` and protect it with ``PARTITION_SCOPES``; the refusal or ``"none"``."""
    s = database.schema
    guard = f"loom_guard_{s}"
    return await _outcome(
        database.migrator,
        f"SELECT {guard}.open_hatch()",
        f"CREATE TABLE {s}.rv_parts_p PARTITION OF {s}.rv_parts "
        "FOR VALUES FROM ('2000-01-01') TO ('2100-01-01')",
        f"SELECT {guard}.protect_scoped_table('{s}.rv_parts_p', "
        f"'{json.dumps(PARTITION_SCOPES)}', {PRIVILEGES})",
    )


async def _matching_partition(database: ScopedDatabase) -> None:
    """``rv_parts_p`` through the guard, registered with its root's scopes."""
    s = database.schema
    state = await _outcome(
        database.migrator,
        f"SELECT loom_guard_{s}.create_range_partition('{s}.rv_parts', 'rv_parts_p', "
        "'2000-01-01', '2100-01-01')",
    )
    assert state == "none"


async def test_protecting_a_partition_with_scopes_other_than_its_roots_is_refused(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Security review: the writer reads the partition's scopes and logs the root's name."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _partitioned(database)

    assert await _mismatched_partition(database) == REJECTED


async def test_a_partition_registered_with_other_scopes_cannot_forge_another_boundary(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Security review: acme writes through the root with ``col2 = 'globex'``.

    The writer logs ``tenant`` from the partition's registry (``col2``) under the
    root's name, which the reader matches with the root's scopes. If the guard
    refuses the registration, the superuser forges it, so the reader is tested
    either way.
    """
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _partitioned(database)
    await _apply_reading(database)
    s = database.schema
    parts = replace(ALL, table="rv_parts")
    plant = (
        f"INSERT INTO {s}.rv_parts (tenant_id, col2, at, id) "
        "VALUES ('acme', 'globex', '2026-01-01', 1)"
    )
    if await _mismatched_partition(database) == "none":
        await _write(database.write, {"tenant": "acme"}, "carla", "part-acme", plant)
        globex = await _read(database, {"tenant": "globex"}, parts)
    else:
        await _matching_partition(database)
        async with _forged_registry(database, "rv_parts_p", PARTITION_SCOPES):
            await _write(database.write, {"tenant": "acme"}, "carla", "part-acme", plant)
            globex = await _read(database, {"tenant": "globex"}, parts)

    assert _traces(globex) == [], "globex reads acme's row"


async def test_the_schema_assertion_refuses_a_partition_registered_with_other_scopes(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Security review: ``assert_scoped_schema`` requires a partition's scopes to be its root's."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    await _partitioned(database)
    await _matching_partition(database)

    async with _forged_registry(database, "rv_parts_p", PARTITION_SCOPES):
        state = await _assert_schema(database)

    assert state == VIOLATION


async def test_scope_names_that_differ_only_in_case_are_refused(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """Security review: ``tenant`` and ``Tenant`` name the same ``loom.scope.*`` setting."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)

    refusal = await _register(
        database,
        "rv_cases_mixed",
        "tenant varchar(36) NOT NULL, tenant2 text NOT NULL, id integer NOT NULL, "
        "PRIMARY KEY (tenant, id)",
        [
            {"col": "tenant", "scope": "tenant", "on": "both"},
            {"col": "tenant2", "scope": "Tenant", "on": "read"},
        ],
    )

    assert refusal == REJECTED


@pytest.mark.parametrize(
    ("column", "stored", "matching", "session"),
    [
        ("tags text[]", "{red}", "{red}", "{}"),
        ("tags jsonb", '{"team": "a"}', '{"team": "a"}', "{}"),
        ("tags jsonb", '{"team": "a"}', '{"team": "a"}', "[]"),
        ("tags jsonb", '{"team": "a"}', '{"team": "a"}', "null"),
    ],
    ids=["array-empty", "jsonb-empty-object", "jsonb-empty-array", "jsonb-null"],
)
async def test_an_empty_container_session_value_matches_no_row(
    scoped_database: BootstrapFactory,
    tmp_path: Path,
    column: str,
    stored: str,
    matching: str,
    session: str,
) -> None:
    """Term 5: the comparison is equality, not ``@>`` containment, for non-scalar scope types."""
    database = await _bootstrap(scoped_database, tmp_path, _schema(), document=READING_ON)
    s = database.schema
    refusal = await _register(
        database,
        "rv_tagged",
        f"tenant varchar(36) NOT NULL, id integer NOT NULL, {column} NOT NULL, "
        "PRIMARY KEY (tenant, id)",
        [
            {"col": "tenant", "scope": "tenant", "on": "both"},
            {"col": "tags", "scope": "tags", "on": "read"},
        ],
    )
    if refusal != "none":
        assert refusal in GUARD_REFUSALS
        return
    await _write(
        database.write,
        {"tenant": "acme"},
        "carla",
        "tagged",
        f"INSERT INTO {s}.rv_tagged (tenant, id, tags) VALUES ('acme', 1, '{stored}')",
    )
    await _apply_reading(database)
    tagged = replace(ALL, table="rv_tagged")

    assert _traces(await _read(database, {"tenant": "acme", "tags": session}, tagged)) == []
    assert _traces(await _read(database, {"tenant": "acme", "tags": matching}, tagged)) == [
        "tagged"
    ]


# --- N19: a store with no boundary ---------------------------------------------------------------


async def test_a_store_without_a_boundary_returns_rows_of_its_unscoped_tables(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """N19: no scoped table and no ``B``: term 1 is empty and unscoped rows are returned."""
    database = await _bootstrap(
        scoped_database, tmp_path, _schema(), document=READING_NO_B, product=GLOBALS
    )
    s = database.schema
    await _write(
        database.write,
        {},
        "carla",
        "bulletin",
        f"INSERT INTO {s}.rv_bulletins (id, body) VALUES (1, 'b')",
    )
    await _apply_reading(database)

    rows = await _read(database, {})

    assert [(row["table_name"], row["trace_id"]) for row in rows] == [("rv_bulletins", "bulletin")]


# --- Window (review M3, US5 #4) ----------------------------------------------------------------


async def _windowed(factory: BootstrapFactory, tmp_path: Path) -> ScopedDatabase:
    """One acme task written now (``w0``) and two copies of its log row at now-40d and now-20d.

    The copies are inserted by the superuser: the trigger stamps ``now()``, so a
    past timestamp cannot come from a real write. The migrator creates the
    partitions for those months through the guard first.
    """
    database = await _bootstrap(factory, tmp_path, _schema(), document=READING_ON)
    s = database.schema
    guard = f"loom_guard_{s}"
    await _run(
        database.migrator,
        *(
            f"SELECT {guard}.create_audit_partition("
            f"{guard}.utc_month(now() - interval '{days} days'))"
            for days in (40, 20)
        ),
    )
    await _write(
        database.write,
        {"tenant": "acme", "editor": "ana"},
        "carla",
        "w0",
        f"INSERT INTO {s}.rv_tasks (tenant, id, editor, body) VALUES ('acme', 7, 'ana', 'w')",
    )
    columns = (
        "txid, schema_name, table_name, row_key, op, changed, subject, mechanism, on_behalf_of, "
        "elevated, scopes, db_user, app_name, client_addr, backend_pid, origin"
    )
    await _run(
        database.superuser,
        *(
            f"INSERT INTO {s}.audit_log (at, trace_id, {columns}) "
            f"SELECT now() - interval '{days} days', 'w{days}', {columns} "
            f"FROM {s}.audit_log WHERE trace_id = 'w0'"
            for days in (40, 20)
        ),
    )
    await _apply_reading(database)
    return database


def _window(**bounds: dt.datetime | None) -> Call:
    return replace(ALL, row_key=_key("acme", 7), **bounds)


async def test_without_bounds_the_window_is_the_31_days_ending_now(
    windowed: ScopedDatabase,
) -> None:
    """US5 #4, review M3: no bounds gives {now-20d, now}."""
    database = windowed

    assert _traces(await _read(database, {"tenant": "acme"}, _window())) == ["w0", "w20"]


async def test_a_lower_bound_alone_gives_the_31_days_from_it(windowed: ScopedDatabase) -> None:
    """Review M3: ``p_from`` = now-45d alone gives [now-45d, now-14d): {now-40d, now-20d}."""
    database = windowed
    call = _window(from_=_now() - dt.timedelta(days=45))

    assert _traces(await _read(database, {"tenant": "acme"}, call)) == ["w20", "w40"]


async def test_an_upper_bound_alone_gives_the_31_days_before_it(windowed: ScopedDatabase) -> None:
    """Review M3: ``p_to`` = now-30d alone gives [now-61d, now-30d): {now-40d}."""
    database = windowed
    call = _window(to=_now() - dt.timedelta(days=30))

    assert _traces(await _read(database, {"tenant": "acme"}, call)) == ["w40"]


async def test_both_bounds_select_the_half_open_range(windowed: ScopedDatabase) -> None:
    """FR-006: ``at >= p_from AND at < p_to``; a 31-day span is accepted."""
    database = windowed
    now = _now()
    call = _window(from_=now - dt.timedelta(days=41), to=now - dt.timedelta(days=10))

    assert _traces(await _read(database, {"tenant": "acme"}, call)) == ["w20", "w40"]


async def test_the_lower_bound_is_inclusive_and_the_upper_bound_exclusive(
    windowed: ScopedDatabase,
) -> None:
    """FR-006: a row exactly at ``p_from`` is returned; a row exactly at ``p_to`` is not."""
    database = windowed
    s = database.schema
    async with _connect(database.superuser) as connection:
        at = {
            row["trace_id"]: row["at"]
            for row in await connection.fetch(
                f"SELECT trace_id, at FROM {s}.audit_log WHERE trace_id IN ('w40', 'w20', 'w0')"
            )
        }

    exact = _window(from_=at["w40"], to=at["w20"])
    upper_on_w0 = _window(from_=at["w20"], to=at["w0"])

    assert _traces(await _read(database, {"tenant": "acme"}, exact)) == ["w40"]
    assert _traces(await _read(database, {"tenant": "acme"}, upper_on_w0)) == ["w20"]


# --- Backstops (FR-005) ------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("call", "state"),
    [
        (replace(ALL, subjects=["carla"]), INVALID_ARGUMENTS),
        (replace(ALL, all=False, subjects=None), INVALID_ARGUMENTS),
        (replace(ALL, all=None), INVALID_ARGUMENTS),
        (_subjects("carla", None), INVALID_ARGUMENTS),
        (_subjects("carla", ""), INVALID_ARGUMENTS),
        (_subjects("carla", cap=0), INVALID_ARGUMENTS),
        (replace(ALL, limit=102), INVALID_ARGUMENTS),
        (replace(ALL, limit=0), INVALID_ARGUMENTS),
        (
            replace(
                ALL,
                from_=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                to=dt.datetime(2026, 2, 2, tzinfo=dt.UTC),
            ),
            INVALID_ARGUMENTS,
        ),
        (_subjects("carla", "bob", "dave", cap=2), OVER_CAP),
        (_subjects("carla", "", "dave", cap=2), OVER_CAP),
        (_subjects("carla", "bob", cap=2), "none"),
        (replace(ALL, limit=101), "none"),
        (
            replace(
                ALL,
                from_=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
                to=dt.datetime(2026, 2, 1, tzinfo=dt.UTC),
            ),
            "none",
        ),
    ],
    ids=[
        "all-with-subjects",
        "neither-all-nor-subjects",
        "all-null",
        "null-element",
        "empty-element",
        "cap-below-one",
        "limit-102",
        "limit-0",
        "span-32-days",
        "over-the-cap",
        "over-the-cap-before-the-element-scan",
        "at-the-cap",
        "limit-101",
        "span-31-days",
    ],
)
async def test_the_function_refuses_invalid_arguments(
    seeded: ScopedDatabase, call: Call, state: str
) -> None:
    """FR-005 backstops: ``LG005`` for invalid arguments, ``LG004`` over ``p_cap``."""
    database = seeded

    assert await _refusal(database, ACME_NORTH, call) == state


# --- Actor term at SQL level ---------------------------------------------------------------------


async def test_a_subject_set_returns_only_those_actors_rows(seeded: ScopedDatabase) -> None:
    """Actor term: ``p_subjects`` narrows to rows whose ``subject`` is in the set."""
    database = seeded

    bob = await _read(database, ACME_SOUTH, _subjects("bob"))
    carla = await _read(database, ACME_SOUTH, _subjects("carla", "dave"))
    nobody = await _read(database, ACME_SOUTH, _subjects("erin"))

    assert _traces(bob) == ["as-after-move", "as-insert"]
    assert _traces(carla) == ["task-acme"]
    assert nobody == []


async def test_all_subjects_returns_every_actor_within_the_scopes(seeded: ScopedDatabase) -> None:
    """Actor term: ``p_all`` returns every subject, still within terms 1 to 5."""
    database = seeded

    rows = await _read(database, ACME_SOUTH)

    assert {row["subject"] for row in rows} == {"bob", "carla"}
    assert _traces(rows) == SEEN_ACME_SOUTH


async def test_a_subject_filter_outside_the_set_returns_nothing(seeded: ScopedDatabase) -> None:
    """Filters only narrow: ``p_subject`` outside ``p_subjects`` is an empty page."""
    database = seeded

    rows = await _read(database, ACME_SOUTH, replace(_subjects("bob"), subject="carla"))

    assert rows == []


# --- Filters and keyset order (FR-006) ---------------------------------------------------------


@pytest.mark.parametrize(
    ("scopes", "call", "expected"),
    [
        (ACME_SOUTH, replace(ALL, subject="bob"), ["as-after-move", "as-insert"]),
        (ACME_NORTH, replace(ALL, table="rv_tasks"), ["task-acme"]),
        (ACME_NORTH, replace(ALL, tables=["rv_tasks", "rv_notes"]), ["task-acme"]),
        (
            ACME_NORTH,
            replace(ALL, table="rv_tickets", row_key=_key("acme", 1)),
            ["an-insert", "an-update"],
        ),
        (ACME_NORTH, replace(ALL, trace_id="an-update"), ["an-update"]),
        (ACME_NORTH, replace(ALL, op="U"), ["an-before-move", "an-update"]),
        (ACME_NORTH, replace(ALL, op="D"), []),
        (ACME_NORTH, replace(ALL, table="rv_never_audited"), []),
    ],
    ids=["subject", "table", "tables", "row-key", "trace-id", "op", "op-none", "unknown-table"],
)
async def test_filters_narrow_the_visible_rows(
    seeded: ScopedDatabase,
    scopes: Mapping[str, str],
    call: Call,
    expected: list[str],
) -> None:
    """FR-006 filters, each an equality on the visible rows."""
    database = seeded

    assert _traces(await _read(database, scopes, call)) == expected


@pytest.mark.parametrize("ascending", [False, True], ids=["descending", "ascending"])
async def test_keyset_pages_follow_at_then_id(seeded: ScopedDatabase, ascending: bool) -> None:
    """FR-006: order by ``(at, id)`` in the asked direction; the cursor resumes strictly after."""
    database = seeded
    order = replace(ALL, ascending=ascending)

    every = await _read(database, ACME_NORTH, order)
    keys = [(row["at"], row["id"]) for row in every]
    pages: list[tuple[dt.datetime, int]] = []
    call = replace(order, limit=2)
    while True:
        page = await _read(database, ACME_NORTH, call)
        pages.extend((row["at"], row["id"]) for row in page)
        if len(page) < 2:
            break
        call = replace(call, after_at=page[-1]["at"], after_id=page[-1]["id"])

    assert len(every) == len(SEEN_ACME_NORTH)
    assert keys == sorted(keys, reverse=not ascending)
    assert pages == keys
