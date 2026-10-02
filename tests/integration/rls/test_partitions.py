"""Range-partitioned scoped tables: declared on the model, partitioned through the guard.

``notes.NoteEvent`` declares ``__partition_by__ = ("RANGE", "at")``. Every test
installs its own copy of ``notes`` under a fresh schema, creates partitions
with the migrator and reads them as the application users.
"""

from __future__ import annotations

import datetime as dt
import secrets
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.config import ConfigError
from loom.core.locator import Application
from loom.core.repository.sqlalchemy.rls import (
    create_schema,
    detach_range_partitions,
    ensure_range_partitions,
    guard_manifest,
    verify,
)
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
    scalar,
)

pytestmark = pytest.mark.integration

U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"
JAN = dt.date(2026, 1, 1)
APR = dt.date(2026, 4, 1)
MONTHS = ("note_events_p202601", "note_events_p202602", "note_events_p202603")


async def _installed(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> tuple[str, ScopedDatabase, Application]:
    schema = f"np{secrets.token_hex(3)}"
    database = await scoped_database(schema)
    application = application_for(notes, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    return schema, database, application


async def _scoped_count(url: str, table: str, owner: str | None) -> int:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            if owner is not None:
                await connection.execute(
                    text("SELECT set_config('loom.scope.owner', :owner, true)"), {"owner": owner}
                )
            return int((await connection.execute(text(f"SELECT count(*) FROM {table}"))).scalar())
    finally:
        await engine.dispose()


async def _sqlstate(url: str, statement: str) -> str:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(statement))
    except DBAPIError as exc:
        return str(getattr(exc.orig, "sqlstate", "") or "")
    finally:
        await engine.dispose()
    return "none"


async def test_the_compiled_parent_is_range_partitioned_and_protected(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema, database, application = await _installed(scoped_database, tmp_path)

    strategy = (
        "SELECT p.partstrat::text || ':' || a.attname FROM pg_partitioned_table p "
        "JOIN pg_attribute a ON a.attrelid = p.partrelid AND a.attnum = p.partattrs[0] "
        f"WHERE p.partrelid = '{schema}.note_events'::regclass"
    )
    assert await scalar(database.superuser, strategy) == "r:at"
    registered = (
        f"SELECT count(*) FROM loom_guard_{schema}.scoped_table "
        f"WHERE rel = '{schema}.note_events'::regclass"
    )
    assert await scalar(database.superuser, registered) == 1
    report = await verify(database.superuser, application)
    assert report.ok, report.findings


async def test_partitions_are_created_registered_and_protected_idempotently(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema, database, application = await _installed(scoped_database, tmp_path)

    created = await ensure_range_partitions(
        database.migrator, application, notes.NoteEvent, JAN, APR
    )
    again = await ensure_range_partitions(
        database.migrator, application, "note_events", dt.date(2026, 2, 10), APR
    )

    assert created == MONTHS
    assert again == ()
    inherited = (
        f"SELECT count(*) FROM loom_guard_{schema}.scoped_table t "
        f"JOIN loom_guard_{schema}.scoped_table p ON p.rel = '{schema}.note_events'::regclass "
        "JOIN pg_inherits i ON i.inhrelid = t.rel AND i.inhparent = p.rel "
        "JOIN pg_class c ON c.oid = t.rel "
        "WHERE t.scopes = p.scopes AND t.privileges = p.privileges "
        "AND c.relrowsecurity AND c.relforcerowsecurity"
    )
    assert await scalar(database.superuser, inherited) == 3
    bounds = (
        "SELECT pg_get_expr(relpartbound, oid) FROM pg_class "
        f"WHERE oid = '{schema}.{MONTHS[1]}'::regclass"
    )
    assert await scalar(database.superuser, bounds) == (
        "FOR VALUES FROM ('2026-02-01 00:00:00+00') TO ('2026-03-01 00:00:00+00')"
    )
    report = await verify(database.superuser, application)
    assert report.ok, report.findings


async def test_g5_scoped_reads_through_the_parent_and_a_partition_are_filtered(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema, database, application = await _installed(scoped_database, tmp_path)
    await ensure_range_partitions(database.migrator, application, notes.NoteEvent, JAN, APR)
    await execute(
        database.bypass,
        f"INSERT INTO {schema}.note_events (owner_id, at, kind) VALUES "
        f"('{U1}', '2026-01-05', 'a'), ('{U1}', '2026-02-05', 'b'), ('{U2}', '2026-02-06', 'c')",
    )

    for url in (database.read, database.write):
        assert await _scoped_count(url, f"{schema}.note_events", U1) == 2
        assert await _scoped_count(url, f"{schema}.{MONTHS[1]}", U1) == 1
        assert await _scoped_count(url, f"{schema}.{MONTHS[1]}", U2) == 1
        assert await _scoped_count(url, f"{schema}.note_events", None) == 0
        assert await _scoped_count(url, f"{schema}.{MONTHS[0]}", None) == 0
    insert = (
        f"INSERT INTO {schema}.{MONTHS[0]} (owner_id, at, kind) VALUES ('{U1}', '2026-01-09', 'x')"
    )
    assert await _sqlstate(database.write, insert) == "42501"


async def test_the_application_users_cannot_create_or_detach_partitions(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema, database, application = await _installed(scoped_database, tmp_path)
    await ensure_range_partitions(database.migrator, application, notes.NoteEvent, JAN, APR)
    guard = f"loom_guard_{schema}"
    create = (
        f"SELECT {guard}.create_range_partition('{schema}.note_events', 'note_events_p202605', "
        "'2026-05-01', '2026-06-01')"
    )
    detach = f"SELECT {guard}.detach_partition('{schema}.note_events', '{MONTHS[0]}', true)"
    by_hand = (
        f"CREATE TABLE {schema}.note_events_p202605 PARTITION OF {schema}.note_events "
        "FOR VALUES FROM ('2026-05-01') TO ('2026-06-01')"
    )

    for url in (database.read, database.write):
        assert await _sqlstate(url, create) == "42501"
        assert await _sqlstate(url, detach) == "42501"
        assert await _sqlstate(url, by_hand) == "42501"
        with pytest.raises(ConfigError, match="migrator"):
            await ensure_range_partitions(
                url, application, notes.NoteEvent, APR, APR.replace(month=5)
            )
    assert await _sqlstate(database.bypass, create) == "42501"


async def test_partitions_created_in_a_rolled_back_transaction_do_not_exist(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema, database, application = await _installed(scoped_database, tmp_path)
    engine = create_async_engine(database.migrator, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            created = await ensure_range_partitions(
                connection, application, notes.NoteEvent, JAN, APR
            )
            assert created == MONTHS
            path = "SELECT current_setting('search_path')"
            assert (
                await connection.execute(text(path))
            ).scalar() == f"{schema}, loom_guard_{schema}"
            await connection.rollback()
    finally:
        await engine.dispose()

    partitions = (
        f"SELECT count(*) FROM pg_inherits WHERE inhparent = '{schema}.note_events'::regclass"
    )
    assert await scalar(database.superuser, partitions) == 0


async def test_retention_detaches_or_drops_partitions_through_the_guard(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema, database, application = await _installed(scoped_database, tmp_path)
    await ensure_range_partitions(database.migrator, application, notes.NoteEvent, JAN, APR)
    await execute(
        database.bypass,
        f"INSERT INTO {schema}.note_events (owner_id, at, kind) VALUES "
        f"('{U1}', '2026-01-05', 'a'), ('{U1}', '2026-02-05', 'b'), ('{U1}', '2026-03-05', 'c')",
    )

    detached = await detach_range_partitions(
        database.migrator, application, notes.NoteEvent, JAN, dt.date(2026, 2, 1)
    )
    dropped = await detach_range_partitions(
        database.migrator,
        application,
        notes.NoteEvent,
        dt.date(2026, 2, 1),
        dt.date(2026, 3, 1),
        drop=True,
    )
    repeated = await detach_range_partitions(
        database.migrator, application, notes.NoteEvent, JAN, dt.date(2026, 3, 1), drop=False
    )

    assert (detached, dropped, repeated) == ((MONTHS[0],), (MONTHS[1],), ())
    assert await _scoped_count(database.write, f"{schema}.note_events", U1) == 1
    assert await scalar(database.bypass, f"SELECT count(*) FROM {schema}.{MONTHS[0]}") == 1
    assert await scalar(database.superuser, f"SELECT to_regclass('{schema}.{MONTHS[1]}')") is None
    assert await _sqlstate(database.read, f"SELECT 1 FROM {schema}.{MONTHS[0]}") == "42501"
    registered = f"SELECT count(*) FROM loom_guard_{schema}.scoped_table"
    assert await scalar(database.superuser, registered) == 4
    with pytest.raises(DBAPIError, match="dropped by hand"):
        await detach_range_partitions(
            database.migrator, application, notes.NoteEvent, JAN, dt.date(2026, 2, 1), drop=True
        )
    report = await verify(database.superuser, application)
    assert report.ok, report.findings


async def test_a_guard_without_partition_support_is_reported_as_pending(
    scoped_database: BootstrapFactory, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = guard_manifest.GUARD_REVISIONS[:1]
    monkeypatch.setattr(guard_manifest, "GUARD_REVISIONS", first)
    schema, database, application = await _installed(scoped_database, tmp_path)

    with pytest.raises(ConfigError, match="guard revision pending"):
        await ensure_range_partitions(database.migrator, application, notes.NoteEvent, JAN, APR)

    monkeypatch.undo()
    database = await scoped_database(schema)
    created = await ensure_range_partitions(
        database.migrator, application, notes.NoteEvent, JAN, APR
    )
    assert created == MONTHS
    revisions = f"SELECT string_agg(n::text, ',' ORDER BY n) FROM loom_guard_{schema}.revision"
    assert await scalar(database.superuser, revisions) == "1,2"


async def test_verify_reports_a_partition_registered_unlike_its_parent(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema, database, application = await _installed(scoped_database, tmp_path)
    await ensure_range_partitions(database.migrator, application, notes.NoteEvent, JAN, APR)
    await execute(
        database.superuser,
        f"UPDATE loom_guard_{schema}.scoped_table SET privileges = ARRAY['SELECT', 'INSERT'] "
        f"WHERE rel = '{schema}.{MONTHS[0]}'::regclass",
    )

    report = await verify(database.superuser, application)

    assert {(f.table, f.check) for f in report.findings} == {(MONTHS[0], "partition.registration")}
