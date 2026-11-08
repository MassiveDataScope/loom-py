"""Audit log partitions: a year ahead on deploy, in UTC months, dropped one at a time (FR-017)."""

from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.repository.sqlalchemy.rls import create_schema, ensure_range_partitions, verify
from loom.core.repository.sqlalchemy.rls.audit import (
    AUDIT_HORIZON_MONTHS,
    drop_expired_audit_partition,
    ensure_audit_partitions,
)
from loom.core.repository.sqlalchemy.rls.integrity import audit_months_covered
from tests.integration.agnosticism import bookkeeping, journal
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
    scalar,
)
from tests.integration.rls.test_audit import ACTOR

pytestmark = pytest.mark.integration

OLD = (
    ("audit_log_p202401", "2024-01-01", "2024-02-01"),
    ("audit_log_p202402", "2024-02-01", "2024-03-01"),
)


async def _drop_expired(migrator_url: str) -> str | None:
    """Drop the oldest expired partition in one migrator transaction, as the CLI does."""
    engine = create_async_engine(migrator_url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            return await drop_expired_audit_partition(connection)
    finally:
        await engine.dispose()


async def _ensure(migrator_url: str) -> int:
    """Run ``loom schema audit ensure-partitions``'s transaction as the migrator."""
    engine = create_async_engine(migrator_url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            return await ensure_audit_partitions(connection)
    finally:
        await engine.dispose()


async def _months_covered(database: ScopedDatabase) -> int | None:
    engine = create_async_engine(database.read, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            return await audit_months_covered(connection, database.schema)
    finally:
        await engine.dispose()


async def _partitions(database: ScopedDatabase) -> list[str]:
    names = await scalar(
        database.superuser,
        "SELECT string_agg(c.relname, ',' ORDER BY c.relname) FROM pg_inherits i "
        "JOIN pg_class c ON c.oid = i.inhrelid "
        f"WHERE i.inhparent = '{database.schema}.audit_log'::regclass",
    )
    return [] if names is None else str(names).split(",")


async def _with_old_partitions(
    factory: BootstrapFactory, tmp_path: Path, schema: str, **options: object
) -> ScopedDatabase:
    database = await factory(schema, **options)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=schema)
    )
    await execute(
        database.migrator,
        *(f"SELECT create_audit_partition('{low} 00:00:00+00')" for _, low, _ in OLD),
    )
    return database


async def test_without_retention_nothing_is_ever_dropped(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _with_old_partitions(scoped_database, tmp_path, "books_keep")

    assert await _drop_expired(database.migrator) is None
    with pytest.raises(DBAPIError, match="no audit retention is configured"):
        await execute(database.migrator, "SELECT drop_expired_audit_partition('audit_log_p202401')")
    assert "audit_log_p202401" in await _partitions(database)


async def test_with_retention_one_call_drops_only_the_oldest_expired_partition(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "books_prune"
    database = await _with_old_partitions(
        scoped_database, tmp_path, schema, audit_retention_months=12
    )
    application = application_for(bookkeeping, database, tmp_path, schema=schema)
    current = f"audit_log_p{dt.datetime.now(dt.UTC):%Y%m}"

    listed = (await verify(database.superuser, application)).expired_audit_partitions
    with pytest.raises(DBAPIError, match="within the 12-month retention"):
        await execute(database.migrator, f"SELECT drop_expired_audit_partition('{current}')")
    first = await _drop_expired(database.migrator)
    after_first = await _partitions(database)
    second = await _drop_expired(database.migrator)

    assert listed == ("audit_log_p202401", "audit_log_p202402")
    assert first == "audit_log_p202401"
    assert "audit_log_p202402" in after_first
    assert second == "audit_log_p202402"
    assert current in await _partitions(database)
    assert await _drop_expired(database.migrator) is None


async def test_detach_partition_refuses_the_audit_log(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _with_old_partitions(scoped_database, tmp_path, "books_detach")

    with pytest.raises(DBAPIError, match="audit"):
        await execute(
            database.migrator,
            "SELECT detach_partition("
            "'books_detach.audit_log'::regclass, 'audit_log_p202401', true)",
        )


async def test_a_partition_of_a_scoped_audited_parent_records_each_change_once(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(journal.SCHEMA)
    application = application_for(journal, database, tmp_path)
    await create_schema(database.migrator, application)
    today = dt.date.today().replace(day=1)
    following = (today + dt.timedelta(days=32)).replace(day=1)
    await ensure_range_partitions(database.migrator, application, journal.Entry, today, following)
    partition = f"entries_p{today:%Y%m}"

    triggers = await scalar(
        database.superuser,
        "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'loom_audit_record%' "
        f"AND tgrelid = '{journal.SCHEMA}.{partition}'::regclass",
    )
    engine = create_async_engine(database.write, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(ACTOR), {"subject": "ana", "trace": "t", "tenant": "acme"}
            )
            await connection.execute(
                text("INSERT INTO entries (tenant, at, body) VALUES ('acme', now(), 'hello')")
            )
    finally:
        await engine.dispose()
    recorded = await scalar(
        database.superuser,
        f"SELECT count(*) FROM {journal.SCHEMA}.audit_log WHERE table_name = 'entries'",
    )

    assert triggers == 1
    assert recorded == 1


async def test_deploy_covers_a_year_and_ensure_partitions_is_idempotent(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "books_horizon"
    database = await scoped_database(schema)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=schema)
    )

    created_again = await _ensure(database.migrator)

    assert created_again == 0
    assert await _months_covered(database) == AUDIT_HORIZON_MONTHS + 1


async def test_two_concurrent_ensures_create_each_missing_month_once(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "books_race"
    database = await scoped_database(schema)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=schema)
    )

    async def ensure_two_years() -> None:
        await execute(database.migrator, "SELECT ensure_audit_partitions(24)")

    await asyncio.gather(ensure_two_years(), ensure_two_years())

    assert await _months_covered(database) == 25


async def test_partitions_are_utc_months_whatever_the_session_time_zone(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "books_utc"
    database = await scoped_database(schema)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=schema)
    )

    await execute(
        database.migrator,
        "SET TIME ZONE 'Pacific/Kiritimati'",
        "SELECT create_audit_partition('2030-05-01 00:00:00+00')",
    )
    bounds = await scalar(
        database.superuser,
        "SELECT lower_bound = '2030-05-01 00:00:00+00' AND upper_bound = '2030-06-01 00:00:00+00' "
        f"FROM loom_guard_{schema}.audit_partition p "
        f"WHERE p.rel = '{schema}.audit_log_p203005'::regclass",
    )

    assert bounds is True
    with pytest.raises(DBAPIError, match="is not the start of a UTC month"):
        await execute(database.migrator, "SELECT create_audit_partition('2030-07-01 00:00:00+14')")
