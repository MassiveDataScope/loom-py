"""The audit log records who changed which row, with which request (spec 008, US1)."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import asyncpg
import msgspec
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.repository.sqlalchemy.rls import create_schema
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
    scalar,
)

pytestmark = pytest.mark.integration

ACTOR = (
    "SELECT set_config('loom.actor.subject', :subject, true), "
    "set_config('loom.actor.mechanism', 'jwt', true), "
    "set_config('loom.trace_id', :trace, true), "
    "set_config('loom.scope.tenant', :tenant, true)"
)
COLUMNS = "tenant, id, name, email, balance, currency, active, internal_note, api_token"
LOG = (
    "SELECT op, table_name, row_key, changed, subject, mechanism, trace_id, scopes, "
    "db_user, origin FROM {schema}.audit_log ORDER BY id"
)


async def _ledger(factory: BootstrapFactory, tmp_path: Path, schema: str) -> ScopedDatabase:
    database = await factory(schema)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=schema)
    )
    return database


async def _as_person(url: str, *statements: str, subject: str = "ana", trace: str = "t-1") -> None:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(ACTOR), {"subject": subject, "trace": trace, "tenant": "acme"}
            )
            for statement in statements:
                await connection.execute(text(statement))
    finally:
        await engine.dispose()


def _one(table: str) -> str:
    return f"INSERT INTO {table} ({COLUMNS}) VALUES ('acme', 1, 'Ana', 'a', 0, 'EUR', true, '', '')"


async def _log(database: ScopedDatabase) -> list[dict[str, Any]]:
    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(text(LOG.format(schema=database.schema)))
            return [dict(row._mapping) for row in result]
    finally:
        await engine.dispose()


async def test_create_schema_audits_the_marked_tables_and_prepares_two_partitions(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_setup")

    guard = "loom_guard_ledger_setup"
    audited = f"SELECT array_agg(rel::text ORDER BY rel::text) FROM {guard}.audited_table"
    partitions = f"SELECT count(*) FROM {guard}.audit_partition"
    assert await scalar(database.superuser, audited) == [
        "ledger_setup.accounts",
        "ledger_setup.memos",
    ]
    assert await scalar(database.superuser, partitions) == 2
    triggers = (
        "SELECT count(*) FROM pg_trigger WHERE tgname = 'loom_audit_record' "
        "AND tgrelid = 'ledger_setup.drafts'::regclass"
    )
    assert await scalar(database.superuser, triggers) == 0


async def test_insert_update_and_delete_record_the_person_the_request_and_the_change(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_rows")
    table = "ledger_rows.accounts"

    await _as_person(
        database.write,
        f"INSERT INTO {table} ({COLUMNS}) "
        "VALUES ('acme', 1, 'Ana', 'a@acme', 0, 'EUR', true, 'vip', 's3cret')",
    )
    await _as_person(
        database.write,
        f"UPDATE {table} SET name = 'Ana B', balance = 10 WHERE id = 1",
        subject="bea",
        trace="t-2",
    )
    await _as_person(database.write, f"DELETE FROM {table} WHERE id = 1", trace="t-3")

    inserted, updated, deleted = await _log(database)
    assert inserted["op"] == "I"
    assert inserted["row_key"] == {"tenant": "acme", "id": 1}
    assert inserted["changed"]["name"] == {"new": "Ana"}
    assert inserted["changed"]["internal_note"] == {"redacted": True}
    assert inserted["changed"]["api_token"] == {"redacted": True}
    assert (inserted["subject"], inserted["mechanism"], inserted["trace_id"]) == (
        "ana",
        "jwt",
        "t-1",
    )
    assert inserted["scopes"] == {"tenant": "acme"}
    assert inserted["db_user"] == "ledger_rows_rw"
    assert inserted["origin"] == "trigger"
    assert updated["changed"] == {
        "name": {"old": "Ana", "new": "Ana B"},
        "balance": {"old": 0, "new": 10},
    }
    assert (updated["subject"], updated["trace_id"]) == ("bea", "t-2")
    assert deleted["op"] == "D"
    assert deleted["changed"]["email"] == {"old": "a@acme"}
    assert deleted["changed"]["api_token"] == {"redacted": True}


async def test_an_update_that_changes_nothing_leaves_no_record(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_noop")
    table = "ledger_noop.accounts"
    await _as_person(
        database.write,
        _one(table),
    )

    await _as_person(database.write, f"UPDATE {table} SET name = 'Ana' WHERE id = 1")

    assert [row["op"] for row in await _log(database)] == ["I"]


async def test_a_change_to_a_secret_column_alone_is_recorded_as_redacted(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_rotate")
    table = "ledger_rotate.accounts"
    await _as_person(
        database.write,
        _one(table),
    )

    await _as_person(database.write, f"UPDATE {table} SET api_token = 'rotated' WHERE id = 1")

    rotation = (await _log(database))[-1]
    assert rotation["changed"] == {"api_token": {"redacted": True}}
    assert "rotated" not in json.dumps(rotation, default=str)


async def test_an_unscoped_audited_table_records_without_scopes(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_memo")

    await _as_person(database.write, "INSERT INTO ledger_memo.memos (body) VALUES ('hello')")
    await _as_person(database.write, "INSERT INTO ledger_memo.drafts (body) VALUES ('free')")

    (memo,) = await _log(database)
    assert (memo["table_name"], memo["scopes"], memo["changed"]["body"]) == (
        "memos",
        {},
        {"new": "hello"},
    )


async def test_audit_failure_rolls_back_business_write(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_fail")
    guard = "loom_guard_ledger_fail"
    current = (
        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
        "WHERE i.inhparent = 'ledger_fail.audit_log'::regclass "
        "AND c.relname = 'audit_log_p' || to_char(now(), 'YYYYMM')"
    )
    partition = await scalar(database.superuser, current)
    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        with pytest.raises(DBAPIError, match="no partition of relation"):
            async with engine.begin() as connection:
                await connection.execute(text(f"SELECT {guard}.enter_hatch()"))
                detach = (
                    f"ALTER TABLE ledger_fail.audit_log DETACH PARTITION ledger_fail.{partition}"
                )
                await connection.execute(text(detach))
                await connection.execute(text(_one("ledger_fail.accounts")))
    finally:
        await engine.dispose()

    assert await scalar(database.superuser, "SELECT count(*) FROM ledger_fail.accounts") == 0
    assert await scalar(database.superuser, "SELECT count(*) FROM ledger_fail.audit_log") == 0


async def test_write_outside_loom_is_recorded_without_actor(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_direct")

    await execute(database.bypass, "INSERT INTO ledger_direct.memos (body) VALUES ('by hand')")

    (row,) = await _log(database)
    assert (row["subject"], row["mechanism"], row["trace_id"]) == ("", "", "")
    assert row["db_user"] == "ledger_direct_ops"


async def test_a_forged_actor_is_recorded_next_to_the_true_connection(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_spoof")

    await _as_person(
        database.bypass, "INSERT INTO ledger_spoof.memos (body) VALUES ('forged')", subject="ceo"
    )

    (row,) = await _log(database)
    assert (row["subject"], row["db_user"]) == ("ceo", "ledger_spoof_ops")


async def test_unmarking_and_disabling_stop_the_audit_and_keep_the_history(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "ledger_off"
    database = await scoped_database(schema)
    application = application_for(bookkeeping, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    await _as_person(database.write, f"INSERT INTO {schema}.memos (body) VALUES ('kept')")
    guard = f"loom_guard_{schema}"
    audited = f"SELECT array_agg(rel::text ORDER BY rel::text) FROM {guard}.audited_table"

    without_memo = {key: table for key, table in application.audited.items() if key[1] != "memos"}
    await create_schema(database.migrator, replace(application, audited=without_memo))
    assert await scalar(database.superuser, audited) == [f"{schema}.accounts"]
    await _as_person(database.write, f"INSERT INTO {schema}.memos (body) VALUES ('not audited')")

    schema_config = msgspec.structs.replace(
        application.database.schema,
        audit=msgspec.structs.replace(application.database.schema.audit, enabled=False),
    )
    disabled = replace(
        application, database=msgspec.structs.replace(application.database, schema=schema_config)
    )
    await create_schema(database.migrator, disabled)
    assert await scalar(database.superuser, audited) is None
    triggers = (
        "SELECT count(*) FROM pg_trigger WHERE tgname = 'loom_audit_record' "
        f"AND tgrelid::regclass::text LIKE '{schema}.%'"
    )
    assert await scalar(database.superuser, triggers) == 0

    await create_schema(database.migrator, application)
    assert await scalar(database.superuser, audited) == [f"{schema}.accounts", f"{schema}.memos"]
    assert [row["changed"]["body"]["new"] for row in await _log(database)] == ["kept"]


async def test_the_application_and_bypass_users_cannot_become_auditors(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await _ledger(scoped_database, tmp_path, "ledger_member")
    await execute(database.superuser, "GRANT ledger_member_auditor TO ledger_member_rw")

    with pytest.raises(asyncpg.InsufficientPrivilegeError, match="unexpected role memberships"):
        await scoped_database("ledger_member")
