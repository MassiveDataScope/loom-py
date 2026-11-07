"""Nobody but the trigger writes the audit log, and only the auditor reads it (SC-003, FR-005).

Every role a deploy hands out is tried against the log and against one of its
partitions addressed directly: the application's users, the bypass user, the
migrator acting as the schema's owner and the auditor role.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.repository.sqlalchemy.rls import create_schema
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase, application_for

pytestmark = pytest.mark.integration

SCHEMA = "ledger_ro"
ROW = (
    "INSERT INTO {rel} (at, txid, schema_name, table_name, row_key, op, changed, db_user, origin) "
    "VALUES (now(), pg_current_xact_id(), 's', 't', '{{}}', 'I', '{{}}', 'x', 'trigger')"
)
WRITES = {
    "insert": ROW,
    "update": "UPDATE {rel} SET subject = 'forged'",
    "delete": "DELETE FROM {rel}",
    "truncate": "TRUNCATE {rel}",
}


@pytest.fixture(scope="module")
def anyio_backend() -> str:
    return "asyncio"


async def _database(factory: BootstrapFactory, tmp_path: Path) -> tuple[ScopedDatabase, str]:
    database = await factory(SCHEMA)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=SCHEMA)
    )
    engine = create_async_engine(database.superuser, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(f"INSERT INTO {SCHEMA}.memos (body) VALUES ('seed')"))
            partition = (
                await connection.execute(
                    text(
                        "SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid "
                        f"WHERE i.inhparent = '{SCHEMA}.audit_log'::regclass ORDER BY 1 LIMIT 1"
                    )
                )
            ).scalar_one()
    finally:
        await engine.dispose()
    return database, f"{SCHEMA}.{partition}"


async def _run(url: str, statement: str, *, role: str | None = None) -> None:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.begin() as connection:
            if role is not None:
                await connection.execute(text(f"SET LOCAL ROLE {role}"))
            await connection.execute(text(statement))
    finally:
        await engine.dispose()


async def test_no_role_can_change_the_log_or_a_partition(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, partition = await _database(scoped_database, tmp_path)
    users = {
        "write": database.write,
        "read": database.read,
        "bypass": database.bypass,
        "migrator": database.migrator,
    }
    accepted: list[str] = []
    for rel in (f"{SCHEMA}.audit_log", partition):
        for who, url in users.items():
            for operation, statement in WRITES.items():
                if who == "migrator" and operation == "insert":
                    continue  # the schema's owner is trusted to insert; see below
                try:
                    await _run(url, statement.format(rel=rel))
                except DBAPIError:
                    continue
                accepted.append(f"{who} {operation} {rel}")
        for operation, statement in WRITES.items():
            try:
                await _run(database.superuser, statement.format(rel=rel), role=f"{SCHEMA}_auditor")
            except DBAPIError:
                continue
            accepted.append(f"auditor {operation} {rel}")

    assert accepted == []


async def test_only_the_auditor_and_the_owner_read_the_log(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database, partition = await _database(scoped_database, tmp_path)
    read = "SELECT count(*) FROM {rel}"
    refused: list[str] = []
    for rel in (f"{SCHEMA}.audit_log", partition):
        statement = read.format(rel=rel)
        for who, url in {
            "write": database.write,
            "read": database.read,
            "bypass": database.bypass,
        }.items():
            with pytest.raises(DBAPIError, match="permission denied"):
                await _run(url, statement)
            refused.append(who)
        await _run(database.superuser, statement, role=f"{SCHEMA}_auditor")
        await _run(database.migrator, statement)

    assert len(refused) == 6


@pytest.mark.parametrize(
    "statement",
    [
        "ALTER TABLE {log} DISABLE TRIGGER loom_audit_log_immutable_row",
        "DROP TRIGGER loom_audit_log_immutable_truncate ON {log}",
        "ALTER TABLE {schema}.accounts DISABLE TRIGGER loom_audit_record",
        "DROP TRIGGER loom_audit_record ON {schema}.memos",
        "ALTER TABLE {log} DETACH PARTITION {partition}",
        "DROP TABLE {partition}",
        "DROP TABLE {log}",
        "SELECT detach_partition('{log}'::regclass, '{partition_name}', true)",
    ],
)
async def test_the_owner_cannot_disable_detach_or_drop_the_audit(
    scoped_database: BootstrapFactory, tmp_path: Path, statement: str
) -> None:
    database, partition = await _database(scoped_database, tmp_path)
    sql = statement.format(
        log=f"{SCHEMA}.audit_log",
        schema=SCHEMA,
        partition=partition,
        partition_name=partition.split(".", 1)[1],
    )

    with pytest.raises(DBAPIError, match="loom_guard"):
        await _run(database.migrator, sql)
