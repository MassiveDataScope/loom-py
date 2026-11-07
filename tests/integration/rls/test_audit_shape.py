"""The audit log's shape is the contract a CDC consumer must also produce (FR-011, SC-006)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.repository.sqlalchemy.rls import create_schema
from loom.core.repository.sqlalchemy.rls.guard_manifest import AUDIT_LOG_COLUMNS
from loom.core.repository.sqlalchemy.rls.integrity import AUDIT_SHAPE
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = pytest.mark.integration

SCHEMA = "books_shape_contract"
CONSTRAINTS = (
    "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
    "WHERE conrelid = CAST(:log AS regclass) ORDER BY 1"
)
INDEXES = (
    "SELECT pg_get_indexdef(indexrelid) FROM pg_index "
    "WHERE indrelid = CAST(:log AS regclass) ORDER BY 1"
)
EXPECTED_CONSTRAINTS = (
    "CHECK ((op = ANY (ARRAY['I'::bpchar, 'U'::bpchar, 'D'::bpchar])))",
    "CHECK ((origin = ANY (ARRAY['trigger'::text, 'cdc'::text])))",
    "PRIMARY KEY (at, id)",
)
EXPECTED_INDEXES = (
    "CREATE INDEX audit_log_row_idx ON ONLY audit_log "
    "USING btree (schema_name, table_name, row_key)",
    "CREATE INDEX audit_log_scopes_idx ON ONLY audit_log USING gin (scopes)",
    "CREATE INDEX audit_log_trace_idx ON ONLY audit_log USING btree (trace_id)",
    "CREATE UNIQUE INDEX audit_log_pkey ON ONLY audit_log USING btree (at, id)",
)
EXPECTED_TRIGGERS = ("loom_audit_log_immutable_row A", "loom_audit_log_immutable_truncate A")
TRIGGERS = (
    "SELECT tgname || ' ' || tgenabled::text FROM pg_trigger "
    "WHERE tgrelid = CAST(:log AS regclass) AND NOT tgisinternal ORDER BY 1"
)


async def _lines(url: str, query: str, **parameters: str) -> tuple[str, ...]:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            rows = await connection.execute(text(query), parameters)
            return tuple(str(row[0]) for row in rows)
    finally:
        await engine.dispose()


async def test_the_log_has_exactly_the_contracted_shape(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(SCHEMA)
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=SCHEMA)
    )
    log = f"{SCHEMA}.audit_log"

    columns = await _lines(database.superuser, AUDIT_SHAPE, schema=SCHEMA)
    constraints = await _lines(database.superuser, CONSTRAINTS, log=log)
    indexes = tuple(
        line.replace(f"{SCHEMA}.", "")
        for line in await _lines(database.superuser, INDEXES, log=log)
    )
    triggers = await _lines(database.superuser, TRIGGERS, log=log)

    assert columns == AUDIT_LOG_COLUMNS
    assert constraints == EXPECTED_CONSTRAINTS
    assert indexes == EXPECTED_INDEXES
    assert triggers == EXPECTED_TRIGGERS
