"""DDL that escaped the per-guard relevance of T12c, or its assertion (T12c fix round).

Each case runs its statements in one transaction as the superuser. Every
statement passes; the guard refuses the transaction at COMMIT with ``LG002``.
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
from tests.integration.rls.conftest import (
    BootstrapFactory,
    ScopedDatabase,
    application_for,
    execute,
)

pytestmark = pytest.mark.integration


async def _created(factory: BootstrapFactory, tmp_path: Path, schema: str) -> ScopedDatabase:
    database = await factory(schema)
    application = application_for(bookkeeping, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    return database


async def _refused_at_commit(url: str, *statements: str) -> str:
    """Run *statements* in one transaction; each must pass and COMMIT must fail with LG002."""
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            for statement in statements:
                await connection.execute(text(statement))
            with pytest.raises(DBAPIError) as refused:
                await connection.commit()
    finally:
        await engine.dispose()
    assert getattr(refused.value.orig, "sqlstate", None) == "LG002", refused.value
    return str(refused.value)


# --- F1: renaming the store schema -------------------------------------------------------------


async def test_renaming_the_store_schema_is_refused_at_commit(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """The guard finds its schema by name; once it no longer resolves, it fails closed."""
    schema = "esc_rename_schema"
    database = await _created(scoped_database, tmp_path, schema)

    await _refused_at_commit(
        database.superuser,
        f"ALTER SCHEMA {schema} RENAME TO {schema}_moved",
        f"CREATE VIEW public.{schema}_leak AS SELECT * FROM {schema}_moved.audit_log",
        f"GRANT SELECT ON public.{schema}_leak TO {schema}_readers",
    )


# --- F2: the audit log is anchored by oid ------------------------------------------------------


async def test_moving_the_audit_log_to_another_schema_is_refused_at_commit(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "esc_log_schema"
    database = await _created(scoped_database, tmp_path, schema)
    await execute(database.superuser, f"CREATE SCHEMA {schema}_x")

    await _refused_at_commit(
        database.superuser,
        f"ALTER TABLE {schema}.audit_log SET SCHEMA {schema}_x",
        f"GRANT SELECT ON {schema}_x.audit_log TO {schema}_readers",
    )


async def test_renaming_the_audit_log_without_audited_tables_is_refused_at_commit(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """With no audited table left, a missing log used to pass the assertion."""
    schema = "esc_log_rename"
    database = await _created(scoped_database, tmp_path, schema)
    guard = f"loom_guard_{schema}"
    await execute(
        database.migrator,
        f"SELECT {guard}.unprotect_audited_table('{schema}.accounts')",
        f"SELECT {guard}.unprotect_audited_table('{schema}.memos')",
    )

    await _refused_at_commit(
        database.superuser, f"ALTER TABLE {schema}.audit_log RENAME TO audit_log_y"
    )


# --- F3: column grants on the audit log --------------------------------------------------------


async def test_a_column_grant_on_the_audit_log_is_refused_at_commit(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "esc_log_columns"
    database = await _created(scoped_database, tmp_path, schema)

    message = await _refused_at_commit(
        database.superuser,
        f"GRANT SELECT (subject, changed) ON {schema}.audit_log TO {schema}_readers",
    )

    assert "audit_log.subject" in message


# --- F4: a registered table under an unregistered parent -------------------------------------


async def test_attaching_a_registered_table_to_a_foreign_parent_is_refused_at_commit(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """The ATTACH reports the parent only; the guard finds its table below it."""
    schema = "esc_attach"
    database = await _created(scoped_database, tmp_path, schema)
    leak = f"public.{schema}_leak"

    message = await _refused_at_commit(
        database.superuser,
        f"CREATE TABLE {leak} (LIKE {schema}.accounts) PARTITION BY LIST (tenant)",
        f"ALTER TABLE {leak} ATTACH PARTITION {schema}.accounts DEFAULT",
    )

    assert "unregistered parent" in message


async def test_a_registered_table_inheriting_a_foreign_parent_is_refused_at_commit(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    schema = "esc_inherit"
    database = await _created(scoped_database, tmp_path, schema)
    leak = f"public.{schema}_leak"

    message = await _refused_at_commit(
        database.superuser,
        f"CREATE TABLE {leak} (LIKE {schema}.accounts)",
        f"ALTER TABLE {schema}.accounts INHERIT {leak}",
        f"GRANT SELECT ON {leak} TO {schema}_readers",
    )

    assert "unregistered parent" in message


# --- F5: deeper partitions ---------------------------------------------------------------------


async def test_disabling_the_audit_trigger_on_a_grandchild_elsewhere_is_refused_at_commit(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    """The partition two levels below an audited table, outside the store schema."""
    schema = "esc_grandchild"
    database = await _created(scoped_database, tmp_path, schema)
    guard = f"loom_guard_{schema}"
    mid, leaf = f"public.{schema}_mid", f"public.{schema}_leaf"
    await execute(
        database.migrator,
        f"SELECT {guard}.open_hatch()",
        f"CREATE TABLE {schema}.events (id int NOT NULL, at date NOT NULL, PRIMARY KEY (id, at)) "
        "PARTITION BY RANGE (at)",
        f"SELECT {guard}.protect_audited_table('{schema}.events', ARRAY[]::text[])",
    )
    await execute(
        database.superuser,
        f"CREATE TABLE {mid} PARTITION OF {schema}.events "
        "FOR VALUES FROM ('2000-01-01') TO ('2100-01-01') PARTITION BY RANGE (at)",
        f"CREATE TABLE {leaf} PARTITION OF {mid} FOR VALUES FROM ('2000-01-01') TO ('2100-01-01')",
    )

    message = await _refused_at_commit(
        database.superuser, f"ALTER TABLE {leaf} DISABLE TRIGGER loom_audit_record"
    )

    assert f"{schema}_leaf" in message
