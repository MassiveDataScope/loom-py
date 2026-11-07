"""Startup refuses an audited application the database cannot serve (FR-018, SC-002)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.repository.sqlalchemy.rls import create_schema, guard_manifest
from loom.core.repository.sqlalchemy.rls.integrity import startup_problems
from tests.integration.agnosticism import bookkeeping
from tests.integration.rls.conftest import BootstrapFactory, ScopedDatabase, application_for
from tests.integration.rls.test_audit_verify import _drift

pytestmark = pytest.mark.integration


async def _problems(
    database: ScopedDatabase, *, audited: bool, recording: tuple[str, ...] = ()
) -> set[str]:
    engine = create_async_engine(database.write, poolclass=NullPool)
    try:
        async with engine.connect() as connection:
            problems = await startup_problems(
                connection,
                f"loom_guard_{database.schema}",
                {},
                audited=audited,
                recording=recording,
            )
    finally:
        await engine.dispose()
    return {problem.check for problem in problems}


async def test_an_audited_schema_starts_clean(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("books_start")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=database.schema)
    )

    assert await _problems(database, audited=True) == set()


async def test_a_marked_table_without_its_trigger_stops_startup(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("books_unmigrated")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=database.schema)
    )

    protected = await _problems(database, audited=True, recording=("accounts", "memos"))
    unmigrated = await _problems(database, audited=True, recording=("memos", "drafts"))

    assert protected == set()
    assert unmigrated == {"audit.trigger"}


async def test_a_changed_log_column_stops_startup_even_without_marked_tables(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("books_shape")
    await create_schema(
        database.migrator, application_for(bookkeeping, database, tmp_path, schema=database.schema)
    )
    await _drift(database, "ALTER TABLE books_shape.audit_log ADD COLUMN note text")
    try:
        assert "audit.shape" in await _problems(database, audited=False)
    finally:
        await _drift(database, "ALTER TABLE books_shape.audit_log DROP COLUMN note")


async def test_an_audited_application_on_revision_two_does_not_start(
    scoped_database: BootstrapFactory, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(guard_manifest, "GUARD_REVISIONS", guard_manifest.GUARD_REVISIONS[:2])
    database = await scoped_database("books_old")
    monkeypatch.undo()

    assert await _problems(database, audited=True) == {"audit.guard_revision"}
