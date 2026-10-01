"""``create_schema`` against a real Postgres (T015, FR-014, A11)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.rls import create_schema
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = pytest.mark.integration


async def _scalar(url: str, sql: str) -> object:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            return (await conn.execute(text(sql))).scalar()
    finally:
        await engine.dispose()


async def test_create_schema_creates_and_protects_every_scoped_table(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(notes.SCHEMA)
    application = application_for(notes, database, tmp_path)

    await create_schema(database.migrator, application)

    registered = "SELECT count(*) FROM loom_guard_notes.scoped_table"
    assert await _scalar(database.superuser, registered) == 3
    asserted = "SELECT 1 FROM (SELECT loom_guard_notes.assert_scoped_schema()) AS guard"
    assert await _scalar(database.superuser, asserted) == 1
    assert await _scalar(database.read, "SELECT count(*) FROM notes.notes") == 0


async def test_create_schema_is_idempotent(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("notes_twice")
    application = application_for(notes, database, tmp_path, schema="notes_twice")

    await create_schema(database.migrator, application)
    await create_schema(database.migrator, application)

    registered = "SELECT count(*) FROM loom_guard_notes_twice.scoped_table"
    assert await _scalar(database.superuser, registered) == 3


async def test_create_schema_without_the_bootstrap_names_the_entry_point(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("notes_boot")
    application = application_for(notes, database, tmp_path, schema="missing_guard")

    with pytest.raises(ConfigError, match=r"apply_bootstrap|loom schema bootstrap"):
        await create_schema(database.migrator, application)
