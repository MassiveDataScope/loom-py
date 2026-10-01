"""``create_schema`` against a real Postgres (T015, FR-014, A11)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text

from loom.core.config import ConfigError
from loom.core.repository.sqlalchemy.rls import create_schema
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import BootstrapFactory, application_for, execute, scalar

pytestmark = pytest.mark.integration


async def test_create_schema_creates_and_protects_every_scoped_table(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(notes.SCHEMA)
    application = application_for(notes, database, tmp_path)

    await create_schema(database.migrator, application)

    registered = "SELECT count(*) FROM loom_guard_notes.scoped_table"
    assert await scalar(database.superuser, registered) == 3
    asserted = "SELECT 1 FROM (SELECT loom_guard_notes.assert_scoped_schema()) AS guard"
    assert await scalar(database.superuser, asserted) == 1
    assert await scalar(database.read, "SELECT count(*) FROM notes.notes") == 0


async def test_create_schema_is_idempotent(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("notes_twice")
    application = application_for(notes, database, tmp_path, schema="notes_twice")

    await create_schema(database.migrator, application)
    await create_schema(database.migrator, application)

    registered = "SELECT count(*) FROM loom_guard_notes_twice.scoped_table"
    assert await scalar(database.superuser, registered) == 3


async def test_create_schema_without_the_bootstrap_names_the_entry_point(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("notes_boot")
    application = application_for(notes, database, tmp_path, schema="missing_guard")

    with pytest.raises(ConfigError, match=r"apply_bootstrap|loom schema bootstrap"):
        await create_schema(database.migrator, application)


async def test_the_standard_backend_wires_scopes_and_refuses_bypass_connections(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    from loom.core.config import ConfigContext
    from loom.core.identity import Identity, reset_identity, set_identity
    from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
    from loom.core.repository.sqlalchemy.rls.sources import (
        clear_scope_sources,
        register_scope_source,
    )

    database = await scoped_database("notes_wired")
    application = application_for(notes, database, tmp_path, schema="notes_wired")
    await create_schema(database.migrator, application)
    seed = (
        "INSERT INTO notes_wired.notes (owner_id, editor, body) "
        "VALUES ('u1', 'e', 'mine'), ('u2', 'e', 'theirs')"
    )
    await execute(database.bypass, seed)

    def backend(url: str):
        config = {
            "app": {"name": "wired"},
            "database": {
                "url": url,
                "schema": {
                    "mode": "external",
                    "scopes": {"owner": "identity.subject", "editor": "request.editor"},
                },
            },
        }
        return SQLAlchemyBackend().build(ConfigContext.from_dict(config), notes.MODELS)

    register_scope_source("editor", lambda: None)
    try:
        wiring = backend(database.write)
        wiring.prepare_models(notes.MODELS)
        async with wiring.lifespan_init():
            manager = wiring.uow_factory._session_manager  # type: ignore[union-attr]
            token = set_identity(Identity(subject="u1"))
            try:
                async with manager.session() as session:
                    bodies = (await session.execute(text("SELECT body FROM notes"))).scalars()
                    assert list(bodies) == ["mine"]
            finally:
                reset_identity(token)
            async with manager.session() as session:
                anonymous = await session.execute(text("SELECT count(*) FROM notes"))
                assert anonymous.scalar() == 0

        bypass = backend(database.bypass)
        bypass.prepare_models(notes.MODELS)
        with pytest.raises(ConfigError, match="bypass"):
            async with bypass.lifespan_init():
                pass
    finally:
        clear_scope_sources()


async def test_a_disabled_guard_event_trigger_stops_create_schema_and_fails_verify(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    from loom.core.repository.sqlalchemy.rls import verify

    database = await scoped_database("notes_evt")
    application = application_for(notes, database, tmp_path, schema="notes_evt")
    await create_schema(database.migrator, application)
    await execute(database.superuser, "ALTER EVENT TRIGGER loom_guard_notes_evt_ddl DISABLE")

    report = await verify(database.superuser, application)
    assert "guard.event_triggers" in {finding.check for finding in report.findings}
    with pytest.raises(ConfigError, match="event trigger"):
        await create_schema(database.migrator, application)


async def test_external_mode_refuses_a_bypass_connection_without_scoped_tables(
    scoped_database: BootstrapFactory,
) -> None:
    from loom.core.config import ConfigContext
    from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
    from tests.unit.core.locator_fixtures.plain import Widget

    database = await scoped_database("plain_external")
    await execute(
        database.migrator,
        "SET ROLE plain_external_owner",
        "CREATE TABLE plain_external.widgets (id integer PRIMARY KEY, name text)",
    )
    config = {
        "app": {"name": "plain"},
        "database": {"url": database.bypass, "schema": {"mode": "external"}},
    }
    wiring = SQLAlchemyBackend().build(ConfigContext.from_dict(config), (Widget,))
    wiring.prepare_models((Widget,))

    with pytest.raises(ConfigError, match="bypass"):
        async with wiring.lifespan_init():
            pass
