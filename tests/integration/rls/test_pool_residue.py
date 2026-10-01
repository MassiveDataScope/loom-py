"""Session-level residue never crosses transactions or pooled connections (T016, FR-012, A4)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.repository.sqlalchemy.rls import (
    create_schema,
    install_pool_reset,
    register_scope_source,
    rls_session_settings,
)
from loom.core.repository.sqlalchemy.rls.sources import clear_scope_sources
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = pytest.mark.integration

U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"
FLAG = "SELECT coalesce(current_setting('loom.scope.editor.any', true), '')"


@pytest.fixture(autouse=True)
def _editor_source() -> Iterator[None]:
    clear_scope_sources()
    register_scope_source("editor", lambda: "ana")
    yield
    clear_scope_sources()


async def _seed(bypass_url: str) -> None:
    engine = create_async_engine(bypass_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO notes (owner_id, editor, body) VALUES "
                    f"('{U1}', 'ana', 'a'), ('{U1}', 'bob', 'b'), ('{U2}', 'zoe', 'z')"
                )
            )
    finally:
        await engine.dispose()


async def test_every_key_is_re_emitted_and_reset_all_runs_when_the_connection_returns(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(notes.SCHEMA)
    application = application_for(notes, database, tmp_path)
    await create_schema(database.migrator, application)
    await _seed(database.bypass)

    manager = SessionManager(
        database.write,
        session_settings=rls_session_settings(application),
        pool_size=1,
        max_overflow=0,
    )
    install_pool_reset(manager.engine)
    token = set_identity(Identity(subject=U1))
    try:
        async with manager.session() as session:
            residue = (
                "SELECT set_config('loom.scope.editor.any', 'on', false), "
                f"set_config('loom.scope.owner', '{U2}', false)"
            )
            await session.execute(text(residue))
            await session.commit()

        async with manager.session() as session:
            flag = (await session.execute(text(FLAG))).scalar()
            rows = (await session.execute(text("SELECT count(*) FROM notes"))).scalar()
            assert flag == ""
            assert rows == 2

        async with manager.engine.connect() as raw:
            assert (await raw.execute(text(FLAG))).scalar() == ""
            owner = "SELECT coalesce(current_setting('loom.scope.owner', true), '')"
            assert (await raw.execute(text(owner))).scalar() == ""
    finally:
        reset_identity(token)
        await manager.dispose()
