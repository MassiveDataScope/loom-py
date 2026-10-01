"""Elevation against a real Postgres (T017): sequences S1 and S2 of the data model."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from loom.core.authz.elevation import elevation_scope
from loom.core.authz.product import clear_authz_product, register_authz_product
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.repository.sqlalchemy.rls import (
    SQLAlchemyElevationSink,
    create_schema,
    elevate,
    register_scope_source,
    rls_session_settings,
)
from loom.core.repository.sqlalchemy.rls.sources import clear_scope_sources
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import reset_active_session, set_active_session
from tests.integration.agnosticism import notes
from tests.integration.rls.conftest import BootstrapFactory, application_for

pytestmark = pytest.mark.integration

U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"
MANAGE = Permission("members.manage")
CATALOG = RoleCatalog((MANAGE,), (Role("admin", (MANAGE,)),))
FLAG = "SELECT coalesce(current_setting('loom.scope.editor.any', true), '')"


@dataclass
class Product:
    catalog: RoleCatalog = CATALOG
    delegate: Permission = MANAGE
    elevations: dict[str, Permission] = field(default_factory=lambda: {"editor": MANAGE})


@pytest.fixture(autouse=True)
def _wiring() -> Iterator[None]:
    clear_scope_sources()
    clear_authz_product()
    register_scope_source("editor", lambda: "ana")
    register_authz_product(Product())
    yield
    clear_scope_sources()
    clear_authz_product()


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


async def test_read_then_authorize_sets_the_flag_on_the_open_transaction_and_commit_ends_it(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database(notes.SCHEMA)
    application = application_for(notes, database, tmp_path)
    await create_schema(database.migrator, application)
    await _seed(database.bypass)

    manager = SessionManager(database.write, session_settings=rls_session_settings(application))
    decision = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))
    identity = set_identity(Identity(subject=U1))
    try:
        async with manager.session() as session:
            active = set_active_session(session)
            try:
                async with elevation_scope(owns_transaction=True, sink=SQLAlchemyElevationSink()):
                    assert (await session.execute(text("SELECT count(*) FROM notes"))).scalar() == 2
                    assert (await session.execute(text(FLAG))).scalar() == ""

                    await elevate("editor", decision, at=Scope.of(U1))

                    assert (await session.execute(text(FLAG))).scalar() == "on"
                    update = text("UPDATE notes SET body = 'edited' WHERE editor = 'bob'")
                    assert (await session.execute(update)).rowcount == 1
                    await session.commit()
            finally:
                reset_active_session(active)

        async with manager.session() as session:
            assert (await session.execute(text(FLAG))).scalar() == ""
            edited = text("SELECT count(*) FROM notes WHERE body = 'edited'")
            assert (await session.execute(edited)).scalar() == 1
    finally:
        reset_identity(identity)
        await manager.dispose()


async def test_elevation_without_an_open_transaction_arrives_with_the_next_one(
    scoped_database: BootstrapFactory, tmp_path: Path
) -> None:
    database = await scoped_database("notes_later")
    application = application_for(notes, database, tmp_path, schema="notes_later")
    await create_schema(database.migrator, application)
    await _seed(database.bypass)

    manager = SessionManager(database.write, session_settings=rls_session_settings(application))
    decision = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))
    identity = set_identity(Identity(subject=U1))
    try:
        async with elevation_scope(owns_transaction=True, sink=SQLAlchemyElevationSink()):
            await elevate("editor", decision, at=Scope.of(U1))
            async with manager.session() as session:
                assert (await session.execute(text(FLAG))).scalar() == "on"
    finally:
        reset_identity(identity)
        await manager.dispose()
