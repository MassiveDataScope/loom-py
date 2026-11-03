"""Elevation against a real Postgres (T017): sequences S1 and S2 of the data model."""

from __future__ import annotations

import secrets
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import CursorResult, text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from loom.core.authz import Decision, Grant, Permission, Role, RoleCatalog, Scope
from loom.core.authz.elevation import elevation_scope
from loom.core.authz.product import clear_authz_product, register_authz_product
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
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork
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
        async with SQLAlchemyUnitOfWork(manager):
            session = manager.active_session
            assert session is not None
            async with elevation_scope(
                owns_transaction=True, sink=SQLAlchemyElevationSink(manager)
            ):
                assert (await session.execute(text("SELECT count(*) FROM notes"))).scalar() == 2
                assert (await session.execute(text(FLAG))).scalar() == ""

                await elevate("editor", decision, at=Scope.of(U1))

                assert (await session.execute(text(FLAG))).scalar() == "on"
                update = text("UPDATE notes SET body = 'edited' WHERE editor = 'bob'")
                assert cast(CursorResult[Any], await session.execute(update)).rowcount == 1
                await session.commit()

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
        async with elevation_scope(owns_transaction=True, sink=SQLAlchemyElevationSink(manager)):
            await elevate("editor", decision, at=Scope.of(U1))
            async with manager.session() as session:
                assert (await session.execute(text(FLAG))).scalar() == "on"
    finally:
        reset_identity(identity)
        await manager.dispose()


_PID = "SELECT pg_backend_pid()"
_BODIES = "SELECT body FROM {schema}.notes WHERE owner_id = '" + U1 + "' ORDER BY body"


@dataclass(frozen=True)
class _Nested:
    """What the outer transaction saw around a nested execution that elevated."""

    flag_inside: str
    flag_after: str
    pid_before: int
    pid_after: int
    bodies: tuple[str, ...]


async def _nested_elevation(
    manager: SessionManager, decision: Decision
) -> tuple[str, str, int, int]:
    """Write, run a nested execution that elevates ``editor``, write again; one unit of work."""
    sink = SQLAlchemyElevationSink(manager)
    insert = "INSERT INTO notes (owner_id, editor, body) VALUES ('" + U1 + "', 'ana', :body)"
    async with (
        elevation_scope(owns_transaction=True, sink=sink),
        SQLAlchemyUnitOfWork(manager),
    ):
        session = manager.active_session
        assert session is not None
        await session.execute(text(insert), {"body": "before nested"})
        pid_before = (await session.execute(text(_PID))).scalar_one()
        async with elevation_scope(owns_transaction=False, sink=sink):
            await elevate("editor", decision, at=Scope.of(U1))
            flag_inside = (await session.execute(text(FLAG))).scalar_one()
        flag_after = (await session.execute(text(FLAG))).scalar_one()
        pid_after = (await session.execute(text(_PID))).scalar_one()
        await session.execute(text(insert), {"body": "after nested"})
    return flag_inside, flag_after, pid_before, pid_after


async def _bodies(url: str, schema: str) -> tuple[str, ...]:
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        async with engine.connect() as conn:
            return tuple((await conn.execute(text(_BODIES.format(schema=schema)))).scalars())
    finally:
        await engine.dispose()


@pytest.fixture
async def nested(scoped_database: BootstrapFactory, tmp_path: Path) -> _Nested:
    schema = f"notes_nested_{secrets.token_hex(3)}"
    database = await scoped_database(schema)
    application = application_for(notes, database, tmp_path, schema=schema)
    await create_schema(database.migrator, application)
    manager = SessionManager(database.write, session_settings=rls_session_settings(application))
    decision = Decision(allowed=True, grant=Grant(subject=U1, role="admin", scope=Scope.of(U1)))
    identity = set_identity(Identity(subject=U1))
    try:
        flag_inside, flag_after, pid_before, pid_after = await _nested_elevation(manager, decision)
    finally:
        reset_identity(identity)
        await manager.dispose()
    return _Nested(
        flag_inside=flag_inside,
        flag_after=flag_after,
        pid_before=pid_before,
        pid_after=pid_after,
        bodies=await _bodies(database.bypass, schema),
    )


class TestANestedExecutionThatElevates:
    """F1: clearing a nested frame's flag must never invalidate the outer invocation's session."""

    def test_its_flag_is_on_inside_the_nested_frame(self, nested: _Nested) -> None:
        assert nested.flag_inside == "on"

    def test_its_flag_is_cleared_when_the_nested_frame_closes(self, nested: _Nested) -> None:
        assert nested.flag_after == ""

    def test_the_outer_transaction_keeps_its_connection(self, nested: _Nested) -> None:
        assert nested.pid_after == nested.pid_before

    def test_the_writes_before_and_after_it_both_commit(self, nested: _Nested) -> None:
        assert nested.bodies == ("after nested", "before nested")
