from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import ProgrammingError

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.model import BaseModel, Cardinality, ColumnField, ProjectionField, RelationField
from loom.core.projection.loaders import CountLoader, ExistsLoader
from loom.core.repository import PageParams
from loom.core.repository.abc import CursorCodec
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

pytestmark = pytest.mark.integration


class Customer(BaseModel):
    __tablename__ = "loom_it_pl_customers"

    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=64)


class Comment(BaseModel):
    __tablename__ = "loom_it_pl_comments"

    id: int = ColumnField(primary_key=True)
    ticket_id: int = ColumnField(foreign_key="loom_it_pl_tickets.id")
    body: str = ColumnField(length=200)


class Watcher(BaseModel):
    __tablename__ = "loom_it_pl_watchers"

    id: int = ColumnField(primary_key=True)
    ticket_id: int = ColumnField(foreign_key="loom_it_pl_tickets.id")
    login: str = ColumnField(length=64)


class Ticket(BaseModel):
    __tablename__ = "loom_it_pl_tickets"

    id: int = ColumnField(primary_key=True)
    title: str = ColumnField(length=64)
    customer_id: int = ColumnField(foreign_key="loom_it_pl_customers.id")

    comments: list[Comment] = RelationField(
        foreign_key="ticket_id", cardinality=Cardinality.ONE_TO_MANY, profiles=("detail",)
    )
    customer: Customer | None = RelationField(
        foreign_key="loom_it_pl_customers.id",
        cardinality=Cardinality.MANY_TO_ONE,
        profiles=("detail",),
    )
    watchers: list[Watcher] = RelationField(
        foreign_key="ticket_id", cardinality=Cardinality.ONE_TO_MANY, profiles=("detail",)
    )

    comment_count: int = ProjectionField(
        loader=CountLoader(model=Comment), profiles=("counts",), default=0
    )
    has_watchers: bool = ProjectionField(
        loader=ExistsLoader(model=Watcher), profiles=("counts",), default=False
    )


_SEED = (
    "INSERT INTO loom_it_pl_customers VALUES (1, 'acme'), (2, 'globex')",
    "INSERT INTO loom_it_pl_tickets VALUES (1, 't1', 1), (2, 't2', 2)",
    "INSERT INTO loom_it_pl_comments VALUES (1, 1, 'c1'), (2, 1, 'c2'), (3, 2, 'c3')",
    "INSERT INTO loom_it_pl_watchers VALUES (1, 1, 'ann')",
)

_PAGE = PageParams(limit=20)


class StatementLog:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0

    def before(self, *args: Any) -> None:
        self.statements.append(args[2])
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)

    def after(self, *args: Any) -> None:
        self.in_flight -= 1


@pytest.fixture
async def manager(pg_admin_uri: str) -> AsyncIterator[SessionManager]:
    reset_registry()
    compile_all(Ticket)
    session_manager = SessionManager(pg_admin_uri, pool_size=1, max_overflow=0, pool_pre_ping=False)
    async with session_manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().drop_all)
        await conn.run_sync(get_metadata().create_all)
        for statement in _SEED:
            await conn.execute(text(statement))
    try:
        yield session_manager
    finally:
        async with session_manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await session_manager.dispose()
        reset_registry()


def _log_statements(manager: SessionManager) -> StatementLog:
    log = StatementLog()
    event.listen(manager.engine.sync_engine, "before_cursor_execute", log.before)
    event.listen(manager.engine.sync_engine, "after_cursor_execute", log.after)
    return log


async def _execute(manager: SessionManager, statement: str) -> None:
    async with manager.engine.begin() as conn:
        await conn.execute(text(statement))


async def test_relation_profile_runs_one_statement_per_relation_one_at_a_time(
    manager: SessionManager,
) -> None:
    repo = RepositorySQLAlchemy(manager, Ticket, cursor_codec=CursorCodec(signing="ephemeral"))
    log = _log_statements(manager)

    page = await repo.list_paginated(_PAGE, profile="detail")

    first, second = page.items
    assert [c.body for c in first.comments] == ["c1", "c2"]
    assert first.customer.name == "acme"
    assert [w.login for w in first.watchers] == ["ann"]
    assert [c.body for c in second.comments] == ["c3"]
    assert second.customer.name == "globex"
    assert second.watchers == []
    assert len(log.statements) == 1 + 3
    assert log.max_in_flight == 1


async def test_projection_profile_runs_one_statement_per_projection_one_at_a_time(
    manager: SessionManager,
) -> None:
    repo = RepositorySQLAlchemy(manager, Ticket, cursor_codec=CursorCodec(signing="ephemeral"))
    log = _log_statements(manager)

    page = await repo.list_paginated(_PAGE, profile="counts")

    assert [(t.comment_count, t.has_watchers) for t in page.items] == [(2, True), (1, False)]
    assert len(log.statements) == 1 + 2
    assert log.max_in_flight == 1


async def test_failing_relation_step_raises_its_error_and_the_engine_stays_usable(
    manager: SessionManager,
) -> None:
    await _execute(manager, "ALTER TABLE loom_it_pl_comments RENAME COLUMN body TO gone")
    repo = RepositorySQLAlchemy(manager, Ticket, cursor_codec=CursorCodec(signing="ephemeral"))

    with pytest.raises(ProgrammingError, match="body"):
        await repo.list_paginated(_PAGE, profile="detail")

    page = await repo.list_paginated(_PAGE)
    assert [t.title for t in page.items] == ["t1", "t2"]


async def test_failing_projection_step_raises_its_error_and_the_engine_stays_usable(
    manager: SessionManager,
) -> None:
    await _execute(manager, "ALTER TABLE loom_it_pl_comments RENAME COLUMN ticket_id TO gone")
    repo = RepositorySQLAlchemy(manager, Ticket, cursor_codec=CursorCodec(signing="ephemeral"))

    with pytest.raises(ProgrammingError, match="ticket_id"):
        await repo.list_paginated(_PAGE, profile="counts")

    page = await repo.list_paginated(_PAGE)
    assert [t.title for t in page.items] == ["t1", "t2"]
