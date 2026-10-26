from __future__ import annotations

import itertools
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import event

from loom.core.command import Command
from loom.core.model import BaseModel, ColumnField, ServerDefault
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import (
    reset_active_mutations,
    set_active_mutations,
)
from tests.unit.core.conftest import SessionManagerFactory

_token_counter = itertools.count(1)


def _next_token() -> str:
    return f"tok-{next(_token_counter)}"


class _Item(BaseModel):
    __tablename__ = "bulk_items"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    sku: str = ColumnField(length=32, unique=True)
    qty: int = ColumnField(default=0)
    note: str | None = ColumnField(nullable=True, default=None)
    token: str = ColumnField(length=32, default=_next_token)
    created_at: str = ColumnField(length=32, server_default=ServerDefault.NOW)


class _CreateItem(Command, frozen=True):
    sku: str
    qty: int = 0
    note: str | None = None
    token: str | None = None
    created_at: str | None = None


@pytest.fixture
async def session_manager(sqlite_manager_for: SessionManagerFactory) -> SessionManager:
    return await sqlite_manager_for(_Item)


@pytest.fixture
def repository(session_manager: SessionManager) -> RepositorySQLAlchemy[_Item, int]:
    return RepositorySQLAlchemy(
        session_manager=session_manager, model=_Item, cursor_codec=CursorCodec(signing="ephemeral")
    )


@pytest.fixture
def mutations() -> Iterator[list[MutationEvent]]:
    events, token = set_active_mutations()
    yield events
    reset_active_mutations(token)


@pytest.fixture
def statements(session_manager: SessionManager) -> Iterator[list[str]]:
    captured: list[str] = []

    def _capture(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool
    ) -> None:
        captured.append(statement)

    sync_engine = session_manager.engine.sync_engine
    event.listen(sync_engine, "before_cursor_execute", _capture)
    yield captured
    event.remove(sync_engine, "before_cursor_execute", _capture)


class TestCreateMany:
    async def test_empty_input_issues_no_statement(
        self, repository: RepositorySQLAlchemy[_Item, int], statements: list[str]
    ) -> None:
        created = await repository.create_many([])

        assert created == ()
        assert statements == []

    async def test_inserts_every_row_in_one_statement(
        self, repository: RepositorySQLAlchemy[_Item, int], statements: list[str]
    ) -> None:
        await repository.create_many([_CreateItem(sku=f"sku-{index}") for index in range(5)])

        inserts = [statement for statement in statements if statement.startswith("INSERT")]
        assert len(inserts) == 1
        assert await repository.count() == 5

    async def test_falls_back_to_reselect_when_dialect_lacks_returning(
        self,
        repository: RepositorySQLAlchemy[_Item, int],
        session_manager: SessionManager,
        statements: list[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(session_manager.engine.dialect, "insert_returning", False)

        created = await repository.create_many(
            [_CreateItem(sku="b"), _CreateItem(sku="a", qty=3), _CreateItem(sku="c")]
        )

        assert [item.sku for item in created] == ["b", "a", "c"]
        assert [item.qty for item in created] == [0, 3, 0]
        assert not any("RETURNING" in statement for statement in statements)
        assert sum(statement.startswith("SELECT") for statement in statements) == 1

    @pytest.mark.parametrize(
        "notes",
        [(None, "second"), ("first", None)],
        ids=["optional-only-in-later-row", "optional-only-in-first-row"],
    )
    async def test_optional_field_supplied_by_some_rows_is_persisted(
        self, repository: RepositorySQLAlchemy[_Item, int], notes: tuple[str | None, str | None]
    ) -> None:
        created = await repository.create_many(
            [_CreateItem(sku="a", note=notes[0]), _CreateItem(sku="b", note=notes[1])]
        )

        assert [item.note for item in created] == list(notes)
        assert [item.qty for item in created] == [0, 0]
        stored = [await repository.get_by_id(item.id) for item in created]
        assert [item.note for item in stored if item is not None] == list(notes)

    async def test_records_one_mutation_event_with_every_id(
        self, repository: RepositorySQLAlchemy[_Item, int], mutations: list[MutationEvent]
    ) -> None:
        created = await repository.create_many([_CreateItem(sku="a"), _CreateItem(sku="b")])

        assert len(mutations) == 1
        assert mutations[0].op == "create"
        assert mutations[0].entity == repository.entity_name
        assert mutations[0].ids == tuple(item.id for item in created)
        assert mutations[0].changed_fields == frozenset({"sku"})

    async def test_callable_default_is_evaluated_for_the_row_that_omits_it(
        self, repository: RepositorySQLAlchemy[_Item, int], statements: list[str]
    ) -> None:
        created = await repository.create_many(
            [_CreateItem(sku="a", token="given"), _CreateItem(sku="b")]
        )

        assert created[0].token == "given"
        assert created[1].token.startswith("tok-")
        stored = await repository.get_by_id(created[1].id)
        assert stored is not None
        assert stored.token == created[1].token
        assert sum(statement.startswith("INSERT") for statement in statements) == 1

    async def test_server_default_applies_to_the_row_that_omits_it(
        self, repository: RepositorySQLAlchemy[_Item, int], statements: list[str]
    ) -> None:
        given = "2020-01-02 03:04:05"
        created = await repository.create_many(
            [_CreateItem(sku="a", created_at=given), _CreateItem(sku="b")]
        )

        selects = sum(statement.startswith("SELECT") for statement in statements)

        assert [item.sku for item in created] == ["a", "b"]
        assert created[0].created_at == given
        assert created[1].created_at is not None
        assert selects == 1
        stored = await repository.get_by_id(created[1].id)
        assert stored is not None
        assert stored.created_at == created[1].created_at
