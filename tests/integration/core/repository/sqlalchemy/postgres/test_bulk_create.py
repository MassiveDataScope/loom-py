"""``create_many`` on Postgres: ``IGNORE`` skips existing keys only; chunks share a transaction."""

from __future__ import annotations

import math
from collections.abc import AsyncIterator, Iterator
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event

from loom.core.command import Command
from loom.core.errors import Conflict
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.schema import compile_all, get_metadata, reset_registry
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.repository import CursorCodec, OnConflict
from loom.core.stores.repository.mutation import MutationEvent

pytestmark = pytest.mark.integration


class AcmeBulkSku(BaseModel):
    __tablename__ = "loom_it_bulk_skus"

    id: int = ColumnField(primary_key=True)
    code: str = ColumnField(length=32, unique=True)


class CreateSku(Command, frozen=True):
    id: int
    code: str | None = None


class AcmeBulkTag(BaseModel):
    __tablename__ = "loom_it_bulk_tags"

    id: UUID = ColumnField(primary_key=True, default=uuid4)
    label: str = ColumnField(length=32)


class CreateTag(Command, frozen=True):
    label: str
    id: UUID | None = None


@pytest.fixture
async def manager(pg_admin_uri: str) -> AsyncIterator[SessionManager]:
    reset_registry()
    compile_all(AcmeBulkSku, AcmeBulkTag)
    session_manager = SessionManager(
        pg_admin_uri, pool_size=1, max_overflow=0, pool_pre_ping=False, store=DEFAULT_STORE
    )
    async with session_manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().drop_all)
        await conn.run_sync(get_metadata().create_all)
    try:
        yield session_manager
    finally:
        async with session_manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await session_manager.dispose()
        reset_registry()


@pytest.fixture
def skus(manager: SessionManager) -> RepositorySQLAlchemy[AcmeBulkSku, int]:
    return RepositorySQLAlchemy(
        session_manager=manager, model=AcmeBulkSku, cursor_codec=CursorCodec(signing="ephemeral")
    )


@pytest.fixture
def mutations(monkeypatch: pytest.MonkeyPatch) -> list[MutationEvent]:
    events: list[MutationEvent] = []
    monkeypatch.setattr("loom.core.repository.sqlalchemy.mixins.record_mutation", events.append)
    return events


@pytest.fixture
def statements(manager: SessionManager) -> Iterator[list[str]]:
    captured: list[str] = []

    def _statement(
        conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: bool
    ) -> None:
        if statement.startswith("INSERT"):
            captured.append(statement)

    def _begin(conn: Any) -> None:
        captured.append("BEGIN")

    def _commit(conn: Any) -> None:
        captured.append("COMMIT")

    sync_engine = manager.engine.sync_engine
    listeners = (("before_cursor_execute", _statement), ("begin", _begin), ("commit", _commit))
    for name, listener in listeners:
        event.listen(sync_engine, name, listener)
    yield captured
    for name, listener in listeners:
        event.remove(sync_engine, name, listener)


class TestPostgresCreateManyOrder:
    @pytest.mark.parametrize("on_conflict", list(OnConflict))
    async def test_generated_and_explicit_keys_keep_input_order(
        self, manager: SessionManager, on_conflict: OnConflict
    ) -> None:
        tags: RepositorySQLAlchemy[AcmeBulkTag, UUID] = RepositorySQLAlchemy(
            session_manager=manager,
            model=AcmeBulkTag,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        explicit = UUID(int=1)

        created = await tags.create_many(
            [CreateTag(label="a"), CreateTag(label="b", id=explicit), CreateTag(label="c")],
            on_conflict=on_conflict,
        )

        assert [item.label for item in created] == ["a", "b", "c"]
        assert created[1].id == explicit


class TestPostgresCreateManyIgnore:
    async def test_overlapping_keys_insert_only_new_rows_in_input_order(
        self, skus: RepositorySQLAlchemy[AcmeBulkSku, int], mutations: list[MutationEvent]
    ) -> None:
        await skus.create_many([CreateSku(id=1, code="a"), CreateSku(id=2, code="b")])
        mutations.clear()

        created = await skus.create_many(
            [
                CreateSku(id=4, code="d"),
                CreateSku(id=1, code="a"),
                CreateSku(id=3, code="c"),
                CreateSku(id=2, code="b"),
                CreateSku(id=3, code="z"),
            ],
            on_conflict=OnConflict.IGNORE,
        )

        assert [(item.id, item.code) for item in created] == [(4, "d"), (3, "c")]
        assert await skus.count() == 4
        assert [event.ids for event in mutations] == [(4, 3)]

    @pytest.mark.parametrize(
        "batch",
        [
            [CreateSku(id=5, code="e"), CreateSku(id=6)],
            [CreateSku(id=5, code="a")],
        ],
        ids=["not-null", "other-unique"],
    )
    async def test_other_constraint_violation_raises_conflict(
        self, skus: RepositorySQLAlchemy[AcmeBulkSku, int], batch: list[CreateSku]
    ) -> None:
        await skus.create_many([CreateSku(id=1, code="a")])

        with pytest.raises(Conflict):
            await skus.create_many(batch, on_conflict=OnConflict.IGNORE)

        assert await skus.count() == 1


class TestPostgresCreateManyChunks:
    @pytest.mark.parametrize("on_conflict", list(OnConflict))
    async def test_batch_above_the_chunk_emits_one_insert_per_chunk_in_one_transaction(
        self,
        skus: RepositorySQLAlchemy[AcmeBulkSku, int],
        manager: SessionManager,
        statements: list[str],
        monkeypatch: pytest.MonkeyPatch,
        on_conflict: OnConflict,
    ) -> None:
        monkeypatch.setattr(manager.engine.dialect, "insertmanyvalues_max_parameters", 4)
        batch = [CreateSku(id=key, code=f"c-{key}") for key in (5, 3, 9, 1, 7)]

        created = await skus.create_many(batch, on_conflict=on_conflict)

        inserts = [statement for statement in statements if statement.startswith("INSERT")]
        assert [item.id for item in created] == [5, 3, 9, 1, 7]
        assert len(inserts) == math.ceil(len(batch) / 2)
        assert [s for s in statements if not s.startswith("INSERT")] == ["BEGIN", "COMMIT"]
