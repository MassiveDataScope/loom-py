"""Unit tests for InMemoryRepository."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import msgspec
import pytest
from msgspec import UnsetType

from loom.core.model import (
    BaseModel,
    Cardinality,
    ColumnField,
    CreatedAtField,
    ProjectionField,
    RelationField,
    UpdatedAtField,
)
from loom.core.projection.loaders import CountLoader
from loom.testing.in_memory import InMemoryRepository

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class Widget(BaseModel):
    __tablename__ = "in_memory_widgets"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=64)


class CreateWidgetCmd(msgspec.Struct):
    name: str


class UpdateWidgetCmd(msgspec.Struct):
    name: str


class _Review(msgspec.Struct):
    id: int
    product_id: int
    rating: int


class _Product(BaseModel):
    __tablename__ = "in_memory_products"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)
    reviews: list[_Review] = RelationField(
        foreign_key="product_id",
        cardinality=Cardinality.ONE_TO_MANY,
        profiles=("with_details",),
    )
    review_count: int = ProjectionField(
        loader=CountLoader(model=_Review),
        profiles=("with_details",),
        default=0,
    )


class _Attempt(BaseModel):
    __tablename__ = "in_memory_attempts"

    run_id: str = ColumnField(primary_key=True, length=32)
    key: str = ColumnField(primary_key=True, length=32)
    attempt: int = ColumnField(primary_key=True)
    outcome: str = ColumnField(length=16)


class _Ticket(BaseModel):
    __tablename__ = "in_memory_tickets"

    number: int = ColumnField(primary_key=True, autoincrement=True)
    title: str = ColumnField(length=64)


class _CreateTicketCmd(msgspec.Struct):
    title: str


class TestDeclaredPrimaryKey:
    @pytest.mark.asyncio
    async def test_a_single_key_not_named_id_keys_the_store(self) -> None:
        repo: InMemoryRepository[_Ticket] = InMemoryRepository(_Ticket)
        repo.seed(_Ticket(number=7, title="seeded"))

        created = await repo.create(_CreateTicketCmd(title="created"))

        assert created == _Ticket(number=8, title="created")
        assert await repo.get_by_id(7) == _Ticket(number=7, title="seeded")

    def test_a_model_without_a_declared_primary_key_is_refused(self) -> None:
        with pytest.raises(ValueError, match="No primary key field found on _Review"):
            InMemoryRepository(_Review)


class TestCompositePrimaryKey:
    @pytest.mark.asyncio
    async def test_a_seeded_row_is_read_by_its_key_tuple(self) -> None:
        repo: InMemoryRepository[_Attempt] = InMemoryRepository(_Attempt)
        row = _Attempt(run_id="r", key="k", attempt=2, outcome="failed")
        repo.seed(row)

        assert await repo.get_by_id(("r", "k", 2)) == row
        assert await repo.get_by_id(("r", "k", 1)) is None

    @pytest.mark.asyncio
    async def test_a_command_without_an_id_field_is_created(self) -> None:
        repo: InMemoryRepository[_Attempt] = InMemoryRepository(_Attempt)

        created = await repo.create(_Attempt(run_id="r", key="k", attempt=1, outcome="ok"))

        assert await repo.get_by_id(("r", "k", 1)) == created


# ---------------------------------------------------------------------------
# get_by_id
# ---------------------------------------------------------------------------


class TestGetById:
    @pytest.mark.asyncio
    async def test_returns_seeded_entity(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"))
        result = await repo.get_by_id(1)
        assert result == Widget(id=1, name="A")

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.get_by_id(99)
        assert result is None

    @pytest.mark.asyncio
    async def test_profile_param_ignored(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"))
        result = await repo.get_by_id(1, profile="detail")
        assert result == Widget(id=1, name="A")


# ---------------------------------------------------------------------------
# seed
# ---------------------------------------------------------------------------


class TestSeed:
    def test_multiple_entities(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"), Widget(id=2, name="B"))
        assert len(repo._store) == 2

    def test_advances_next_id_past_max(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=5, name="X"))
        assert repo._next_id == 6

    def test_does_not_regress_next_id(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=3, name="A"))
        repo.seed(Widget(id=1, name="B"))
        assert repo._next_id == 4


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


class TestCreate:
    @pytest.mark.asyncio
    async def test_auto_derives_fields_from_cmd(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.create(CreateWidgetCmd(name="NewWidget"))
        assert result.name == "NewWidget"
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_auto_increments_id(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        w1 = await repo.create(CreateWidgetCmd(name="A"))
        w2 = await repo.create(CreateWidgetCmd(name="B"))
        assert w1.id == 1
        assert w2.id == 2

    @pytest.mark.asyncio
    async def test_stores_created_entity(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        created = await repo.create(CreateWidgetCmd(name="X"))
        stored = await repo.get_by_id(created.id)
        assert stored == created

    @pytest.mark.asyncio
    async def test_id_continues_after_seed(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=10, name="Seed"))
        result = await repo.create(CreateWidgetCmd(name="New"))
        assert result.id == 11

    @pytest.mark.asyncio
    async def test_uses_creator_callable_when_provided(self) -> None:
        def _creator(cmd: Any, next_id: int) -> Widget:
            return Widget(id=next_id * 100, name=cmd.name.upper())

        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget, creator=_creator)
        result = await repo.create(CreateWidgetCmd(name="hello"))
        assert result.id == 100
        assert result.name == "HELLO"


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------


class TestUpdate:
    @pytest.mark.asyncio
    async def test_updates_matching_fields(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="Old"))
        result = await repo.update(1, UpdateWidgetCmd(name="New"))
        assert result is not None
        assert result.name == "New"
        assert result.id == 1

    @pytest.mark.asyncio
    async def test_persists_update(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="Old"))
        await repo.update(1, UpdateWidgetCmd(name="New"))
        stored = await repo.get_by_id(1)
        assert stored is not None
        assert stored.name == "New"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.update(99, UpdateWidgetCmd(name="X"))
        assert result is None

    @pytest.mark.asyncio
    async def test_accepts_dict_as_data(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="Old"))
        result = await repo.update(1, {"name": "DictUpdate"})
        assert result is not None
        assert result.name == "DictUpdate"


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestDelete:
    @pytest.mark.asyncio
    async def test_removes_existing_entity(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"))
        deleted = await repo.delete(1)
        assert deleted is True
        assert await repo.get_by_id(1) is None

    @pytest.mark.asyncio
    async def test_returns_false_when_not_found(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.delete(99)
        assert result is False


# ---------------------------------------------------------------------------
# list_paginated
# ---------------------------------------------------------------------------


class TestListPaginated:
    @pytest.mark.asyncio
    async def test_returns_all_seeded(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"), Widget(id=2, name="B"))
        result = await repo.list_paginated()
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_entities(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        result = await repo.list_paginated()
        assert result == []

    @pytest.mark.asyncio
    async def test_includes_created_entities(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        await repo.create(CreateWidgetCmd(name="X"))
        result = await repo.list_paginated()
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_ignores_extra_kwargs(self) -> None:
        repo: InMemoryRepository[Widget] = InMemoryRepository(Widget)
        repo.seed(Widget(id=1, name="A"))
        result = await repo.list_paginated(page=1, limit=10, profile="default")
        assert len(result) == 1


class TestProjectionProfiles:
    @pytest.mark.asyncio
    async def test_projection_not_included_outside_profile_even_with_default(self) -> None:
        repo: InMemoryRepository[_Product] = InMemoryRepository(_Product)
        repo.seed(_Product(id=1, name="p"))

        loaded = await repo.get_by_id(1, profile="default")
        assert loaded is not None
        payload = msgspec.to_builtins(loaded)
        assert isinstance(payload, dict)
        assert "reviewCount" not in payload

    @pytest.mark.asyncio
    async def test_projection_included_when_profile_is_active(self) -> None:
        repo: InMemoryRepository[_Product] = InMemoryRepository(_Product)
        repo.seed(_Product(id=1, name="p", reviews=[]))

        loaded = await repo.get_by_id(1, profile="with_details")
        assert loaded is not None
        assert loaded.review_count == 0
        payload = msgspec.to_builtins(loaded)
        assert isinstance(payload, dict)
        assert payload["reviewCount"] == 0


class _Stamped(BaseModel):
    __tablename__ = "in_memory_stamped"

    key: str = ColumnField(primary_key=True, length=16)
    note: str = ColumnField(length=16)
    created_at: datetime | UnsetType = CreatedAtField()
    updated_at: datetime | UnsetType = UpdatedAtField()


class _NoteCmd(msgspec.Struct):
    note: str | UnsetType = msgspec.UNSET


_LONG_AGO = datetime(2020, 1, 1, tzinfo=UTC)

_Create = Callable[[InMemoryRepository[_Stamped], _Stamped], Awaitable[_Stamped]]


async def _create_one(repo: InMemoryRepository[_Stamped], row: _Stamped) -> _Stamped:
    return await repo.create(row)


async def _create_in_batch(repo: InMemoryRepository[_Stamped], row: _Stamped) -> _Stamped:
    (created,) = await repo.create_many([row])
    return created


class TestServerStamps:
    @pytest.fixture
    def repo(self) -> InMemoryRepository[_Stamped]:
        return InMemoryRepository(_Stamped)

    @pytest.fixture(params=[_create_one, _create_in_batch], ids=["create", "create_many"])
    def create(self, request: pytest.FixtureRequest) -> _Create:
        return request.param

    async def test_a_create_stamps_the_unset_server_defaults_with_one_current_time(
        self, repo: InMemoryRepository[_Stamped], create: _Create
    ) -> None:
        before = datetime.now(UTC)

        created = await create(repo, _Stamped(key="a", note="new"))

        assert isinstance(created.created_at, datetime)
        assert before <= created.created_at <= datetime.now(UTC)
        assert created.updated_at == created.created_at
        assert await repo.get_by_id("a") == created

    async def test_a_create_keeps_the_stamps_it_is_given(
        self, repo: InMemoryRepository[_Stamped], create: _Create
    ) -> None:
        row = _Stamped(key="a", note="new", created_at=_LONG_AGO, updated_at=_LONG_AGO)

        created = await create(repo, row)

        assert created == row

    async def test_an_update_restamps_the_update_field_and_keeps_the_creation_one(
        self, repo: InMemoryRepository[_Stamped]
    ) -> None:
        repo.seed(_Stamped(key="a", note="old", created_at=_LONG_AGO, updated_at=_LONG_AGO))
        before = datetime.now(UTC)

        updated = await repo.update("a", _NoteCmd(note="new"))

        assert updated is not None
        assert updated.note == "new"
        assert updated.created_at == _LONG_AGO
        assert isinstance(updated.updated_at, datetime)
        assert before <= updated.updated_at <= datetime.now(UTC)
        assert await repo.get_by_id("a") == updated

    async def test_an_update_leaves_a_field_the_command_sends_unset(
        self, repo: InMemoryRepository[_Stamped]
    ) -> None:
        repo.seed(_Stamped(key="a", note="old", created_at=_LONG_AGO, updated_at=_LONG_AGO))

        updated = await repo.update("a", _NoteCmd())

        assert updated is not None
        assert updated.note == "old"
        assert updated.updated_at != _LONG_AGO
