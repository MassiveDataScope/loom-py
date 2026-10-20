"""Typed values on the SQLAlchemy write and filter paths: JSON columns, related structs."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from enum import Enum, IntEnum, StrEnum
from typing import Any
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import and_, bindparam, func, select, text

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import (
    JSON,
    BaseModel,
    BoundaryValidationError,
    ColumnField,
    ColumnType,
    LoomStruct,
    String,
)
from loom.core.model.enums import Cardinality
from loom.core.model.relation import RelationField
from loom.core.repository.abc import CursorCodec
from loom.core.repository.abc.query import (
    FilterGroup,
    FilterOp,
    FilterParams,
    FilterSpec,
    PageParams,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.repository.sqlalchemy.query_compiler.filters import compile_filter_group
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

_CODEC = CursorCodec(signing="ephemeral")


class _Doc(BaseModel):
    __tablename__ = "typed_docs"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    stamped_at: datetime = ColumnField()
    meta: dict[str, Any] = ColumnField(JSON)


class _CreateDoc(Command, frozen=True):
    stamped_at: datetime
    meta: dict[str, Any]


class _State(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


class _Level(IntEnum):
    LOW = 1
    HIGH = 2


class _Case(BaseModel):
    __tablename__ = "typed_cases"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    state: _State = ColumnField(length=16)
    level: _Level = ColumnField()


class _CreateCase(Command, frozen=True):
    state: _State
    level: _Level


class _Shade(Enum):
    LIGHT = "light"
    DARK = "dark"


class _Swatch(BaseModel):
    __tablename__ = "typed_swatches"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    shade: _Shade = ColumnField(String(16))


class _CreateSwatch(Command, frozen=True):
    shade: _Shade


class _Mark(BaseModel):
    __tablename__ = "typed_marks"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    shade: _Shade = ColumnField(JSON)
    noted_at: datetime = ColumnField(JSON)


class _CreateMark(Command, frozen=True):
    shade: _Shade
    noted_at: datetime


class _Slot(BaseModel):
    __tablename__ = "typed_slots"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    opens_at: time = ColumnField()
    noted_at: time = ColumnField(JSON)
    closes_at: time = ColumnField(ColumnType("Time", kwargs={"timezone": True}))


class _CreateSlot(Command, frozen=True):
    opens_at: time
    noted_at: time
    closes_at: time


class _Badge(BaseModel):
    __tablename__ = "typed_badges"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    ref: UUID = ColumnField()


class _CreateBadge(Command, frozen=True):
    ref: UUID


class _Pass(BaseModel):
    __tablename__ = "typed_passes"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    ref: UUID


class _CreatePass(Command, frozen=True):
    ref: UUID


class _CustomerOut(LoomStruct, rename="camel"):
    id: int
    full_name: str


class _Customer(BaseModel):
    __tablename__ = "typed_customers"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    full_name: str = ColumnField(length=64)


class _Ticket(BaseModel):
    __tablename__ = "typed_tickets"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    customer_id: int = ColumnField(foreign_key="typed_customers.id")
    customer: _CustomerOut | None = RelationField(
        foreign_key="typed_customers.id", cardinality=Cardinality.MANY_TO_ONE
    )
    tags: list[dict[str, Any]] = RelationField(
        foreign_key="ticket_id",
        cardinality=Cardinality.ONE_TO_MANY,
        profiles=("with_tags",),
        depends_on=("typed_tags:ticket_id",),
    )


class _Tag(BaseModel):
    __tablename__ = "typed_tags"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    ticket_id: int = ColumnField(foreign_key="typed_tickets.id")
    label_text: str = ColumnField(length=32)


class _CreateCustomer(Command, frozen=True):
    full_name: str


class _CreateTicket(Command, frozen=True):
    customer_id: int


class _CreateTag(Command, frozen=True):
    ticket_id: int
    label_text: str


_STAMP = datetime(2026, 1, 3, 12, 0)
_AWARE_TIME = time(8, 30, tzinfo=timezone(timedelta(hours=2)))
_NAIVE_TIME = time(8, 30)
# A zone on a time gives it no offset: utcoffset() is None.
_ZONED_TIME = time(8, 30, tzinfo=ZoneInfo("Europe/Madrid"))
_META = {"when": datetime(2026, 1, 3, 12, 0), "price": Decimal("9.5"), "tags": ["a"]}
_META_STORED = {"when": "2026-01-03T12:00:00", "price": "9.5", "tags": ["a"]}


@pytest.fixture
async def session_manager() -> AsyncIterator[SessionManager]:
    compile_all(_Doc, _Customer, _Ticket, _Tag, _Case, _Swatch, _Mark, _Slot, _Badge, _Pass)
    manager = SessionManager(
        "sqlite+aiosqlite:///:memory:",
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )
    compiled = get_compiled(_Doc)
    assert compiled is not None
    async with manager.engine.begin() as connection:
        await connection.run_sync(compiled.metadata.create_all)
    yield manager
    await manager.dispose()


class TestJsonColumns:
    async def test_datetime_and_decimal_inside_a_json_column_round_trip(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Doc, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Doc,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )

        created = await repo.create(_CreateDoc(stamped_at=_STAMP, meta=_META))
        stored = await repo.get_by_id(created.id)

        assert created.stamped_at == _STAMP
        assert created.meta == _META_STORED
        assert stored is not None
        assert (stored.stamped_at, stored.meta) == (_STAMP, _META_STORED)


class TestEnumColumns:
    async def test_enum_values_are_stored_read_back_and_filtered_by_equality(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Case, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Case,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        created = await repo.create(_CreateCase(state=_State.CLOSED, level=_Level.HIGH))
        await repo.create(_CreateCase(state=_State.OPEN, level=_Level.LOW))

        stored = await repo.get_by_id(created.id)
        query = QuerySpec(
            filters=FilterGroup(
                filters=(
                    FilterSpec("state", FilterOp.EQ, _State.CLOSED),
                    FilterSpec("level", FilterOp.EQ, _Level.HIGH),
                )
            )
        )
        found = await repo.list_with_query(query)
        async with session_manager.engine.connect() as connection:
            raw = await connection.execute(text("SELECT state, level FROM typed_cases"))

        assert stored is not None
        assert (stored.state, stored.level) == (_State.CLOSED, _Level.HIGH)
        assert [item.id for item in found.items] == [created.id]
        assert sorted(raw.all()) == [("closed", 2), ("open", 1)]


_REF = UUID("6f1c1e0e-7a1b-4c1e-9d1e-1b2c3d4e5f60")
_OTHER_REF = UUID("0b9d2f1c-1111-4c1e-9d1e-1b2c3d4e5f60")


class TestUuidColumns:
    async def test_a_uuid_is_stored_as_hex_read_back_and_filtered_by_equality(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Badge, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Badge
        )
        created = await repo.create(_CreateBadge(ref=_REF))
        await repo.create(_CreateBadge(ref=_OTHER_REF))

        stored = await repo.get_by_id(created.id)
        query = QuerySpec(filters=FilterGroup(filters=(FilterSpec("ref", FilterOp.EQ, _REF),)))
        found = await repo.list_with_query(query)
        async with session_manager.engine.connect() as connection:
            raw = await connection.execute(text("SELECT ref FROM typed_badges ORDER BY id"))

        assert stored is not None
        assert stored.ref == _REF
        assert [item.id for item in found.items] == [created.id]
        assert raw.scalars().all() == [_REF.hex, _OTHER_REF.hex]

    async def test_a_bare_uuid_annotation_round_trips_as_hex(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Pass, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Pass
        )

        created = await repo.create(_CreatePass(ref=_REF))
        stored = await repo.get_by_id(created.id)
        async with session_manager.engine.connect() as connection:
            raw = await connection.execute(text("SELECT ref FROM typed_passes"))

        assert stored is not None
        assert stored.ref == _REF
        assert raw.scalars().all() == [_REF.hex]

    async def test_a_cursor_walk_sorted_by_a_uuid_pages_in_uuid_order(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Badge, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Badge
        )
        refs = [UUID(int=value << 64) for value in (7, 2, 9, 4, 1)]
        await repo.create_many([_CreateBadge(ref=ref) for ref in refs])

        walked: list[UUID] = []
        cursor: str | None = None
        pages = 0
        while True:
            page = await repo.list_with_query(
                QuerySpec(
                    sort=(SortSpec(field="ref", direction="DESC"),),
                    pagination=PaginationMode.CURSOR,
                    limit=2,
                    cursor=cursor,
                )
            )
            walked += [item.ref for item in page.items]
            pages += 1
            cursor = page.next_cursor
            if cursor is None:
                break

        assert walked == sorted(refs, reverse=True)
        assert pages == 3


_OFFSET_REFUSAL = (("opens_at", "time must not carry a UTC offset"),)
_OFFSET_REQUIRED = (("closes_at", "time must carry a UTC offset"),)


async def _lookup(
    repo: RepositorySQLAlchemy[Any, int], lookup: str, field: str, value: object
) -> object:
    """Run one equality lookup that bypasses ``QuerySpec``: get_by, exists_by or filter_by."""
    if lookup == "get_by":
        return await repo.get_by(field, value)
    if lookup == "exists_by":
        return await repo.exists_by(field, value)
    page = await repo.list_paginated(
        PageParams(page=1, limit=10), FilterParams(filters={field: value})
    )
    return page.items


_LOOKUPS = ("get_by", "exists_by", "list_paginated")


class TestTimeColumns:
    async def test_writing_an_aware_time_to_a_time_column_is_refused(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Slot, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Slot,
            cursor_codec=_CODEC,
        )

        with pytest.raises(BoundaryValidationError) as caught:
            await repo.create(
                _CreateSlot(opens_at=_AWARE_TIME, noted_at=_NAIVE_TIME, closes_at=_AWARE_TIME)
            )

        assert (caught.value.code, caught.value.violations) == (
            "boundary_validation",
            _OFFSET_REFUSAL,
        )

    @pytest.mark.parametrize(
        ("op", "value"), [(FilterOp.EQ, _AWARE_TIME), (FilterOp.IN, [_NAIVE_TIME, _AWARE_TIME])]
    )
    def test_filtering_a_time_column_by_an_aware_time_is_refused(
        self, session_manager: SessionManager, op: FilterOp, value: Any
    ) -> None:
        group = FilterGroup(filters=(FilterSpec(field="opens_at", op=op, value=value),))

        with pytest.raises(BoundaryValidationError) as caught:
            compile_filter_group(_compiled(_Slot), group, frozenset())

        assert (caught.value.code, caught.value.violations) == ("query_validation", _OFFSET_REFUSAL)

    @pytest.mark.parametrize("lookup", _LOOKUPS)
    async def test_looking_up_a_time_column_by_an_aware_time_is_refused(
        self, session_manager: SessionManager, lookup: str
    ) -> None:
        repo: RepositorySQLAlchemy[_Slot, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Slot,
            cursor_codec=_CODEC,
        )

        with pytest.raises(BoundaryValidationError) as caught:
            await _lookup(repo, lookup, "opens_at", _AWARE_TIME)

        assert (caught.value.code, caught.value.violations) == ("query_validation", _OFFSET_REFUSAL)

    async def test_an_aware_time_in_a_json_column_keeps_its_offset(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Slot, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Slot,
            cursor_codec=_CODEC,
        )

        created = await repo.create(
            _CreateSlot(opens_at=_NAIVE_TIME, noted_at=_AWARE_TIME, closes_at=_AWARE_TIME)
        )
        stored = await repo.get_by_id(created.id)

        assert stored is not None
        assert (stored.opens_at, stored.noted_at) == (_NAIVE_TIME, _AWARE_TIME)


class TestTimeOffsetEscapeHatches:
    def test_a_time_with_time_zone_column_accepts_an_aware_time_on_write(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Slot, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Slot,
            cursor_codec=_CODEC,
        )

        written = repo._serialize_input(
            _CreateSlot(opens_at=_NAIVE_TIME, noted_at=_NAIVE_TIME, closes_at=_AWARE_TIME)
        )

        assert written["closes_at"] == _AWARE_TIME

    @pytest.mark.parametrize(
        ("field", "bound"),
        [("closes_at", _AWARE_TIME), ("noted_at", _AWARE_TIME.isoformat())],
    )
    def test_a_column_that_keeps_the_offset_accepts_an_aware_filter_value(
        self, session_manager: SessionManager, field: str, bound: object
    ) -> None:
        group = FilterGroup(filters=(FilterSpec(field=field, op=FilterOp.EQ, value=_AWARE_TIME),))

        clause = compile_filter_group(_compiled(_Slot), group, frozenset())

        assert list(clause.compile().params.values()) == [bound]

    @pytest.mark.parametrize("value", [_NAIVE_TIME, _ZONED_TIME])
    def test_a_time_with_time_zone_column_refuses_a_time_without_offset_on_write(
        self, session_manager: SessionManager, value: time
    ) -> None:
        repo: RepositorySQLAlchemy[_Slot, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Slot,
            cursor_codec=_CODEC,
        )

        with pytest.raises(BoundaryValidationError) as caught:
            repo._serialize_input(
                _CreateSlot(opens_at=_NAIVE_TIME, noted_at=_NAIVE_TIME, closes_at=value)
            )

        assert (caught.value.code, caught.value.violations) == (
            "boundary_validation",
            _OFFSET_REQUIRED,
        )

    def test_a_plain_time_column_accepts_a_time_with_a_zone_but_no_offset(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Slot, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Slot,
            cursor_codec=_CODEC,
        )
        group = FilterGroup(
            filters=(FilterSpec(field="opens_at", op=FilterOp.EQ, value=_ZONED_TIME),)
        )

        written = repo._serialize_input(
            _CreateSlot(opens_at=_ZONED_TIME, noted_at=_NAIVE_TIME, closes_at=_AWARE_TIME)
        )
        clause = compile_filter_group(_compiled(_Slot), group, frozenset())

        assert written["opens_at"] == _ZONED_TIME
        assert list(clause.compile().params.values()) == [_ZONED_TIME]

    def test_a_write_reports_every_refused_time_column_in_name_order(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Slot, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Slot,
            cursor_codec=_CODEC,
        )

        with pytest.raises(BoundaryValidationError) as caught:
            repo._serialize_input(
                _CreateSlot(opens_at=_AWARE_TIME, noted_at=_NAIVE_TIME, closes_at=_NAIVE_TIME)
            )

        assert caught.value.violations == _OFFSET_REQUIRED + _OFFSET_REFUSAL

    @pytest.mark.parametrize(
        ("op", "value"),
        [
            (FilterOp.EQ, _NAIVE_TIME),
            (FilterOp.EQ, _ZONED_TIME),
            (FilterOp.IN, [_AWARE_TIME, _NAIVE_TIME]),
        ],
    )
    def test_a_time_with_time_zone_column_refuses_a_filter_value_without_offset(
        self, session_manager: SessionManager, op: FilterOp, value: Any
    ) -> None:
        group = FilterGroup(filters=(FilterSpec(field="closes_at", op=op, value=value),))

        with pytest.raises(BoundaryValidationError) as caught:
            compile_filter_group(_compiled(_Slot), group, frozenset())

        assert (caught.value.code, caught.value.violations) == (
            "query_validation",
            _OFFSET_REQUIRED,
        )

    @pytest.mark.parametrize("lookup", _LOOKUPS)
    async def test_a_time_with_time_zone_column_refuses_a_naive_lookup_value(
        self, session_manager: SessionManager, lookup: str
    ) -> None:
        repo: RepositorySQLAlchemy[_Slot, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Slot,
            cursor_codec=_CODEC,
        )

        with pytest.raises(BoundaryValidationError) as caught:
            await _lookup(repo, lookup, "closes_at", _NAIVE_TIME)

        assert (caught.value.code, caught.value.violations) == (
            "query_validation",
            _OFFSET_REQUIRED,
        )


class TestLookupValues:
    @pytest.mark.parametrize("lookup", _LOOKUPS)
    async def test_a_lookup_by_a_plain_enum_matches_what_the_write_stored(
        self, session_manager: SessionManager, lookup: str
    ) -> None:
        repo: RepositorySQLAlchemy[_Swatch, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Swatch,
            cursor_codec=_CODEC,
        )
        await repo.create(_CreateSwatch(shade=_Shade.DARK))

        found = await _lookup(repo, lookup, "shade", _Shade.DARK)

        assert found

    @pytest.mark.parametrize("lookup", _LOOKUPS)
    @pytest.mark.parametrize(
        ("model", "field"),
        [(_Swatch, "nope"), (_Swatch, "metadata"), (_Ticket, "customer")],
    )
    async def test_a_lookup_on_a_name_that_is_not_a_column_is_refused(
        self, session_manager: SessionManager, lookup: str, model: type[BaseModel], field: str
    ) -> None:
        repo: RepositorySQLAlchemy[Any, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=model,
            cursor_codec=_CODEC,
        )

        with pytest.raises(ValueError, match=f"Unknown field '{field}'"):
            await _lookup(repo, lookup, field, 1)


class TestRelatedStructs:
    def test_output_renames_fields_of_a_loaded_related_struct(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Ticket, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Ticket,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        ticket_sa = get_compiled(_Ticket)
        customer_sa = get_compiled(_Customer)
        assert ticket_sa is not None
        assert customer_sa is not None
        ticket = ticket_sa(id=1, customer_id=7)
        ticket.customer = customer_sa(id=7, full_name="Ann")

        output = repo._to_output(ticket)

        assert output.customer == _CustomerOut(id=7, full_name="Ann")

    async def test_dict_annotated_relation_has_the_same_keys_on_read_and_write(
        self, session_manager: SessionManager
    ) -> None:
        tickets: RepositorySQLAlchemy[_Ticket, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Ticket,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        customers: RepositorySQLAlchemy[_Customer, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Customer,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        tags: RepositorySQLAlchemy[_Tag, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Tag,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        customer = await customers.create(_CreateCustomer(full_name="Ann"))
        ticket = await tickets.create(_CreateTicket(customer_id=customer.id))
        tag = await tags.create(_CreateTag(ticket_id=ticket.id, label_text="urgent"))
        ticket_sa = get_compiled(_Ticket)
        tag_sa = get_compiled(_Tag)
        assert ticket_sa is not None
        assert tag_sa is not None
        in_memory = ticket_sa(id=ticket.id, customer_id=customer.id)
        in_memory.tags = [tag_sa(id=tag.id, ticket_id=ticket.id, label_text="urgent")]

        read = await tickets.get_by_id(ticket.id, profile="with_tags")
        written = tickets._to_output(in_memory, profile="with_tags")

        assert read is not None
        assert read.tags == written.tags
        assert read.tags == [{"id": tag.id, "ticketId": ticket.id, "labelText": "urgent"}]


class TestFilterValues:
    @pytest.mark.parametrize(
        ("op", "value"),
        [
            (FilterOp.EQ, _Shade.DARK),
            (FilterOp.IN, [_Shade.DARK]),
            (FilterOp.IN, (_Shade.DARK,)),
            (FilterOp.IN, {_Shade.DARK}),
        ],
    )
    async def test_a_plain_enum_filter_value_matches_what_the_write_stored(
        self, session_manager: SessionManager, op: FilterOp, value: Any
    ) -> None:
        repo: RepositorySQLAlchemy[_Swatch, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Swatch,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        await repo.create(_CreateSwatch(shade=_Shade.LIGHT))
        dark = await repo.create(_CreateSwatch(shade=_Shade.DARK))

        page = await repo.list_with_query(
            QuerySpec(filters=FilterGroup(filters=(FilterSpec(field="shade", op=op, value=value),)))
        )

        assert [item.id for item in page.items] == [dark.id]

    @pytest.mark.parametrize(("field", "value"), [("shade", _Shade.DARK), ("noted_at", _STAMP)])
    def test_a_json_column_filter_binds_the_value_the_write_hands_sqlalchemy(
        self, session_manager: SessionManager, field: str, value: Any
    ) -> None:
        repo: RepositorySQLAlchemy[_Mark, int] = RepositorySQLAlchemy(
            session_manager=session_manager,
            model=_Mark,
            cursor_codec=CursorCodec(signing="ephemeral"),
        )
        written = repo._serialize_input(_CreateMark(shade=_Shade.DARK, noted_at=_STAMP))[field]

        clause = compile_filter_group(
            _compiled(_Mark),
            FilterGroup(filters=(FilterSpec(field=field, op=FilterOp.EQ, value=value),)),
            frozenset(),
        )

        assert list(clause.compile().params.values()) == [written]

    @pytest.mark.parametrize(
        ("op", "value"),
        [
            (FilterOp.LTE, func.now()),
            (FilterOp.IN, bindparam("stamps", expanding=True)),
            (FilterOp.IN, select(func.now())),
        ],
    )
    def test_a_sqlalchemy_expression_filter_value_is_bound_as_it_is(
        self, session_manager: SessionManager, op: FilterOp, value: Any
    ) -> None:
        column = _compiled(_Doc).stamped_at
        expected = column <= value if op is FilterOp.LTE else column.in_(value)

        clause = compile_filter_group(
            _compiled(_Doc),
            FilterGroup(filters=(FilterSpec(field="stamped_at", op=op, value=value),)),
            frozenset(),
        )

        assert str(clause) == str(and_(expected))


def _compiled(model: type[BaseModel]) -> Any:
    compiled = get_compiled(model)
    assert compiled is not None
    return compiled
