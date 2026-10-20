"""Typed values on the SQLAlchemy write and filter paths: JSON columns, related structs."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any

import pytest
from sqlalchemy import and_, bindparam, func, select

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.command import Command
from loom.core.model import JSON, BaseModel, ColumnField, LoomStruct, String
from loom.core.model.enums import Cardinality
from loom.core.model.relation import RelationField
from loom.core.repository.abc.query import FilterGroup, FilterOp, FilterSpec, QuerySpec
from loom.core.repository.sqlalchemy.query_compiler.filters import compile_filter_group
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


class _Doc(BaseModel):
    __tablename__ = "typed_docs"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    stamped_at: datetime = ColumnField()
    meta: dict[str, Any] = ColumnField(JSON)


class _CreateDoc(Command, frozen=True):
    stamped_at: datetime
    meta: dict[str, Any]


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
    shade: _Shade = ColumnField()
    noted_at: datetime = ColumnField(JSON)


class _CreateMark(Command, frozen=True):
    shade: _Shade
    noted_at: datetime


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
_META = {"when": datetime(2026, 1, 3, 12, 0), "price": Decimal("9.5"), "tags": ["a"]}
_META_STORED = {"when": "2026-01-03T12:00:00", "price": "9.5", "tags": ["a"]}


@pytest.fixture
async def session_manager() -> AsyncIterator[SessionManager]:
    compile_all(_Doc, _Customer, _Ticket, _Tag, _Swatch, _Mark)
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
            session_manager=session_manager, model=_Doc
        )

        created = await repo.create(_CreateDoc(stamped_at=_STAMP, meta=_META))
        stored = await repo.get_by_id(created.id)

        assert created.stamped_at == _STAMP
        assert created.meta == _META_STORED
        assert stored is not None
        assert (stored.stamped_at, stored.meta) == (_STAMP, _META_STORED)


class TestRelatedStructs:
    def test_output_renames_fields_of_a_loaded_related_struct(
        self, session_manager: SessionManager
    ) -> None:
        repo: RepositorySQLAlchemy[_Ticket, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Ticket
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
            session_manager=session_manager, model=_Ticket
        )
        customers: RepositorySQLAlchemy[_Customer, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Customer
        )
        tags: RepositorySQLAlchemy[_Tag, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Tag
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
        [(FilterOp.EQ, _Shade.DARK), (FilterOp.IN, [_Shade.DARK])],
    )
    async def test_a_plain_enum_filter_value_matches_what_the_write_stored(
        self, session_manager: SessionManager, op: FilterOp, value: Any
    ) -> None:
        repo: RepositorySQLAlchemy[_Swatch, int] = RepositorySQLAlchemy(
            session_manager=session_manager, model=_Swatch
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
            session_manager=session_manager, model=_Mark
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
