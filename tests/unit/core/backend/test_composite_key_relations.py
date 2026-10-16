"""Compiling a composite-key model refuses the loads that group rows by one key column."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from typing import Any

import pytest

from loom.core.backend.sqlalchemy import compile_all, get_compiled, reset_registry
from loom.core.model import (
    BaseModel,
    Cardinality,
    ColumnField,
    LoomStruct,
    ProjectionField,
    RelationField,
)


class _CustomerOut(LoomStruct):
    id: int
    name: str


class _Customer(BaseModel):
    __tablename__ = "ck_customers"
    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=16)


class _Ticket(BaseModel):
    __tablename__ = "ck_tickets"
    account_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)
    customer_id: int = ColumnField(foreign_key="ck_customers.id")
    customer: _CustomerOut | None = RelationField(
        foreign_key="ck_customers.id", cardinality=Cardinality.MANY_TO_ONE
    )


class _Child(BaseModel):
    __tablename__ = "ck_children"
    id: int = ColumnField(primary_key=True)
    parent_id: int = ColumnField(foreign_key="ck_parents.id")


class _Parent(BaseModel):
    __tablename__ = "ck_parents"
    account_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True, unique=True)
    children: list[dict[str, object]] = RelationField(
        foreign_key="parent_id", cardinality=Cardinality.ONE_TO_MANY, profiles=("kids",)
    )


class _ByIdLoader:
    async def load_many(self, backend_context: Any, parent_ids: Sequence[object]) -> dict[Any, Any]:
        return {}


class _FirstLetterLoader:
    def load_from_object(self, obj: Any, row_context: dict[str, Any]) -> Any:
        return obj.name[:1]


class _Scored(BaseModel):
    __tablename__ = "ck_scored"
    account_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)
    score: int = ProjectionField(loader=_ByIdLoader(), default=0)


class _Named(BaseModel):
    __tablename__ = "ck_named"
    account_id: int = ColumnField(primary_key=True)
    id: int = ColumnField(primary_key=True)
    name: str = ColumnField(length=16)
    initial: str = ProjectionField(loader=_FirstLetterLoader(), default="")


class _Author(BaseModel):
    __tablename__ = "ck_authors"
    id: int = ColumnField(primary_key=True)
    books: list[dict[str, object]] = RelationField(
        foreign_key="author_id", cardinality=Cardinality.ONE_TO_MANY
    )


class _Book(BaseModel):
    __tablename__ = "ck_books"
    id: int = ColumnField(primary_key=True)
    author_id: int = ColumnField(foreign_key="ck_authors.id")


@pytest.fixture(autouse=True)
def _clean_registry() -> Generator[None, None, None]:
    reset_registry()
    yield
    reset_registry()


@pytest.mark.parametrize(
    ("models", "message"),
    [
        (
            (_Customer, _Ticket),
            r"_Ticket has the composite primary key \(account_id, id\).*customer",
        ),
        ((_Child, _Parent), r"_Parent has the composite primary key \(account_id, id\).*children"),
        ((_Scored,), r"_Scored has the composite primary key \(account_id, id\).*score"),
    ],
    ids=["many-to-one", "one-to-many", "backend-projection"],
)
def test_a_composite_key_model_refuses_backend_relations_and_projections(
    models: tuple[type, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=rf"{message}.*by hand"):
        compile_all(*models)


def test_a_composite_key_model_keeps_its_in_memory_projections() -> None:
    compile_all(_Named)

    assert get_compiled(_Named) is not None


def test_a_single_key_model_with_relations_compiles() -> None:
    compile_all(_Author, _Book)

    assert get_compiled(_Author) is not None
