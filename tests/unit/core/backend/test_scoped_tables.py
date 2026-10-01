from __future__ import annotations

from sqlalchemy import Index, MetaData, UniqueConstraint

from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.model import BaseModel, ColumnField, Privilege, RowScoped, ScopeColumn, ScopedField
from loom.core.model.types import Integer, String, Text


class Ledger(BaseModel, RowScoped):
    __tablename__ = "ledger"
    __unique__ = (("holder", "reference"),)
    __indexes__ = (("holder", "booked_on"),)
    holder: int = ScopedField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    reference: str = ColumnField(Text)
    booked_on: str = ColumnField(Text)


class Readings(BaseModel, RowScoped):
    __tablename__ = "readings"
    __scope_privileges__ = frozenset({Privilege.SELECT})
    region: int = ScopedField(Integer, primary_key=True, scope="region")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    editor: str = ScopedField(String(36), scope="editor", on="write", elevable=True)


class Kinds(BaseModel):
    __tablename__ = "kinds"
    id: int = ColumnField(Integer, primary_key=True)


def test_declared_unique_becomes_a_unique_constraint_on_the_table() -> None:
    metadata = MetaData()

    compile_all(Ledger, metadata=metadata)

    uniques = [
        tuple(col.name for col in constraint.columns)
        for constraint in metadata.tables["ledger"].constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    assert uniques == [("holder", "reference")]


def test_declared_indexes_become_indexes_on_the_table() -> None:
    metadata = MetaData()

    compile_all(Ledger, metadata=metadata)

    indexes = [
        tuple(col.name for col in index.columns)
        for index in metadata.tables["ledger"].indexes
        if isinstance(index, Index) and not index.unique
    ]
    assert indexes == [("holder", "booked_on")]


def test_scoped_tables_lists_every_marked_table_with_its_scopes_and_privileges() -> None:
    metadata = MetaData()

    compile_all(Ledger, Readings, Kinds, metadata=metadata)
    registry = scoped_tables(metadata)

    assert set(registry) == {(None, "ledger"), (None, "readings")}
    assert registry[(None, "ledger")].scopes == (
        ScopeColumn(scope="holder", column="holder", on="both", elevable=False),
    )
    assert registry[(None, "ledger")].privileges == frozenset(Privilege)
    assert registry[(None, "readings")].privileges == frozenset({Privilege.SELECT})
    assert registry[(None, "readings")].boundary.column == "region"


def test_scoped_tables_is_empty_for_a_metadata_without_marked_models() -> None:
    metadata = MetaData()

    compile_all(Kinds, metadata=metadata)

    assert scoped_tables(metadata) == {}
