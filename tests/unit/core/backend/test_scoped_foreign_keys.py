from __future__ import annotations

import pytest
from sqlalchemy import MetaData

from loom.core.backend.sqlalchemy import compile_all
from loom.core.model import BaseModel, ColumnField, OnDelete, Privilege, RowScoped
from loom.core.model.types import Integer, String, Text


class Holder(BaseModel, RowScoped):
    __tablename__ = "holders"
    key: str = ColumnField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    body: str = ColumnField(Text)


class Item(BaseModel, RowScoped):
    __tablename__ = "items"
    key: str = ColumnField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    holder_id: int = ColumnField(Integer, foreign_key="holders.id", on_delete=OnDelete.CASCADE)


class Catalog(BaseModel):
    __tablename__ = "catalog"
    __privileges__ = {"readers": frozenset({Privilege.SELECT})}
    id: int = ColumnField(Integer, primary_key=True)


class Entry(BaseModel, RowScoped):
    __tablename__ = "entries"
    catalog_id: int = ColumnField(
        Integer,
        primary_key=True,
        scope="catalog",
        foreign_key="catalog.id",
        on_delete=OnDelete.RESTRICT,
    )
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)


def _fk_pairs(metadata: MetaData, table: str) -> list[tuple[list[str], list[str]]]:
    return [
        (
            [col.name for col in fk.columns],
            [element.target_fullname for element in fk.elements],
        )
        for fk in metadata.tables[table].foreign_key_constraints
    ]


def test_c6_a_fk_between_scoped_tables_is_compiled_with_the_boundary_first() -> None:
    metadata = MetaData()

    compile_all(Holder, Item, metadata=metadata)

    assert _fk_pairs(metadata, "items") == [(["key", "holder_id"], ["holders.key", "holders.id"])]


def test_c6_the_compiled_fk_keeps_the_declared_cascade() -> None:
    metadata = MetaData()

    compile_all(Holder, Item, metadata=metadata)

    (constraint,) = metadata.tables["items"].foreign_key_constraints
    assert constraint.ondelete == "CASCADE"


@pytest.mark.parametrize("action_name", ["SET NULL", "SET DEFAULT"])
def test_c6_set_null_and_set_default_between_scoped_tables_are_rejected(
    action_name: str,
) -> None:
    action = OnDelete(action_name)

    class Dangling(BaseModel, RowScoped):
        __tablename__ = "dangling"
        key: str = ColumnField(String(36), primary_key=True, scope="holder")
        id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
        holder_id: int | None = ColumnField(
            Integer, nullable=True, foreign_key="holders.id", on_delete=action
        )

    with pytest.raises(ValueError, match=rf"C6.*Dangling.*{action_name}"):
        compile_all(Holder, Dangling, metadata=MetaData())


def test_c6_the_referenced_table_must_share_the_boundary_scope() -> None:
    class Other(BaseModel, RowScoped):
        __tablename__ = "others"
        realm: str = ColumnField(String(36), primary_key=True, scope="realm")
        id: int = ColumnField(Integer, primary_key=True, autoincrement=True)

    class Crossing(BaseModel, RowScoped):
        __tablename__ = "crossing"
        key: str = ColumnField(String(36), primary_key=True, scope="holder")
        id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
        other_id: int = ColumnField(Integer, foreign_key="others.id")

    with pytest.raises(ValueError, match=r"C6.*Crossing.*holder"):
        compile_all(Other, Crossing, metadata=MetaData())


def test_c7_a_fk_from_an_unscoped_table_to_a_scoped_table_is_rejected() -> None:
    class Global(BaseModel):
        __tablename__ = "globals"
        id: int = ColumnField(Integer, primary_key=True)
        holder_id: int = ColumnField(Integer, foreign_key="holders.id")

    with pytest.raises(ValueError, match=r"C7.*Global.*holders"):
        compile_all(Holder, Global, metadata=MetaData())


def test_c9_a_boundary_fk_to_a_read_only_global_table_is_allowed() -> None:
    metadata = MetaData()

    compile_all(Catalog, Entry, metadata=metadata)

    assert _fk_pairs(metadata, "entries") == [(["catalog_id"], ["catalog.id"])]


def test_c9_a_cascading_fk_to_a_global_table_is_rejected() -> None:
    class Cascading(BaseModel, RowScoped):
        __tablename__ = "cascading"
        catalog_id: int = ColumnField(
            Integer,
            primary_key=True,
            scope="catalog",
            foreign_key="catalog.id",
            on_delete=OnDelete.CASCADE,
        )
        id: int = ColumnField(Integer, primary_key=True, autoincrement=True)

    with pytest.raises(ValueError, match=r"C9.*Cascading.*CASCADE"):
        compile_all(Catalog, Cascading, metadata=MetaData())


def test_c9_the_global_table_cannot_be_writable_by_a_group_except_for_insert() -> None:
    class Editable(BaseModel):
        __tablename__ = "editable"
        __privileges__ = {"writers": frozenset({Privilege.INSERT, Privilege.UPDATE})}
        id: int = ColumnField(Integer, primary_key=True)

    class Pointing(BaseModel, RowScoped):
        __tablename__ = "pointing"
        editable_id: int = ColumnField(
            Integer, primary_key=True, scope="editable", foreign_key="editable.id"
        )
        id: int = ColumnField(Integer, primary_key=True, autoincrement=True)

    with pytest.raises(ValueError, match=r"C9.*Pointing.*writers"):
        compile_all(Editable, Pointing, metadata=MetaData())


def test_c9_insert_on_the_global_table_is_allowed() -> None:
    class Appendable(BaseModel):
        __tablename__ = "appendable"
        __privileges__ = {"writers": frozenset({Privilege.INSERT})}
        id: int = ColumnField(Integer, primary_key=True)

    class Pointing(BaseModel, RowScoped):
        __tablename__ = "pointing_ok"
        appendable_id: int = ColumnField(
            Integer, primary_key=True, scope="appendable", foreign_key="appendable.id"
        )
        id: int = ColumnField(Integer, primary_key=True, autoincrement=True)

    metadata = MetaData()
    compile_all(Appendable, Pointing, metadata=metadata)

    assert "pointing_ok" in metadata.tables


def test_a_scoped_model_whose_fk_target_is_not_compiled_is_rejected() -> None:
    class Orphan(BaseModel, RowScoped):
        __tablename__ = "orphans"
        key: str = ColumnField(String(36), primary_key=True, scope="holder")
        id: int = ColumnField(Integer, primary_key=True)
        holder_id: int = ColumnField(Integer, foreign_key="holders.id")

    with pytest.raises(ValueError, match=r"orphans.*holders"):
        compile_all(Orphan, metadata=MetaData())


def test_c6_the_referenced_key_must_match_exactly() -> None:
    class Wide(BaseModel, RowScoped):
        __tablename__ = "wides"
        key: str = ColumnField(String(36), primary_key=True, scope="holder")
        id: int = ColumnField(Integer, primary_key=True)
        version: int = ColumnField(Integer, primary_key=True)

    class Narrow(BaseModel, RowScoped):
        __tablename__ = "narrows"
        key: str = ColumnField(String(36), primary_key=True, scope="holder")
        id: int = ColumnField(Integer, primary_key=True)
        wide_id: int = ColumnField(Integer, foreign_key="wides.id")

    with pytest.raises(ValueError, match=r"C6.*\(key, id\)"):
        compile_all(Wide, Narrow, metadata=MetaData())
