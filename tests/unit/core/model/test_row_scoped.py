from __future__ import annotations

from typing import Annotated

import pytest

from loom.core.model import BaseModel, ColumnField, Field, ScopedField
from loom.core.model.introspection import is_row_scoped, scope_columns
from loom.core.model.privilege import READ_WRITE, Privilege
from loom.core.model.scoped import RowScoped, ScopeColumn
from loom.core.model.types import Integer, String, Text


class Plain(BaseModel):
    __tablename__ = "plain"
    id: int = ColumnField(Integer, primary_key=True)


class Scoped(BaseModel, RowScoped):
    __tablename__ = "scoped"
    key: str = ScopedField(String(36), primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    editor: str = ScopedField(Text, scope="editor", on="write", elevable=True)
    body: str = ColumnField(Text)


def test_the_privilege_set_is_closed_to_the_four_row_operations() -> None:
    assert {p.value for p in Privilege} == {"SELECT", "INSERT", "UPDATE", "DELETE"}
    assert frozenset(Privilege) == READ_WRITE


def test_the_marker_adds_no_column_and_grants_read_write_by_default() -> None:
    assert [c.column for c in scope_columns(Scoped)] == ["key", "editor"]
    assert Scoped.__scope_privileges__ == READ_WRITE
    assert is_row_scoped(Scoped) is True
    assert is_row_scoped(Plain) is False


def test_scope_columns_describe_each_scope_and_its_reach() -> None:
    holder, editor = scope_columns(Scoped)

    assert holder == ScopeColumn(scope="holder", column="key", on="both", elevable=False)
    assert holder.is_boundary is True
    assert editor == ScopeColumn(scope="editor", column="editor", on="write", elevable=True)
    assert editor.is_boundary is False


def test_an_unmarked_model_has_no_scope_columns() -> None:
    assert scope_columns(Plain) == ()


def test_c1_a_marked_table_without_a_boundary_scope_is_rejected() -> None:
    class NoBoundary(BaseModel, RowScoped):
        __tablename__ = "no_boundary"
        id: int = ColumnField(Integer, primary_key=True)
        editor: str = ScopedField(Text, scope="editor", on="write", elevable=True)

    with pytest.raises(ValueError, match=r"C1.*NoBoundary"):
        scope_columns(NoBoundary)


def test_c1_two_boundary_scopes_are_rejected() -> None:
    class TwoBoundaries(BaseModel, RowScoped):
        __tablename__ = "two_boundaries"
        a: int = ScopedField(Integer, primary_key=True, scope="a")
        b: int = ScopedField(Integer, primary_key=True, scope="b")

    with pytest.raises(ValueError, match=r"C1.*TwoBoundaries"):
        scope_columns(TwoBoundaries)


def test_c2_elevable_is_only_allowed_on_write_scopes() -> None:
    class ReadElevable(BaseModel, RowScoped):
        __tablename__ = "read_elevable"
        key: int = ScopedField(Integer, primary_key=True, scope="holder")
        viewer: str = ScopedField(Text, scope="viewer", on="read", elevable=True)

    with pytest.raises(ValueError, match=r"C2.*ReadElevable.*viewer"):
        scope_columns(ReadElevable)


@pytest.mark.parametrize("name", ["Holder", "my-scope", "1st", ""])
def test_c3_scope_names_must_be_identifiers(name: str) -> None:
    class BadName(BaseModel, RowScoped):
        __tablename__ = "bad_name"
        key: int = ScopedField(Integer, primary_key=True, scope=name)

    with pytest.raises(ValueError, match=r"C3.*BadName.*key"):
        scope_columns(BadName)


def test_c3_a_scope_name_cannot_repeat_within_a_table() -> None:
    class Repeated(BaseModel, RowScoped):
        __tablename__ = "repeated"
        key: int = ScopedField(Integer, primary_key=True, scope="holder")
        other: int = ScopedField(Integer, scope="holder", on="write")

    with pytest.raises(ValueError, match=r"C3.*Repeated.*other"):
        scope_columns(Repeated)


def test_c4_scope_options_on_an_unmarked_model_are_rejected() -> None:
    class Unmarked(BaseModel):
        __tablename__ = "unmarked"
        key: int = ScopedField(Integer, primary_key=True, scope="holder")

    with pytest.raises(ValueError, match=r"C4.*Unmarked.*key"):
        scope_columns(Unmarked)


WRITE_ONLY = Field(on="write")
ELEVABLE = Field(elevable=True)


def test_c4_a_reach_without_a_scope_is_rejected() -> None:
    class WriteOnly(BaseModel, RowScoped):
        __tablename__ = "write_only"
        key: int = ScopedField(Integer, primary_key=True, scope="holder")
        loose: Annotated[str, Text, WRITE_ONLY]

    with pytest.raises(ValueError, match=r"C4.*WriteOnly.*loose"):
        scope_columns(WriteOnly)


def test_c4_elevable_without_a_scope_is_rejected() -> None:
    class Elevable(BaseModel, RowScoped):
        __tablename__ = "elevable"
        key: int = ScopedField(Integer, primary_key=True, scope="holder")
        loose: Annotated[str, Text, ELEVABLE]

    with pytest.raises(ValueError, match=r"C4.*Elevable.*loose"):
        scope_columns(Elevable)


def test_c5_the_primary_key_must_contain_the_boundary_column() -> None:
    class PkWithoutBoundary(BaseModel, RowScoped):
        __tablename__ = "pk_without_boundary"
        id: int = ColumnField(Integer, primary_key=True)
        key: int = ScopedField(Integer, scope="holder")

    with pytest.raises(ValueError, match=r"C5.*PkWithoutBoundary.*id"):
        scope_columns(PkWithoutBoundary)


def test_c5_a_single_column_unique_without_the_boundary_is_rejected() -> None:
    class UniqueWithoutBoundary(BaseModel, RowScoped):
        __tablename__ = "unique_without_boundary"
        key: int = ScopedField(Integer, primary_key=True, scope="holder")
        reference: str = ColumnField(Text, unique=True)

    with pytest.raises(ValueError, match=r"C5.*UniqueWithoutBoundary.*reference"):
        scope_columns(UniqueWithoutBoundary)


def test_c8_the_boundary_column_cannot_be_nullable() -> None:
    class NullableBoundary(BaseModel, RowScoped):
        __tablename__ = "nullable_boundary"
        key: int | None = ScopedField(Integer, primary_key=True, nullable=True, scope="holder")

    with pytest.raises(ValueError, match=r"C8.*NullableBoundary.*key"):
        scope_columns(NullableBoundary)


def test_other_scope_columns_may_be_nullable() -> None:
    class NullableEditor(BaseModel, RowScoped):
        __tablename__ = "nullable_editor"
        key: int = ScopedField(Integer, primary_key=True, scope="holder")
        editor: str | None = ScopedField(Text, nullable=True, scope="editor", on="write")

    assert [c.column for c in scope_columns(NullableEditor)] == ["key", "editor"]
