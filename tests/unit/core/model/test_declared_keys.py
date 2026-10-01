from __future__ import annotations

import pytest

from loom.core.model import BaseModel, ColumnField, ScopedField
from loom.core.model.introspection import (
    declared_indexes,
    declared_privileges,
    declared_unique,
    scope_columns,
)
from loom.core.model.privilege import Privilege
from loom.core.model.scoped import RowScoped
from loom.core.model.types import Integer, Text


class Entry(BaseModel, RowScoped):
    __tablename__ = "entries"
    __unique__ = (("holder", "reference"),)
    __indexes__ = (("holder", "booked_on"),)
    holder: int = ScopedField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    reference: str = ColumnField(Text)
    booked_on: str = ColumnField(Text)


class Catalog(BaseModel):
    __tablename__ = "catalog"
    __privileges__ = {"readers": frozenset({Privilege.SELECT})}
    id: int = ColumnField(Integer, primary_key=True)


class Bare(BaseModel):
    __tablename__ = "bare"
    id: int = ColumnField(Integer, primary_key=True)


def test_composite_unique_and_indexes_are_read_from_the_model() -> None:
    assert declared_unique(Entry) == (("holder", "reference"),)
    assert declared_indexes(Entry) == (("holder", "booked_on"),)
    assert declared_unique(Bare) == ()
    assert declared_indexes(Bare) == ()


def test_a_declared_unique_without_the_boundary_fails_c5() -> None:
    class Loose(BaseModel, RowScoped):
        __tablename__ = "loose"
        __unique__ = (("reference",),)
        holder: int = ScopedField(Integer, primary_key=True, scope="holder")
        reference: str = ColumnField(Text)

    with pytest.raises(ValueError, match=r"C5.*Loose.*reference"):
        scope_columns(Loose)


def test_a_declared_unique_naming_an_unknown_column_is_rejected() -> None:
    class Typo(BaseModel, RowScoped):
        __tablename__ = "typo"
        __unique__ = (("holder", "refrence"),)
        holder: int = ScopedField(Integer, primary_key=True, scope="holder")
        reference: str = ColumnField(Text)

    with pytest.raises(ValueError, match=r"Typo.*refrence"):
        declared_unique(Typo)


def test_global_table_privileges_are_keyed_by_group() -> None:
    assert declared_privileges(Catalog) == {"readers": frozenset({Privilege.SELECT})}
    assert declared_privileges(Bare) == {}


def test_privileges_on_a_scoped_model_are_rejected() -> None:
    class ScopedWithGrants(BaseModel, RowScoped):
        __tablename__ = "scoped_with_grants"
        __privileges__ = {"readers": frozenset({Privilege.SELECT})}
        holder: int = ScopedField(Integer, primary_key=True, scope="holder")

    with pytest.raises(ValueError, match=r"ScopedWithGrants.*__privileges__"):
        declared_privileges(ScopedWithGrants)


def test_an_unknown_group_key_is_rejected() -> None:
    class BadGroup(BaseModel):
        __tablename__ = "bad_group"
        __privileges__ = {"admins": frozenset({Privilege.SELECT})}
        id: int = ColumnField(Integer, primary_key=True)

    with pytest.raises(ValueError, match=r"BadGroup.*admins"):
        declared_privileges(BadGroup)


def test_a_privilege_outside_the_closed_set_is_rejected() -> None:
    class Truncating(BaseModel):
        __tablename__ = "truncating"
        __privileges__ = {"writers": frozenset({"TRUNCATE"})}
        id: int = ColumnField(Integer, primary_key=True)

    with pytest.raises(ValueError, match=r"Truncating.*TRUNCATE"):
        declared_privileges(Truncating)
