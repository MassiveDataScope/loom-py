from __future__ import annotations

import pytest

from loom.core.model import BaseModel, ColumnField, ScopedField
from loom.core.model.introspection import (
    PartialUnique,
    declared_checks,
    declared_indexes,
    declared_partial_unique,
    declared_privileges,
    declared_unique,
    scope_columns,
)
from loom.core.model.privilege import Privilege
from loom.core.model.scoped import RowScoped
from loom.core.model.types import Boolean, Integer, Text


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


class Seat(BaseModel, RowScoped):
    __tablename__ = "seats"
    __checks__ = {"status_code": "status_code IN ('active', 'removed')"}
    __partial_unique__ = {"owner": (("holder",), "is_owner")}
    holder: int = ScopedField(Integer, primary_key=True, scope="holder")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    status_code: str = ColumnField(Text)
    is_owner: bool = ColumnField(Boolean)


def test_named_checks_are_read_from_the_model() -> None:
    assert declared_checks(Seat) == {"status_code": "status_code IN ('active', 'removed')"}
    assert declared_checks(Bare) == {}


@pytest.mark.parametrize("rule", ["Status", "1st", "check", "pg_rule", "a-b", ""])
def test_a_check_rule_that_is_not_an_identifier_is_rejected(rule: str) -> None:
    class Odd(BaseModel):
        __tablename__ = "odd"
        __checks__ = {rule: "id > 0"}
        id: int = ColumnField(Integer, primary_key=True)

    with pytest.raises(ValueError, match=r"Odd: __checks__ rule"):
        declared_checks(Odd)


@pytest.mark.parametrize("expression", ["", "   ", 42])
def test_a_check_without_an_sql_expression_is_rejected(expression: object) -> None:
    class Blank(BaseModel):
        __tablename__ = "blank"
        __checks__ = {"positive": expression}
        id: int = ColumnField(Integer, primary_key=True)

    with pytest.raises(ValueError, match=r"Blank: __checks__ rule 'positive'"):
        declared_checks(Blank)


def test_partial_unique_indexes_are_read_from_the_model() -> None:
    assert declared_partial_unique(Seat) == (
        PartialUnique(rule="owner", columns=("holder",), where="is_owner"),
    )
    assert declared_partial_unique(Bare) == ()


def test_a_partial_unique_naming_an_unknown_column_is_rejected() -> None:
    class Typo(BaseModel):
        __tablename__ = "typo"
        __partial_unique__ = {"owner": (("id", "is_ownr"), "is_owner")}
        id: int = ColumnField(Integer, primary_key=True)
        is_owner: bool = ColumnField(Boolean)

    with pytest.raises(ValueError, match=r"Typo: __partial_unique__ rule 'owner'.*'is_ownr'"):
        declared_partial_unique(Typo)


@pytest.mark.parametrize(
    "entry",
    [((), "is_owner"), (("id",), ""), (("id",), None), ("id", "is_owner"), (("id",),)],
)
def test_a_malformed_partial_unique_is_rejected(entry: object) -> None:
    class Malformed(BaseModel):
        __tablename__ = "malformed"
        __partial_unique__ = {"owner": entry}
        id: int = ColumnField(Integer, primary_key=True)
        is_owner: bool = ColumnField(Boolean)

    with pytest.raises(ValueError, match=r"Malformed: __partial_unique__ rule 'owner'"):
        declared_partial_unique(Malformed)


def test_a_partial_unique_rule_that_is_not_an_identifier_is_rejected() -> None:
    class Shouting(BaseModel):
        __tablename__ = "shouting"
        __partial_unique__ = {"Owner": (("id",), "is_owner")}
        id: int = ColumnField(Integer, primary_key=True)
        is_owner: bool = ColumnField(Boolean)

    with pytest.raises(ValueError, match=r"Shouting: __partial_unique__ rule 'Owner'"):
        declared_partial_unique(Shouting)


def test_a_partial_unique_without_the_boundary_fails_c5() -> None:
    class LooseOwner(BaseModel, RowScoped):
        __tablename__ = "loose_owner"
        __partial_unique__ = {"owner": (("roster",), "is_owner")}
        holder: int = ScopedField(Integer, primary_key=True, scope="holder")
        roster: int = ColumnField(Integer)
        is_owner: bool = ColumnField(Boolean)

    with pytest.raises(
        ValueError, match=r"C5: LooseOwner partial unique owner \(roster\) lacks the boundary"
    ):
        scope_columns(LooseOwner)


def test_a_partial_unique_with_the_boundary_passes_c5() -> None:
    assert [scope.column for scope in scope_columns(Seat)] == ["holder"]
