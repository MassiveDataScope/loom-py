from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import CheckConstraint, Index, MetaData, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import AddConstraint, CreateIndex

from loom.core.backend.sqlalchemy import compile_all
from loom.core.model import BaseModel, ColumnField, OnDelete, RowScoped, ScopedField
from loom.core.model.types import Boolean, Integer, String, Text

CONVENTION = {
    "pk": "pk_%(table_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
}


class Roster(BaseModel, RowScoped):
    __tablename__ = "rosters"
    __unique__ = (("tenant_id", "code"),)
    tenant_id: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    code: str = ColumnField(Text)


class Seat(BaseModel, RowScoped):
    __tablename__ = "seats"
    __checks__ = {"status_code": "status_code IN ('active', 'removed')"}
    __partial_unique__ = {"owner": (("tenant_id", "roster_id"), "is_owner")}
    __indexes__ = (("tenant_id", "status_code"),)
    tenant_id: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    roster_id: int = ColumnField(Integer, foreign_key="rosters.id", on_delete=OnDelete.CASCADE)
    status_code: str = ColumnField(Text)
    is_owner: bool = ColumnField(Boolean)


def _compiled(convention: dict[str, str] | None = None) -> MetaData:
    metadata = MetaData(naming_convention=convention)
    compile_all(Roster, Seat, metadata=metadata)
    return metadata


def _checks(metadata: MetaData, table: str) -> list[CheckConstraint]:
    return [c for c in metadata.tables[table].constraints if isinstance(c, CheckConstraint)]


def _index(metadata: MetaData, table: str, name: str) -> Index:
    return next(index for index in metadata.tables[table].indexes if index.name == name)


def _ddl(element: Any) -> str:
    return str(element.compile(dialect=postgresql.dialect()))


def test_a_declared_check_compiles_to_a_named_check_constraint() -> None:
    metadata = _compiled()

    (check,) = _checks(metadata, "seats")

    assert check.name == "status_code"
    assert str(check.sqltext) == "status_code IN ('active', 'removed')"
    assert _ddl(AddConstraint(check)).endswith(
        "CONSTRAINT status_code CHECK (status_code IN ('active', 'removed'))"
    )


def test_a_partial_unique_compiles_to_a_named_unique_index_with_its_predicate() -> None:
    metadata = _compiled()

    index = _index(metadata, "seats", "uq_seats_owner")

    assert index.unique
    assert [column.name for column in index.columns] == ["tenant_id", "roster_id"]
    assert _ddl(CreateIndex(index)) == (
        "CREATE UNIQUE INDEX uq_seats_owner ON seats (tenant_id, roster_id) WHERE is_owner"
    )


def test_a_scoped_partial_unique_without_the_boundary_fails_at_compile_time() -> None:
    class LooseSeat(BaseModel, RowScoped):
        __tablename__ = "loose_seats"
        __partial_unique__ = {"owner": (("roster_id",), "is_owner")}
        tenant_id: str = ScopedField(String(36), primary_key=True, scope="tenant")
        id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
        roster_id: int = ColumnField(Integer)
        is_owner: bool = ColumnField(Boolean)

    with pytest.raises(ValueError, match=r"C5: LooseSeat partial unique owner"):
        compile_all(LooseSeat, metadata=MetaData())


def test_without_a_naming_convention_names_are_unchanged() -> None:
    metadata = _compiled()
    seats = metadata.tables["seats"]
    rosters = metadata.tables["rosters"]

    (unique,) = [c for c in rosters.constraints if isinstance(c, UniqueConstraint)]
    (foreign_key,) = seats.foreign_key_constraints
    assert unique.name is None
    assert foreign_key.name is None
    assert seats.primary_key.name is None
    assert {index.name for index in seats.indexes} == {
        "uq_seats_owner",
        "ix_seats_tenant_id_status_code",
    }


def test_a_naming_convention_names_keys_foreign_keys_and_checks() -> None:
    metadata = _compiled(CONVENTION)
    seats = metadata.tables["seats"]
    rosters = metadata.tables["rosters"]

    (unique,) = [c for c in rosters.constraints if isinstance(c, UniqueConstraint)]
    (foreign_key,) = seats.foreign_key_constraints
    (check,) = _checks(metadata, "seats")
    assert unique.name == "uq_rosters_tenant_id_code"
    assert foreign_key.name == "fk_seats_tenant_id_roster_id"
    assert check.name == "ck_seats_status_code"
    assert seats.primary_key.name == "pk_seats"
    assert rosters.primary_key.name == "pk_rosters"


def test_a_naming_convention_does_not_rename_explicitly_named_indexes() -> None:
    by_constraint_name = {**CONVENTION, "ix": "ix_%(table_name)s_%(constraint_name)s"}

    for convention in (CONVENTION, by_constraint_name):
        seats = _compiled(convention).tables["seats"]
        assert {index.name for index in seats.indexes} == {
            "uq_seats_owner",
            "ix_seats_tenant_id_status_code",
        }


def test_a_check_name_longer_than_postgres_allows_fails_at_compile_time() -> None:
    rule = "r" * 60

    class Verbose(BaseModel):
        __tablename__ = "verbose"
        __checks__ = {rule: "id > 0"}
        id: int = ColumnField(Integer, primary_key=True)

    compile_all(Verbose, metadata=MetaData())
    with pytest.raises(ValueError, match=rf"Verbose: __checks__ rule '{rule}'.*63 bytes"):
        compile_all(Verbose, metadata=MetaData(naming_convention=CONVENTION))


def test_a_partial_unique_name_longer_than_postgres_allows_fails_at_compile_time() -> None:
    rule = "r" * 55

    class Wordy(BaseModel):
        __tablename__ = "wordy"
        __partial_unique__ = {rule: (("id",), "flag")}
        id: int = ColumnField(Integer, primary_key=True)
        flag: bool = ColumnField(Boolean)

    with pytest.raises(ValueError, match=rf"Wordy: __partial_unique__ rule '{rule}'.*63 bytes"):
        compile_all(Wordy, metadata=MetaData())


def test_an_index_name_longer_than_postgres_allows_fails_at_compile_time() -> None:
    class Indexed(BaseModel):
        __tablename__ = "t" * 50
        __indexes__ = (("long_column",),)
        id: int = ColumnField(Integer, primary_key=True)
        long_column: int = ColumnField(Integer)

    with pytest.raises(ValueError, match=r"Indexed: __indexes__ rule 'long_column'.*63 bytes"):
        compile_all(Indexed, metadata=MetaData())


class Shift(BaseModel, RowScoped):
    __tablename__ = "shifts"
    tenant_id: str = ScopedField(String(36), primary_key=True, scope="tenant")
    id: int = ColumnField(Integer, primary_key=True, autoincrement=True)
    roster_id: int = ColumnField(Integer, foreign_key="rosters.id", on_delete=OnDelete.CASCADE)
    cover_id: int = ColumnField(Integer, foreign_key="rosters.id", on_delete=OnDelete.CASCADE)


def test_two_constraints_of_one_table_resolving_to_one_name_fail_at_compile_time() -> None:
    by_first_column = {**CONVENTION, "fk": "fk_%(table_name)s_%(column_0_name)s"}

    with pytest.raises(
        ValueError, match=r"Shift: table shifts has two constraints named 'fk_shifts_tenant_id'"
    ):
        compile_all(Roster, Shift, metadata=MetaData(naming_convention=by_first_column))


def test_the_recommended_fk_convention_names_every_composite_fk_apart() -> None:
    metadata = MetaData(naming_convention=CONVENTION)

    compile_all(Roster, Shift, metadata=metadata)

    assert {fk.name for fk in metadata.tables["shifts"].foreign_key_constraints} == {
        "fk_shifts_tenant_id_roster_id",
        "fk_shifts_tenant_id_cover_id",
    }


def test_a_constraint_and_an_index_sharing_a_name_fail_at_compile_time() -> None:
    class Clash(BaseModel):
        __tablename__ = "clash"
        __checks__ = {"uq_clash_rule": "id > 0"}
        __partial_unique__ = {"rule": (("id",), "id > 0")}
        id: int = ColumnField(Integer, primary_key=True)

    with pytest.raises(ValueError, match=r"Clash: table clash has two constraints named"):
        compile_all(Clash, metadata=MetaData())
