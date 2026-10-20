"""A column's nullability follows its field's annotation unless ``nullable=`` is declared."""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal

import msgspec
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.dialects.sqlite.base import SQLiteDialect
from sqlalchemy.engine import Dialect
from sqlalchemy.schema import CreateTable

from loom.core.backend.sqlalchemy import compile_all
from loom.core.model import (
    BaseModel,
    ColumnField,
    Field,
    Integer,
    LoomStruct,
    String,
    get_column_fields,
)


class Reading(BaseModel):
    __tablename__ = "nullable_readings"

    required: int
    id: int | None = ColumnField(primary_key=True, autoincrement=True, default=None)
    optional: int | None = None
    annotated_optional: Annotated[int | None, Field()] = None
    declared_not_null: int | None = ColumnField(nullable=False, default=None)
    declared_null: int = ColumnField(Integer, nullable=True)


class _Tone(Enum):
    LOW = "low"
    HIGH = "high"


class _Detail(LoomStruct):
    note: str


class Shapes(BaseModel):
    __tablename__ = "nullable_shapes"

    id: int = ColumnField(primary_key=True)
    annotated_then_optional: Annotated[int, Field()] | None = None
    unset_only: int | msgspec.UnsetType = msgspec.UNSET
    enum_optional: _Tone | None = ColumnField(String(8), default=None)
    struct_optional: _Detail | None = None
    literal_none: Literal[None] = ColumnField(String(8), default=None)
    literal_with_none: Literal["a", None] = ColumnField(String(8), default=None)
    literal_without_none: Literal["a", "b"] = ColumnField(String(8), default="a")


@pytest.mark.parametrize(
    ("name", "nullable"),
    [
        ("annotated_then_optional", True),
        ("unset_only", False),
        ("enum_optional", True),
        ("struct_optional", True),
        ("literal_none", True),
        ("literal_with_none", True),
        ("literal_without_none", False),
    ],
)
def test_the_annotation_shape_decides_the_nullability(name: str, nullable: bool) -> None:
    assert get_column_fields(Shapes)[name].field.nullable is nullable


def _ddl(dialect: Dialect) -> str:
    metadata = MetaData()
    compile_all(Reading, metadata=metadata)
    return str(CreateTable(metadata.tables["nullable_readings"]).compile(dialect=dialect))


@pytest.mark.parametrize("dialect", [PGDialect(), SQLiteDialect()])
def test_an_optional_field_gives_a_null_column(dialect: Dialect) -> None:
    ddl = _ddl(dialect)

    assert "\toptional INTEGER," in ddl
    assert "\tannotated_optional INTEGER," in ddl


@pytest.mark.parametrize("dialect", [PGDialect(), SQLiteDialect()])
def test_a_field_that_does_not_admit_none_gives_a_not_null_column(dialect: Dialect) -> None:
    assert "\trequired INTEGER NOT NULL" in _ddl(dialect)


@pytest.mark.parametrize("dialect", [PGDialect(), SQLiteDialect()])
def test_a_declared_nullable_wins_over_the_annotation(dialect: Dialect) -> None:
    ddl = _ddl(dialect)

    assert "\tdeclared_not_null INTEGER NOT NULL" in ddl
    assert "\tdeclared_null INTEGER," in ddl


def test_an_optional_primary_key_stays_not_null() -> None:
    metadata = MetaData()
    compile_all(Reading, metadata=metadata)

    assert metadata.tables["nullable_readings"].c.id.nullable is False
    assert "\tid SERIAL NOT NULL" in _ddl(PGDialect())


def test_the_resolved_field_carries_the_nullability() -> None:
    fields = get_column_fields(Reading)

    assert {name: info.field.nullable for name, info in fields.items()} == {
        "required": False,
        "id": False,
        "optional": True,
        "annotated_optional": True,
        "declared_not_null": False,
        "declared_null": True,
    }
