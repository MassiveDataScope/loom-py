"""``sa_table_for`` gives a struct's column fields the column types ``compile_model`` gives them."""

from __future__ import annotations

from datetime import date, time
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Annotated, Any, Literal

import msgspec
import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql

from loom.core.backend.sqlalchemy import compile_model, get_compiled, sa_table_for
from loom.core.model import BaseModel, ColumnField, LoomStruct, Numeric, String
from loom.core.model.enums import Cardinality
from loom.core.model.relation import RelationField


class _Shade(StrEnum):
    LIGHT = "light"
    DARK = "dark"


class _Mode(Enum):
    AUTO = "auto"


class _Owner(LoomStruct):
    id: int


class _Meter(BaseModel):
    __tablename__ = "sa_table_for_meters"

    id: int = ColumnField(primary_key=True)
    label: str = ColumnField(String(12), unique=True, default="m")
    amount: Annotated[Decimal, Numeric(precision=12, scale=2)]
    shade: _Shade
    taken_on: date
    opens_at: time
    blob: bytes
    owner_id: int
    owner: _Owner | None = RelationField(
        foreign_key="owners.id", cardinality=Cardinality.MANY_TO_ONE
    )


def _ddl_types(columns: Any) -> dict[str, str]:
    dialect = postgresql.dialect()
    return {column.name: column.type.compile(dialect=dialect) for column in columns}


def test_the_columns_are_the_column_fields_in_order_with_compile_model_types() -> None:
    table = sa_table_for(_Meter, "meters")
    compiled = compile_model(_Meter, metadata=MetaData()).__table__

    assert table.name == "meters"
    assert [column.name for column in table.columns] == [
        "id",
        "label",
        "amount",
        "shade",
        "taken_on",
        "opens_at",
        "blob",
        "owner_id",
    ]
    assert _ddl_types(table.columns) == _ddl_types(compiled.columns)


def test_keys_constraints_defaults_and_nullability_are_not_carried() -> None:
    table = sa_table_for(_Meter, "meters")
    compiled = compile_model(_Meter, metadata=MetaData()).__table__

    assert not compiled.c.owner_id.nullable
    assert list(table.primary_key.columns) == []
    assert table.constraints == {table.primary_key}
    assert table.indexes == set()
    assert all(column.default is None and column.server_default is None for column in table.columns)
    assert all(column.nullable for column in table.columns)


def test_the_table_is_bound_to_a_fresh_metadata_and_registered_nowhere() -> None:
    class _Plain(msgspec.Struct):
        reading_id: int

    first = sa_table_for(_Plain, "readings")
    second = sa_table_for(_Plain, "readings")

    assert first.metadata is not second.metadata
    assert list(first.metadata.tables) == ["readings"]
    assert get_compiled(_Plain) is None


class _UndeclaredMode(LoomStruct):
    mode: _Mode


class _UndeclaredAmount(LoomStruct):
    amount: Decimal


class _UndeclaredKind(LoomStruct):
    kind: Literal["first", "second"]


@pytest.mark.parametrize(
    ("struct_cls", "field"),
    [
        (_UndeclaredMode, "_UndeclaredMode.mode"),
        (_UndeclaredAmount, "_UndeclaredAmount.amount"),
        (_UndeclaredKind, "_UndeclaredKind.kind"),
    ],
    ids=["plain_enum", "decimal", "literal"],
)
def test_a_field_without_a_declared_column_type_is_refused(
    struct_cls: type[Any], field: str
) -> None:
    with pytest.raises(ValueError, match=rf"^{field}: no column type is inferred"):
        sa_table_for(struct_cls, "t")
