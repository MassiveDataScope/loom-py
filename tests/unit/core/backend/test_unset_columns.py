"""A field annotated ``X | UnsetType`` compiles to ``X``'s column."""

from __future__ import annotations

from decimal import Decimal

import msgspec
import pytest
from sqlalchemy import MetaData

from loom.core.backend.sqlalchemy import compile_all
from loom.core.model import BaseModel, ColumnField


class _Badge(BaseModel):
    __tablename__ = "unset_compiled_badges"

    id: int = ColumnField(primary_key=True)
    label: str | msgspec.UnsetType = msgspec.UNSET
    note: str | None | msgspec.UnsetType = msgspec.UNSET


def test_an_unset_field_compiles_to_its_scalar_column() -> None:
    metadata = MetaData()
    compile_all(_Badge, metadata=metadata)
    columns = metadata.tables["unset_compiled_badges"].c

    assert (type(columns.label.type).__name__, columns.label.nullable) == ("String", False)
    assert (type(columns.note.type).__name__, columns.note.nullable) == ("String", True)


def test_an_unset_decimal_needs_a_declared_column_type_like_a_decimal() -> None:
    class _Priced(BaseModel):
        __tablename__ = "unset_priced"

        id: int = ColumnField(primary_key=True)
        price: Decimal | msgspec.UnsetType = msgspec.UNSET

    metadata = MetaData()

    with pytest.raises(ValueError, match="no column type is inferred from a Decimal"):
        compile_all(_Priced, metadata=metadata)
