from __future__ import annotations

import datetime as dt

import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from loom.core.backend.sqlalchemy import compile_all, scoped_tables
from loom.core.model import BaseModel, ColumnField, Privilege, RowScoped, ScopedField
from loom.core.model.types import DateTime, Integer, String, Text


class Event(BaseModel, RowScoped):
    __tablename__ = "events"
    __scope_privileges__ = frozenset({Privilege.SELECT})
    __partition_by__ = ("RANGE", "at")
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    at: dt.datetime = ColumnField(DateTime(), primary_key=True)
    kind: str = ColumnField(Text)


class Item(BaseModel, RowScoped):
    __tablename__ = "items"
    owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
    id: int = ColumnField(Integer, primary_key=True)


def _metadata(*models: type) -> MetaData:
    metadata = MetaData()
    compile_all(*models, metadata=metadata)
    return metadata


def test_a_range_partitioned_model_compiles_to_a_partitioned_table() -> None:
    metadata = _metadata(Event)

    ddl = str(CreateTable(metadata.tables["events"]).compile(dialect=postgresql.dialect()))

    assert ddl.rstrip().endswith("PARTITION BY RANGE (at)")


def test_the_scoped_table_records_its_partition_column() -> None:
    metadata = _metadata(Event, Item)

    scoped = scoped_tables(metadata)

    assert scoped[(None, "events")].partition_by == "at"
    assert scoped[(None, "items")].partition_by is None


def test_an_invalid_partition_declaration_fails_at_compile_time() -> None:
    class Wrong(BaseModel, RowScoped):
        __tablename__ = "wrong"
        __partition_by__ = ("HASH", "at")
        owner_id: str = ScopedField(String(36), primary_key=True, scope="owner")
        at: dt.datetime = ColumnField(DateTime(), primary_key=True)

    with pytest.raises(ValueError, match="only RANGE partitioning"):
        _metadata(Wrong)
