"""``__database__`` selects a section. Nothing in this module opens a connection."""

from __future__ import annotations

import msgspec
import pytest

from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField
from loom.core.model.types import Integer
from loom.core.persistence.routing import database_of, first_named_database, group_models


class _Item(BaseModel):
    __tablename__ = "routing_items"
    id: int = ColumnField(Integer, primary_key=True)


class _Event(BaseModel):
    __tablename__ = "routing_events"
    __database__ = "tracking"
    id: int = ColumnField(Integer, primary_key=True)


def test_an_omitted_database_is_the_default_section() -> None:
    assert _Item.__database__ is None
    assert database_of(_Item) is None
    assert "__database__" not in {field.name for field in msgspec.structs.fields(_Item)}


def test_a_name_selects_that_section() -> None:
    assert database_of(_Event) == "tracking"
    assert first_named_database((_Item, _Event)) == (_Event, "tracking")
    assert first_named_database((_Item,)) is None


@pytest.mark.parametrize("value", ["pool", "database.tracking", "", "tracking-db"])
def test_a_database_name_must_be_a_section_name(value: str) -> None:
    class _Named(BaseModel):
        __tablename__ = "routing_named"
        __database__ = value
        id: int = ColumnField(Integer, primary_key=True)

    with pytest.raises(ConfigError, match="__database__"):
        database_of(_Named)


def test_the_default_group_comes_first() -> None:
    groups = group_models((_Event, _Item))

    assert [name for name, _models in groups] == [None, "tracking"]
    assert groups[0][1] == (_Item,)
    assert groups[1][1] == (_Event,)


def test_two_databases_cannot_share_a_table_name() -> None:
    class _Again(BaseModel):
        __tablename__ = "routing_items"
        __database__ = "tracking"
        id: int = ColumnField(Integer, primary_key=True)

    with pytest.raises(ConfigError, match="routing_items"):
        group_models((_Item, _Again))
