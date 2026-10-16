"""A Mongo repository refuses a model whose primary key is composite."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.mongo.repository import RepositoryMongo

from ._fake import FakeCollection


class _Line(BaseModel):
    __tablename__ = "lines"
    order_id: str = ColumnField(primary_key=True)
    id: str = ColumnField(primary_key=True)


def test_a_composite_primary_key_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"_Line has the composite primary key \(order_id, id\)"):
        RepositoryMongo(_Line, FakeCollection())
