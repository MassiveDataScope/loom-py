"""A Mongo repository refuses a model whose primary key is composite."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigError
from loom.core.model import BaseModel, ColumnField
from loom.core.stores.repository import CursorCodec
from loom.stores.mongo.repository import RepositoryMongo

from ._fake import FakeCollection


class AcmeMongoLine(BaseModel):
    __tablename__ = "lines"
    order_id: str = ColumnField(primary_key=True)
    id: str = ColumnField(primary_key=True)


class TestMongoCompositeKey:
    def test_a_composite_primary_key_is_refused(self) -> None:
        collection = FakeCollection()
        with pytest.raises(
            ConfigError, match=r"AcmeMongoLine has the composite primary key \(order_id, id\)"
        ):
            RepositoryMongo(
                AcmeMongoLine, collection, cursor_codec=CursorCodec(signing="ephemeral")
            )
