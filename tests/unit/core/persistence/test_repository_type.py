"""``repository_type`` is a fact of the backend class, not of the wiring."""

from __future__ import annotations

from loom.core.stores.none import NullStorePlugin
from loom.stores.dynamodb.plugin import DynamoDBStorePlugin
from loom.stores.dynamodb.repository import DynamoDBRepository
from loom.stores.mongo.plugin import MongoStorePlugin
from loom.stores.mongo.repository import MongoRepository
from loom.stores.sqlalchemy.plugin import SQLAlchemyStorePlugin
from loom.stores.sqlalchemy.repository import SQLAlchemyRepository


class TestRepositoryType:
    def test_each_backend_names_the_repository_it_serves(self) -> None:
        assert SQLAlchemyStorePlugin.repository_type is SQLAlchemyRepository
        assert DynamoDBStorePlugin.repository_type is DynamoDBRepository
        assert MongoStorePlugin.repository_type is MongoRepository
        assert NullStorePlugin.repository_type is None
