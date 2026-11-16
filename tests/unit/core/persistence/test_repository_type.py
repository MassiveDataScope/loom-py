"""``repository_type`` is a fact of the backend class, not of the wiring."""

from __future__ import annotations

from loom.core.stores.none import NoneBackend
from loom.stores.dynamodb.backend import DynamoDBBackend
from loom.stores.dynamodb.repository import RepositoryDynamoDB
from loom.stores.mongo.backend import MongoBackend
from loom.stores.mongo.repository import RepositoryMongo
from loom.stores.sqlalchemy.backend import SQLAlchemyBackend
from loom.stores.sqlalchemy.repository import RepositorySQLAlchemy


class TestRepositoryType:
    def test_each_backend_names_the_repository_it_serves(self) -> None:
        assert SQLAlchemyBackend.repository_type is RepositorySQLAlchemy
        assert DynamoDBBackend.repository_type is RepositoryDynamoDB
        assert MongoBackend.repository_type is RepositoryMongo
        assert NoneBackend.repository_type is None
