"""``repository_type`` is a fact of the backend class, not of the wiring."""

from __future__ import annotations

from loom.core.stores.none import NoneBackend
from loom.stores.dynamodb.backend import DynamoDBBackend
from loom.stores.dynamodb.repository import DynamoDBRepository
from loom.stores.mongo.backend import MongoBackend
from loom.stores.mongo.repository import MongoRepository
from loom.stores.sqlalchemy.backend import SQLAlchemyBackend
from loom.stores.sqlalchemy.repository import SQLAlchemyRepository


class TestRepositoryType:
    def test_each_backend_names_the_repository_it_serves(self) -> None:
        assert SQLAlchemyBackend.repository_type is SQLAlchemyRepository
        assert DynamoDBBackend.repository_type is DynamoDBRepository
        assert MongoBackend.repository_type is MongoRepository
        assert NoneBackend.repository_type is None
