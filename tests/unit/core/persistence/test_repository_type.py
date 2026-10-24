"""``repository_type`` is a fact of the backend class, not of the wiring."""

from __future__ import annotations

from loom.core.persistence.none import NoneBackend
from loom.core.repository.dynamodb.backend import DynamoDBBackend
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from loom.core.repository.mongo.backend import MongoBackend
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.sqlalchemy.backend import SQLAlchemyBackend
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy


def test_each_backend_names_the_repository_it_serves() -> None:
    assert SQLAlchemyBackend.repository_type is RepositorySQLAlchemy
    assert DynamoDBBackend.repository_type is RepositoryDynamoDB
    assert MongoBackend.repository_type is RepositoryMongo
    assert NoneBackend.repository_type is None
