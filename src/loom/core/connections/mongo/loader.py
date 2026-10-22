"""The typed way to load the pymongo island, and what the island publishes.

Importing this module loads no SDK: :func:`mongo_island` loads
:mod:`loom.core.connections.mongo.client` through
:func:`~loom.core.plugins.optional.import_optional`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, cast

from loom.core.plugins.optional import import_optional

if TYPE_CHECKING:
    from pymongo import AsyncMongoClient, MongoClient

    from loom.core.connections.mongo.config import MongoConnection

_ISLAND = "loom.core.connections.mongo.client"


class MongoIsland(Protocol):
    """What :mod:`loom.core.connections.mongo.client` publishes."""

    def build_mongo_client(self, connection: MongoConnection, **extra: Any) -> MongoClient[Any]: ...

    def build_async_mongo_client(
        self, connection: MongoConnection, **extra: Any
    ) -> AsyncMongoClient[Any]: ...


def mongo_island() -> MongoIsland:
    """Load the pymongo island.

    Raises:
        MissingExtraError: When pymongo is not installed.
    """
    return cast(MongoIsland, import_optional(_ISLAND, extra="mongo"))


__all__ = ["MongoIsland", "mongo_island"]
