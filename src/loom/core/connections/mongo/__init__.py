"""MongoDB: the connection every context shares, and the typed loader of its island.

Importing this package loads no SDK; the client is built by
:mod:`loom.core.connections.mongo.client`, loaded with :func:`mongo_island`.
"""

from __future__ import annotations

from loom.core.connections.mongo.config import MongoConnection, MongoPool
from loom.core.connections.mongo.loader import MongoIsland, mongo_island

__all__ = ["MongoConnection", "MongoIsland", "MongoPool", "mongo_island"]
