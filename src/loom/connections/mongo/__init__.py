"""MongoDB: the loader of the island that builds the driver client.

Importing this package loads no SDK; the client is built by
:mod:`loom.connections.mongo.client`, loaded with :func:`mongo_island`.
"""

from __future__ import annotations

from loom.connections.mongo.loader import MongoIsland, mongo_island

__all__ = ["MongoIsland", "mongo_island"]
