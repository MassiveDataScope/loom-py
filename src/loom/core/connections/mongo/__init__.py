"""MongoDB: the connection every context shares.

The driver client is built by :mod:`loom.connections.mongo`; this package loads no SDK.
"""

from __future__ import annotations

from loom.core.connections.mongo.config import MongoConnection, MongoPool

__all__ = ["MongoConnection", "MongoPool"]
