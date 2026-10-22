"""MongoDB connection settings shared by the repository, the ETL and CDC streaming.

The repository (``persistence.mongo``), the ETL sources (``storage.mongo``) and
the change-stream sources (``mongo.source`` / ``mongo.sources.<name>``) declare
:class:`MongoConnection` or a subclass of it, and build their clients through
:mod:`loom.core.connections.clients.mongo`. Unset bounds keep the driver's
defaults.
"""

from __future__ import annotations

from typing import ClassVar

from loom.core.connections._base import ConnectionSettings, require_text
from loom.core.model.types import NonNegativeInt, PositiveInt


class MongoConnection(ConnectionSettings, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Address and client options of one MongoDB deployment.

    Args:
        uri: pymongo connection string; its credentials never show in a repr.
        database: Database the context reads or writes.
        max_pool_size: Connections the client keeps; ``0`` means no limit,
            ``None`` the driver default (100).
        server_selection_timeout_ms: Milliseconds to find a server; ``None``
            is the driver default (30000).
        server_api_version: Stable API version to declare, e.g. ``"1"``.
    """

    _URI_FIELDS: ClassVar[frozenset[str]] = frozenset({"uri"})

    uri: str
    database: str
    max_pool_size: NonNegativeInt | None = None
    server_selection_timeout_ms: PositiveInt | None = None
    server_api_version: str | None = None

    def __post_init__(self) -> None:
        require_text(self.uri, "uri")
        require_text(self.database, "database")


__all__ = ["MongoConnection"]
