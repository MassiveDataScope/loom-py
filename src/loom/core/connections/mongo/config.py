"""MongoDB connection settings shared by the repository, the ETL and CDC streaming.

The repository (``persistence.mongo``), the ETL sources (``storage.mongo``) and
the change-stream sources (``mongo.source`` / ``mongo.sources.<name>``) declare
:class:`MongoConnection` or a subclass of it, and build their clients through
:mod:`loom.core.connections.mongo.client`. Unset bounds keep the driver's
defaults.
"""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any, ClassVar

import msgspec

from loom.core.connections._base import ConnectionPool, ConnectionSettings, require_text
from loom.core.model.types import PositiveInt

_MS_PER_SECOND = 1000


class MongoPool(ConnectionPool, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """``pool`` of a Mongo client.

    Args:
        size: Connections the client keeps per server; ``None`` is the driver
            default (100).
        timeout: Seconds an operation waits for a free connection; ``None``
            waits as long as the driver does (no limit).
    """

    timeout: PositiveInt | None = None


class MongoConnection(ConnectionSettings, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Address, credentials and client bounds of one MongoDB deployment.

    Args:
        url: pymongo connection string; its credentials never show in a repr.
        database: Database the context reads or writes.
        username: Username; wins over the one of the URL.
        password: Password; wins over the one of the URL, never shown in a repr.
        connect_timeout: Seconds to establish a connection; ``None`` is the
            driver default (20).
        read_timeout: Seconds to wait for one operation's response; ``None``
            waits as long as the driver does (no limit).
        server_selection_timeout: Seconds to find a suitable server; ``None``
            is the driver default (30).
        pool: Connections the client keeps and how long one is waited for.
        server_api_version: Stable API version to declare, e.g. ``"1"``.
    """

    _RETIRED_KEYS: ClassVar[Mapping[str, str]] = MappingProxyType(
        {
            "uri": "url",
            "max_pool_size": "pool.size",
            "server_selection_timeout_ms": "server_selection_timeout, in seconds",
        }
    )

    url: str
    database: str
    username: str | None = None
    password: str | None = None
    connect_timeout: PositiveInt | None = None
    read_timeout: PositiveInt | None = None
    server_selection_timeout: PositiveInt | None = None
    pool: MongoPool = msgspec.field(default_factory=MongoPool)
    server_api_version: str | None = None

    def __post_init__(self) -> None:
        require_text(self.url, "url")
        require_text(self.database, "database")

    def client_options(self) -> dict[str, Any]:
        """Return the ``MongoClient`` keyword options, the URL as ``host``, timeouts in ms.

        ``server_api_version`` is left to the island, which builds the driver's
        ``ServerApi`` from it.
        """
        options: dict[str, Any] = {"host": self.url}
        named = {
            "username": self.username,
            "password": self.password,
            "maxPoolSize": self.pool.size,
        }
        options.update({name: value for name, value in named.items() if value is not None})
        seconds = {
            "connectTimeoutMS": self.connect_timeout,
            "socketTimeoutMS": self.read_timeout,
            "serverSelectionTimeoutMS": self.server_selection_timeout,
            "waitQueueTimeoutMS": self.pool.timeout,
        }
        options.update(
            {name: value * _MS_PER_SECOND for name, value in seconds.items() if value is not None}
        )
        return options


__all__ = ["MongoConnection", "MongoPool"]
