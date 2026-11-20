"""Connection settings, one type per engine, shared by every loom context.

A context using an engine declares the engine's type here or a subclass
adding only its own keys, so the same keys validate the same way wherever
the engine is configured. Each engine has a package of its own
(:mod:`~loom.core.connections.aws`, :mod:`~loom.core.connections.mongo`,
:mod:`~loom.core.connections.clickhouse`): its connection type and, for AWS,
the port that serves the clients. The driver clients are built by the adapters
of :mod:`loom.connections`. Importing this package or an engine package loads
no driver.

The relational type is not re-exported here: it lives in
:mod:`loom.core.stores.database`, and importing it from this package would
make every reader of a connection type, the config resolvers among them, load
the store ports.
"""

from __future__ import annotations

from loom.core.connections._base import (
    ConnectionPool,
    ConnectionSettings,
    merge_extra,
    redact_uri,
    require_text,
)
from loom.core.connections.aws import AwsClientConfig, RetryMode
from loom.core.connections.clickhouse import ClickHouseConnection
from loom.core.connections.mongo import MongoConnection

__all__ = [
    "AwsClientConfig",
    "ClickHouseConnection",
    "ConnectionPool",
    "ConnectionSettings",
    "MongoConnection",
    "RetryMode",
    "merge_extra",
    "redact_uri",
    "require_text",
]
