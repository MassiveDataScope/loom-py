"""Connection settings, one type per engine, shared by every loom context.

A context using an engine declares the engine's type here or a subclass
adding only its own keys, so the same keys validate the same way wherever
the engine is configured. This package imports no driver: the client
builders live in the islands of :mod:`loom.core.connections.clients`, loaded
with :func:`~loom.core.plugins.optional.import_optional`.

The relational type is not re-exported here: it lives with the persistence
layer (:mod:`loom.core.connections.relational` names it), and importing it
from this package would make every reader of a connection type, the config
resolvers among them, load the persistence layer.
"""

from __future__ import annotations

from loom.core.connections._base import ConnectionSettings, redact_uri
from loom.core.connections.aws import AwsClientConfig, RetryMode
from loom.core.connections.clickhouse import ClickHouseConnection
from loom.core.connections.mongo import MongoConnection

__all__ = [
    "AwsClientConfig",
    "ClickHouseConnection",
    "ConnectionSettings",
    "MongoConnection",
    "RetryMode",
    "redact_uri",
]
