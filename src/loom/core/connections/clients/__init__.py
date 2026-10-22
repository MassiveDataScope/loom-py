"""Client builders, one island per engine, and the typed way to load each.

Each module imports its driver at its top. Every context loads it with the
loader named after its engine, which wraps
:func:`~loom.core.plugins.optional.import_optional`: this package imports none
of the islands, so naming it requires no extra.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, cast

from loom.core.plugins.optional import import_optional

if TYPE_CHECKING:
    from loom.core.connections.aws import AwsClientConfig
    from loom.core.connections.clickhouse import ClickHouseConnection
    from loom.core.connections.mongo import MongoConnection


class AwsIsland(Protocol):
    """What :mod:`loom.core.connections.clients.aws` publishes."""

    def build_aws_client(self, service: str, config: AwsClientConfig, **extra: Any) -> Any: ...


class MongoIsland(Protocol):
    """What :mod:`loom.core.connections.clients.mongo` publishes."""

    def build_mongo_client(self, connection: MongoConnection, **extra: Any) -> Any: ...

    def build_async_mongo_client(self, connection: MongoConnection, **extra: Any) -> Any: ...


class ClickHouseIsland(Protocol):
    """What :mod:`loom.core.connections.clients.clickhouse` publishes to contexts."""

    def build_clickhouse_client(self, connection: ClickHouseConnection, **extra: Any) -> Any: ...


def aws_island(*, extra: str) -> AwsIsland:
    """Load the AWS island; *extra* is the loom extra that brings boto3 to the caller.

    Raises:
        MissingExtraError: When boto3 is not installed.
    """
    return cast(AwsIsland, import_optional("loom.core.connections.clients.aws", extra=extra))


def mongo_island() -> MongoIsland:
    """Load the pymongo island.

    Raises:
        MissingExtraError: When pymongo is not installed.
    """
    return cast(MongoIsland, import_optional("loom.core.connections.clients.mongo", extra="mongo"))


def clickhouse_island() -> ClickHouseIsland:
    """Load the clickhouse-connect island.

    Raises:
        MissingExtraError: When clickhouse-connect is not installed.
    """
    return cast(
        ClickHouseIsland,
        import_optional("loom.core.connections.clients.clickhouse", extra="clickhouse"),
    )


__all__ = [
    "AwsIsland",
    "ClickHouseIsland",
    "MongoIsland",
    "aws_island",
    "clickhouse_island",
    "mongo_island",
]
