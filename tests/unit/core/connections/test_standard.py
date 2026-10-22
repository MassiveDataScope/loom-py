"""Every connection type follows the standard: the same names and units wherever an engine has them.

Walks every connection and pool type loom defines, the contexts' subclasses
included, so a field named the old way, or a duration in milliseconds, fails
here before it reaches a section. Each engine type also translates its fields
to its driver in ``client_options()``.
"""

from __future__ import annotations

import pytest

import loom.core.connections.aws
import loom.core.connections.clickhouse
import loom.core.connections.mongo
import loom.core.persistence.database
import loom.core.repository.dynamodb.backend
import loom.core.repository.mongo.backend
import loom.core.sql.config
import loom.etl.storage._config
import loom.streaming.mongo._config
from loom.core.connections import ConnectionPool, ConnectionSettings

# Imported for the connection types they define; the walk below finds them.
_DEFINING_MODULES = (
    loom.core.connections.aws,
    loom.core.connections.clickhouse,
    loom.core.connections.mongo,
    loom.core.persistence.database,
    loom.core.repository.dynamodb.backend,
    loom.core.repository.mongo.backend,
    loom.core.sql.config,
    loom.etl.storage._config,
    loom.streaming.mongo._config,
)

_RETIRED_NAMES = frozenset(
    {
        "uri",
        "endpoint_url",
        "pool_size",
        "max_pool_size",
        "max_pool_connections",
        "send_receive_timeout",
    }
)


def _loom_subclasses(root: type) -> list[type]:
    found: list[type] = []
    pending = list(root.__subclasses__())
    while pending:
        cls = pending.pop()
        if cls.__module__.startswith("loom."):
            found.append(cls)
        pending.extend(cls.__subclasses__())
    return sorted(found, key=lambda cls: cls.__qualname__)


_TYPES = [*_loom_subclasses(ConnectionSettings), *_loom_subclasses(ConnectionPool)]


def test_the_walk_finds_every_engine_and_context() -> None:
    names = {cls.__name__ for cls in _TYPES}

    assert {
        "AwsClientConfig",
        "AwsPool",
        "ClickHouseConnection",
        "DatabaseConfig",
        "DynamoDBRepositoryConfig",
        "MongoConnection",
        "MongoPool",
        "MongoRepositoryConfig",
        "MongoSourceConfig",
        "PoolConfig",
        "SqlConnectionConfig",
    } <= names


@pytest.mark.parametrize("kind", _TYPES, ids=lambda cls: cls.__name__)
def test_no_field_uses_a_retired_name_or_milliseconds(kind: type[ConnectionSettings]) -> None:
    offending = sorted(
        name for name in kind.__struct_fields__ if name in _RETIRED_NAMES or name.endswith("_ms")
    )

    assert offending == []


_ENGINE_TYPES = _loom_subclasses(ConnectionSettings)
_ENGINES = [cls for cls in _ENGINE_TYPES if ConnectionSettings in cls.__bases__]


def test_each_engine_type_translates_its_fields_for_its_driver() -> None:
    assert sorted(cls.__name__ for cls in _ENGINES) == [
        "AwsClientConfig",
        "ClickHouseConnection",
        "DatabaseConfig",
        "MongoConnection",
    ]
    assert all("client_options" in vars(cls) for cls in _ENGINES)
