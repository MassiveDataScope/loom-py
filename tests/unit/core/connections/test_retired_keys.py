"""A key the connection standard retired fails at startup, naming its replacement, everywhere."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.persistence.profile import SERVER_PROFILE, BackendRuntime
from loom.core.repository.dynamodb.backend import DynamoDBBackend
from loom.core.sql.config import reject_retired_sql_keys
from loom.etl.storage._config import normalise_storage_section
from loom.streaming.mongo._config import load_mongo_config

_AWS_RETIRED = [
    ("endpoint_url", "http://localhost:8000", "url"),
    ("max_pool_connections", 8, "pool.size"),
]
_MONGO_RETIRED = [
    ("uri", "mongodb://h", "url"),
    ("max_pool_size", 8, "pool.size"),
    ("server_selection_timeout_ms", 500, "server_selection_timeout, in seconds"),
]
_CLICKHOUSE_RETIRED = [
    ("send_receive_timeout", 60, "read_timeout"),
    ("pool_size", 4, "pool.size"),
]


def _names(path: str, retired: str, replacement: str) -> str:
    return rf"{path}\.{retired} was removed; use {path}\.{replacement}"


@pytest.mark.parametrize(("retired", "value", "replacement"), _AWS_RETIRED)
def test_persistence_dynamodb(retired: str, value: Any, replacement: str) -> None:
    section = {"region": "eu-west-1", "table": "t", retired: value}
    ctx = ConfigContext.from_dict({"persistence": {"backend": "dynamodb", "dynamodb": section}})

    with pytest.raises(ConfigError, match=_names("persistence.dynamodb", retired, replacement)):
        DynamoDBBackend().build(ctx, (), BackendRuntime(SERVER_PROFILE))


@pytest.mark.parametrize(("retired", "value", "replacement"), _MONGO_RETIRED)
def test_persistence_mongo(retired: str, value: Any, replacement: str) -> None:
    pytest.importorskip("pymongo")
    from loom.core.repository.mongo.backend import MongoBackend

    section = {"url": "mongodb://h", "database": "app", retired: value}
    ctx = ConfigContext.from_dict({"persistence": {"backend": "mongo", "mongo": section}})

    with pytest.raises(ConfigError, match=_names("persistence.mongo", retired, replacement)):
        MongoBackend().build(ctx, (), BackendRuntime(SERVER_PROFILE))


@pytest.mark.parametrize(
    ("section", "base", "retired", "value", "replacement"),
    [
        *[("dynamodb", {"region": "eu-west-1"}, *case) for case in _AWS_RETIRED],
        *[("mongo", {"url": "mongodb://h", "database": "app"}, *case) for case in _MONGO_RETIRED],
        *[("clickhouse", {"url": "clickhouse://h/db"}, *case) for case in _CLICKHOUSE_RETIRED],
    ],
)
def test_storage_engine_sections(
    section: str, base: dict[str, Any], retired: str, value: Any, replacement: str
) -> None:
    with pytest.raises(ConfigError, match=_names(f"storage.{section}", retired, replacement)):
        normalise_storage_section({section: {**base, retired: value}})


@pytest.mark.parametrize(("retired", "value", "replacement"), _CLICKHOUSE_RETIRED)
def test_sql_connections(retired: str, value: Any, replacement: str) -> None:
    raw = {"connections": {"analytics": {"backend": "clickhouse", "url": "x", retired: value}}}

    named = _names("sql.connections.analytics", retired, replacement)
    with pytest.raises(ConfigError, match=named):
        reject_retired_sql_keys(raw)


def test_the_sql_registry_refuses_a_retired_key_before_decoding() -> None:
    pytest.importorskip("clickhouse_connect")
    from loom.core.sql.clickhouse import ClickHouseConnectionRegistry

    raw = {"connections": {"a": {"backend": "clickhouse", "url": "http://h", "pool_size": 2}}}

    with pytest.raises(ConfigError, match=_names("sql.connections.a", "pool_size", "pool.size")):
        ClickHouseConnectionRegistry.from_config(raw)


@pytest.mark.parametrize(("retired", "value", "replacement"), _CLICKHOUSE_RETIRED)
def test_a_streaming_sink_database(retired: str, value: Any, replacement: str) -> None:
    from loom.streaming.nodes._table.config import _clickhouse_connection

    database = {"url": "clickhouse://h/db", retired: value}
    ctx = ConfigContext.from_dict({"database": {"warehouse": database}})

    with pytest.raises(ConfigError, match=_names("database.warehouse", retired, replacement)):
        _clickhouse_connection(ctx, "database.warehouse")


@pytest.mark.parametrize(("retired", "value", "replacement"), _MONGO_RETIRED)
@pytest.mark.parametrize("path", ["mongo.source", "mongo.sources.orders"])
def test_cdc_mongo_sources(path: str, retired: str, value: Any, replacement: str) -> None:
    source = {"url": "mongodb://h", "database": "app", retired: value}
    section: dict[str, Any] = (
        {"source": source} if path == "mongo.source" else {"sources": {"orders": source}}
    )
    ctx = ConfigContext.from_dict({"mongo": section})

    with pytest.raises(ConfigError, match=_names(path, retired, replacement)):
        load_mongo_config(ctx)


def test_cdc_mongo_sources_decode_without_a_retired_key() -> None:
    ctx = ConfigContext.from_dict(
        {"mongo": {"source": {"url": "mongodb://h", "database": "app", "pool": {"size": 4}}}}
    )

    source = load_mongo_config(ctx).source

    assert source is not None
    assert source.pool.size == 4
