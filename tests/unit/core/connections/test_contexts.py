"""The same engine connection is configured the same way in every context using it.

One connection per engine is written into each section the contract names;
each context must accept it with the same values and refuse the same unknown
key. Retired keys name their replacement.
"""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.connections import AwsClientConfig, ClickHouseConnection, MongoConnection
from loom.core.persistence.database import load_database_config
from loom.core.repository.dynamodb.backend import DynamoDBRepositoryConfig
from loom.core.repository.mongo.backend import MongoRepositoryConfig
from loom.core.sql.config import SqlConnectionConfig
from loom.etl.storage._config import StorageConfig, normalise_storage_section
from loom.streaming.mongo import MongoSourceConfig

_AWS = {"region": "eu-west-1", "url": "http://localhost:8000", "read_timeout": 9}
_MONGO = {"url": "mongodb://h:27017", "database": "app", "server_selection_timeout": 4}
_CLICKHOUSE = {"url": "clickhouse://h:8123/db", "username": "u", "password": "p", "read_timeout": 2}


def _storage(**sections: Any) -> StorageConfig:
    return msgspec.convert(normalise_storage_section(sections), StorageConfig)


def _values(connection: Any, keys: dict[str, Any]) -> dict[str, Any]:
    return {key: getattr(connection, key) for key in keys}


# ----------------------------------------------------------------- same values


def test_aws_settings_read_the_same_in_the_repository_and_the_etl() -> None:
    repository = msgspec.convert({**_AWS, "table": "t"}, DynamoDBRepositoryConfig)
    etl = _storage(dynamodb=_AWS).dynamodb

    assert isinstance(repository, AwsClientConfig)
    assert etl is not None
    assert _values(repository, _AWS) == _values(etl, _AWS) == _AWS
    assert repository.connect_timeout == etl.connect_timeout == 2


def test_mongo_settings_read_the_same_in_the_repository_the_etl_and_cdc() -> None:
    repository = msgspec.convert({**_MONGO, "transactions": True}, MongoRepositoryConfig)
    etl = _storage(mongo=_MONGO).mongo
    cdc = msgspec.convert({**_MONGO, "on_oplog_expired": "fail"}, MongoSourceConfig)

    assert all(isinstance(c, MongoConnection) for c in (repository, etl, cdc))
    assert _values(repository, _MONGO) == _values(etl, _MONGO) == _values(cdc, _MONGO) == _MONGO


def test_clickhouse_settings_read_the_same_in_sql_and_the_etl() -> None:
    sql = msgspec.convert({**_CLICKHOUSE, "backend": "clickhouse"}, SqlConnectionConfig)
    etl = _storage(clickhouse=_CLICKHOUSE).clickhouse

    assert isinstance(sql, ClickHouseConnection)
    assert etl is not None
    assert _values(sql, _CLICKHOUSE) == _values(etl, _CLICKHOUSE) == _CLICKHOUSE


def test_each_context_keeps_its_own_bounds() -> None:
    sql = msgspec.convert(
        {"url": "clickhouse://h/db", "backend": "clickhouse"}, SqlConnectionConfig
    )
    etl = _storage(clickhouse={"url": "clickhouse://h/db"}).clickhouse

    assert (sql.connect_timeout, sql.read_timeout) == (10, 60)
    assert etl is not None
    assert (etl.connect_timeout, etl.read_timeout) == (None, None)  # driver defaults


# ------------------------------------------------------------ same refusals


@pytest.mark.parametrize(
    ("kind", "data"),
    [
        (DynamoDBRepositoryConfig, {**_AWS, "table": "t", "nope": 1}),
        (MongoRepositoryConfig, {**_MONGO, "nope": 1}),
        (MongoSourceConfig, {**_MONGO, "nope": 1}),
        (SqlConnectionConfig, {**_CLICKHOUSE, "backend": "clickhouse", "nope": 1}),
    ],
)
def test_an_unknown_key_fails_in_every_context(kind: type[Any], data: dict[str, Any]) -> None:
    with pytest.raises(msgspec.ValidationError, match="unknown field `nope`"):
        msgspec.convert(data, kind)


@pytest.mark.parametrize("section", ["dynamodb", "mongo", "clickhouse"])
def test_an_unknown_key_in_an_etl_engine_names_its_section(section: str) -> None:
    connection = {"dynamodb": _AWS, "mongo": _MONGO, "clickhouse": _CLICKHOUSE}[section]

    with pytest.raises(msgspec.ValidationError, match=rf"at `\$\.{section}`"):
        _storage(**{section: {**connection, "nope": 1}})


def test_the_repository_requires_its_region() -> None:
    with pytest.raises(msgspec.ValidationError, match="region is required"):
        msgspec.convert({"table": "t"}, DynamoDBRepositoryConfig)


# --------------------------------------------------------------- retired keys


def test_storage_dynamodb_enabled_names_its_replacement() -> None:
    with pytest.raises(ValueError, match=r"storage\.dynamodb\.enabled was removed"):
        _storage(dynamodb={"enabled": True, "region": "eu-west-1"})


@pytest.mark.parametrize(
    ("section", "data", "field"),
    [("mongo", {"url": "", "database": ""}, "url"), ("clickhouse", {"url": ""}, "url")],
)
def test_an_empty_address_no_longer_disables_an_etl_engine(
    section: str, data: dict[str, Any], field: str
) -> None:
    with pytest.raises(msgspec.ValidationError, match=f"{field} must not be empty"):
        _storage(**{section: data})


def test_an_omitted_etl_engine_is_absent() -> None:
    config = _storage()

    assert (config.dynamodb, config.mongo, config.clickhouse) == (None, None, None)


# --------------------------------------------------------- the relational one


def test_a_stray_key_in_database_schema_fails_naming_it() -> None:
    ctx = ConfigContext.from_dict(
        {"database": {"url": "sqlite+aiosqlite:///:memory:", "schema": {"guard_name": "g"}}}
    )

    with pytest.raises(ConfigError, match="guard_name"):
        load_database_config(ctx)


def test_database_schema_carries_every_key_its_readers_use() -> None:
    schema = {
        "mode": "external",
        "name": "app",
        "roles": {"owner": "app_owner", "migrator": "app_migrator"},
        "database_users": {"app_rw": {"login": True, "access": "write"}},
        "scopes": {"org": "identity.org_id"},
        "guard": "app_guard",
        "groups": {"readers": "app_r", "writers": "app_w"},
        "version_tables": {"structure": "v_s", "data": "v_d"},
        "naming_convention": {"pk": "pk_%(table_name)s"},
    }
    ctx = ConfigContext.from_dict(
        {"database": {"url": "postgresql+asyncpg://h/db", "schema": schema}}
    )

    decoded = load_database_config(ctx).schema

    assert decoded.roles is not None
    assert decoded.groups is not None
    assert (decoded.name, decoded.roles.owner, decoded.groups.writers) == (
        "app",
        "app_owner",
        "app_w",
    )


# ------------------------------------------------------- streaming sink databases


def _sink_connection(database: dict[str, Any]) -> ClickHouseConnection:
    from loom.streaming.nodes._table.config import _clickhouse_connection

    ctx = ConfigContext.from_dict({"database": {"warehouse": database}})
    return _clickhouse_connection(ctx, "database.warehouse")


def test_a_sink_database_is_the_shared_clickhouse_connection() -> None:
    connection = _sink_connection(_CLICKHOUSE)

    assert _values(connection, _CLICKHOUSE) == _CLICKHOUSE


def test_a_sink_database_with_http_url_names_its_new_key() -> None:
    with pytest.raises(ValueError, match=r"database\.warehouse\.http_url was renamed to url"):
        _sink_connection({"http_url": "http://h:8123"})


def test_a_relational_database_behind_a_clickhouse_sink_names_the_engine() -> None:
    with pytest.raises(ValueError, match=r"database\.warehouse holds a 'postgresql\+asyncpg'"):
        _sink_connection({"url": "postgresql+asyncpg://h/db"})


@pytest.mark.parametrize(
    "query", ["autogenerate_session_id=1", "query_retries=3", "role=admin", "password=x"]
)
def test_the_sql_url_may_not_set_an_option_the_service_decides(query: str) -> None:
    data = {"backend": "clickhouse", "url": f"clickhouse://h:8123/db?{query}"}

    with pytest.raises(msgspec.ValidationError, match="which the SQL service decides"):
        msgspec.convert(data, SqlConnectionConfig)


def test_the_sql_url_keeps_its_other_query_options() -> None:
    data = {"backend": "clickhouse", "url": "clickhouse://h:8123/db?compress=lz4"}

    assert msgspec.convert(data, SqlConnectionConfig).url.endswith("?compress=lz4")
