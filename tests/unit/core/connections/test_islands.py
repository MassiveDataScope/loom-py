"""The options ``client_options()`` and each island produce are accepted by the real driver.

``test_client_options.py`` pins the names; this suite checks the drivers take
them, so a renamed driver option fails here rather than at a deployment's
first connection. Each test is skipped when its SDK is not installed.
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from loom.core.connections import (
    AwsClientConfig,
    ClickHouseConnection,
    ConnectionPool,
    MongoConnection,
)
from loom.core.connections.aws import AwsPool
from loom.core.connections.mongo import MongoPool
from loom.core.model.redaction import Dsn, Secret
from loom.core.persistence.database import DatabaseConfig
from loom.core.plugins.optional import import_optional


def _parameters(function: Any) -> set[str]:
    return {
        name
        for name, parameter in inspect.signature(function).parameters.items()
        if parameter.kind is not inspect.Parameter.VAR_KEYWORD
    }


def test_botocore_and_boto3_take_the_aws_options() -> None:
    boto3 = pytest.importorskip("boto3")
    from botocore.config import Config

    options = AwsClientConfig(url=Dsn("http://localhost:8000")).client_options()

    Config(**options["config"])  # an unknown keyword raises TypeError
    assert set(options) <= _parameters(boto3.session.Session.client)


def test_the_aws_island_builds_a_client_with_every_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("boto3")
    # Static credentials: building a client resolves them, and the host's own
    # configuration must not decide the test.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    island = import_optional("loom.core.connections.aws.client", extra="dynamodb")
    config = AwsClientConfig(
        region="eu-west-1",
        url=Dsn("http://localhost:8000"),
        pool=AwsPool(size=4),
        connect_timeout=1,
        read_timeout=7,
        max_attempts=2,
    )

    client = island.build_aws_client("dynamodb", config)

    assert client.meta.endpoint_url == "http://localhost:8000"
    botocore = client.meta.config
    bounds = (botocore.max_pool_connections, botocore.connect_timeout, botocore.read_timeout)
    assert bounds == (4, 1, 7)
    assert botocore.retries["total_max_attempts"] == 2


def test_pymongo_takes_the_mongo_options_in_its_units() -> None:
    pytest.importorskip("pymongo")
    island = import_optional("loom.core.connections.mongo.client", extra="mongo")
    connection = MongoConnection(
        url=Dsn("mongodb://h:27017"),
        database="app",
        username="u",
        password=Secret("p"),
        connect_timeout=2,
        read_timeout=30,
        server_selection_timeout=5,
        pool=MongoPool(size=8, timeout=1),
        server_api_version="1",
    )

    # pymongo refuses an unknown option when the client is built.
    client = island.build_mongo_client(connection, connect=False)
    try:
        pool = client.options.pool_options
        assert (pool.max_pool_size, pool.connect_timeout, pool.socket_timeout) == (8, 2.0, 30.0)
        assert pool.wait_queue_timeout == 1.0
        assert client.options.server_selection_timeout == 5.0
        assert pool.server_api is not None
        assert pool.server_api.version == "1"
    finally:
        client.close()


def test_clickhouse_connect_takes_the_clickhouse_options(monkeypatch: pytest.MonkeyPatch) -> None:
    clickhouse_connect = pytest.importorskip("clickhouse_connect")
    from clickhouse_connect.driver.httpclient import HttpClient

    island = import_optional("loom.core.connections.clickhouse.client", extra="clickhouse")
    connection = ClickHouseConnection(
        url=Dsn("clickhouse://h:8123/db"),
        username="u",
        password=Secret("p"),
        connect_timeout=3,
        read_timeout=40,
        executor_threads=2,
        pool=ConnectionPool(size=5),
    )
    accepted = _parameters(clickhouse_connect.get_client)
    accepted_async = _parameters(clickhouse_connect.get_async_client)
    recorded: dict[str, Any] = {}
    monkeypatch.setattr(island.clickhouse_connect, "get_client", lambda **kw: recorded.update(kw))

    island.build_clickhouse_client(connection)
    async_options = island.async_client_options(connection)

    # get_client hands what it does not name to HttpClient, which names the rest.
    http = _parameters(HttpClient.__init__)
    assert set(recorded) <= accepted | http
    assert set(async_options) <= accepted_async | http
    assert {"pool_mgr", "send_receive_timeout"} <= set(recorded)


def test_asyncpg_takes_the_relational_timeouts() -> None:
    asyncpg = pytest.importorskip("asyncpg")
    config = DatabaseConfig(
        url=Dsn("postgresql+asyncpg://h/db"), connect_timeout=3, read_timeout=30
    )

    assert set(config.client_options()) <= _parameters(asyncpg.connect)
