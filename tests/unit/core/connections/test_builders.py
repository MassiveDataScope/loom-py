"""The client builders: every connection field reaches the driver, no context overrides one.

The driver entry points are replaced by recorders here; ``test_islands.py``
checks the same options against the real drivers' signatures.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from loom.core.connections import (
    AwsClientConfig,
    ClickHouseConnection,
    ConnectionPool,
    MongoConnection,
    merge_extra,
)
from loom.core.connections.aws import AwsPool
from loom.core.connections.mongo import MongoPool
from loom.core.model.redaction import Dsn, Secret
from loom.core.plugins.optional import import_optional
from tests.helpers.aws import REGION


class _Recorder:
    """A stand-in driver entry point recording how it was called."""

    def __init__(self) -> None:
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def __call__(self, *args: Any, **kwargs: Any) -> _Recorder:
        self.calls.append((args, kwargs))
        return self

    @property
    def kwargs(self) -> dict[str, Any]:
        return self.calls[-1][1]


# --------------------------------------------------------------------------- AWS


class TestAwsClientBuilder:
    def test_aws_builder_opens_a_session_of_the_profile_and_gives_every_bound(
        self, mocked_aws: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        profiles = tmp_path / "aws-config"
        profiles.write_text(
            f"[profile dev]\nregion = {REGION}\n"
            "aws_access_key_id = testing\naws_secret_access_key = testing\n",
            encoding="utf-8",
        )
        monkeypatch.setenv("AWS_CONFIG_FILE", str(profiles))
        island = import_optional("loom.core.connections.aws.client", extra="aws")
        config = AwsClientConfig(
            region="eu-west-1",
            url=Dsn("http://localhost:8000"),
            profile="dev",
            pool=AwsPool(size=4),
            connect_timeout=1,
            read_timeout=7,
            max_attempts=2,
            retry_mode="adaptive",
        )

        session = island.open_session("dev")
        client = island.build_aws_client(session, "dynamodb", config)

        assert session.profile_name == "dev"
        assert client.meta.service_model.service_name == "dynamodb"
        assert client.meta.endpoint_url == "http://localhost:8000"
        assert client.meta.region_name == "eu-west-1"
        botocore = client.meta.config
        bounds = (botocore.connect_timeout, botocore.read_timeout, botocore.max_pool_connections)
        assert bounds == (1, 7, 4)
        assert botocore.retries == {"total_max_attempts": 2, "mode": "adaptive"}


class TestMongoClientBuilder:
    @pytest.mark.parametrize(
        ("builder", "client_name"),
        [("build_mongo_client", "MongoClient"), ("build_async_mongo_client", "AsyncMongoClient")],
    )
    def test_mongo_builders_forward_the_connection_and_the_context_options(
        self, monkeypatch: pytest.MonkeyPatch, builder: str, client_name: str
    ) -> None:
        island = import_optional("loom.core.connections.mongo.client", extra="mongo")
        client = _Recorder()
        monkeypatch.setattr(island, client_name, client)
        connection = MongoConnection(
            url=Dsn("mongodb://h:27017"),
            database="app",
            pool=MongoPool(size=8),
            server_selection_timeout=3,
            server_api_version="1",
        )

        getattr(island, builder)(connection, tz_aware=True)

        args, kwargs = client.calls[-1]
        assert args == ()
        assert kwargs["host"] == "mongodb://h:27017"
        assert (kwargs["maxPoolSize"], kwargs["serverSelectionTimeoutMS"]) == (8, 3000)
        assert kwargs["server_api"].version == "1"
        assert kwargs["tz_aware"] is True

    def test_mongo_builder_leaves_unset_options_to_the_driver(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        island = import_optional("loom.core.connections.mongo.client", extra="mongo")
        client = _Recorder()
        monkeypatch.setattr(island, "MongoClient", client)

        island.build_mongo_client(MongoConnection(url=Dsn("mongodb://h"), database="app"))

        assert client.kwargs == {"host": "mongodb://h"}

    def test_mongo_builder_refuses_an_extra_the_connection_decides(self) -> None:
        island = import_optional("loom.core.connections.mongo.client", extra="mongo")
        connection = MongoConnection(url=Dsn("mongodb://h"), database="app", pool=MongoPool(size=4))

        with pytest.raises(ValueError, match="maxPoolSize"):
            island.build_mongo_client(connection, maxPoolSize=1)
        with pytest.raises(ValueError, match="host"):
            island.build_mongo_client(connection, host="mongodb://other")

    @pytest.mark.parametrize("option", ["maxpoolsize", "MAXPOOLSIZE", "SocketTimeoutMS", "Host"])
    def test_mongo_builder_compares_option_names_ignoring_case(self, option: str) -> None:
        island = import_optional("loom.core.connections.mongo.client", extra="mongo")
        connection = MongoConnection(
            url=Dsn("mongodb://h"), database="app", read_timeout=5, pool=MongoPool(size=4)
        )

        with pytest.raises(ValueError, match=option):
            island.build_mongo_client(connection, **{option: 1})


class TestMergeExtraOptionNames:
    def test_merge_extra_keeps_the_case_of_other_drivers(self) -> None:
        connection = ClickHouseConnection(url=Dsn("clickhouse://h/db"))

        assert merge_extra({"dsn": "x"}, {"DSN": "y"}, connection) == {"dsn": "x", "DSN": "y"}


class TestClickHouseClientBuilder:
    def test_clickhouse_builder_forwards_only_what_is_declared(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        island = import_optional("loom.core.connections.clickhouse.client", extra="clickhouse")
        get_client = _Recorder()
        monkeypatch.setattr(island.clickhouse_connect, "get_client", get_client)

        island.build_clickhouse_client(ClickHouseConnection(url=Dsn("clickhouse://h:8123/db")))

        assert get_client.kwargs == {"dsn": "clickhouse://h:8123/db"}

    def test_clickhouse_builder_forwards_credentials_and_bounds(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        island = import_optional("loom.core.connections.clickhouse.client", extra="clickhouse")
        get_client = _Recorder()
        monkeypatch.setattr(island.clickhouse_connect, "get_client", get_client)
        connection = ClickHouseConnection(
            url=Dsn("clickhouse://h:8123/db"),
            username="u",
            password=Secret("p"),
            connect_timeout=3,
            read_timeout=40,
            executor_threads=2,
            pool=ConnectionPool(size=5),
        )

        island.build_clickhouse_client(connection)

        kwargs = get_client.kwargs
        assert (kwargs["username"], kwargs["password"]) == ("u", "p")
        assert (kwargs["connect_timeout"], kwargs["send_receive_timeout"]) == (3, 40)
        assert "pool_mgr" in kwargs
        assert "executor_threads" not in kwargs  # a synchronous client has no thread pool

    def test_async_clickhouse_builder_sizes_its_thread_pool(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        island = import_optional("loom.core.connections.clickhouse.client", extra="clickhouse")
        recorded: dict[str, Any] = {}

        async def get_async_client(**kwargs: Any) -> str:
            recorded.update(kwargs)
            return "client"

        monkeypatch.setattr(island.clickhouse_connect, "get_async_client", get_async_client)
        connection = ClickHouseConnection(url=Dsn("clickhouse://h/db"), executor_threads=3)

        options = island.async_client_options(connection, query_retries=0)
        client = asyncio.run(island.open_async_client(options))

        assert client == "client"
        assert recorded == {"dsn": "clickhouse://h/db", "executor_threads": 3, "query_retries": 0}

    def test_clickhouse_builder_refuses_an_extra_the_connection_decides(self) -> None:
        island = import_optional("loom.core.connections.clickhouse.client", extra="clickhouse")

        with pytest.raises(ValueError, match="password"):
            island.build_clickhouse_client(
                ClickHouseConnection(url=Dsn("clickhouse://h/db")), password="x"
            )
