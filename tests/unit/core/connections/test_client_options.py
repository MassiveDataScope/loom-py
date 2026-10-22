"""``client_options()``: each engine's standard fields in its driver's names and units, SDK-free."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.config import ConfigError
from loom.core.connections import (
    AwsClientConfig,
    ClickHouseConnection,
    ConnectionPool,
    MongoConnection,
)
from loom.core.connections.aws import AwsPool
from loom.core.connections.mongo import MongoPool
from loom.core.persistence.database import DatabaseConfig, decode_database_config

# --------------------------------------------------------------------------- AWS


def test_aws_options_name_the_botocore_config_and_count_every_attempt() -> None:
    config = AwsClientConfig(
        url="http://localhost:8000",
        pool=AwsPool(size=4),
        connect_timeout=1,
        read_timeout=7,
        max_attempts=2,
        retry_mode="adaptive",
    )

    assert config.client_options() == {
        "endpoint_url": "http://localhost:8000",
        "config": {
            "max_pool_connections": 4,
            "connect_timeout": 1,
            "read_timeout": 7,
            "retries": {"total_max_attempts": 2, "mode": "adaptive"},
        },
    }


def test_aws_options_leave_the_regional_endpoint_to_boto3() -> None:
    options = AwsClientConfig().client_options()

    assert "endpoint_url" not in options
    assert options["config"]["max_pool_connections"] == 32


# ------------------------------------------------------------------------- Mongo


def test_mongo_options_are_pymongo_keywords_in_milliseconds() -> None:
    connection = MongoConnection(
        url="mongodb://h:27017",
        database="app",
        username="u",
        password="p",
        connect_timeout=2,
        read_timeout=30,
        server_selection_timeout=5,
        pool=MongoPool(size=8, timeout=1),
        server_api_version="1",
    )

    assert connection.client_options() == {
        "host": "mongodb://h:27017",
        "username": "u",
        "password": "p",
        "maxPoolSize": 8,
        "connectTimeoutMS": 2000,
        "socketTimeoutMS": 30000,
        "serverSelectionTimeoutMS": 5000,
        "waitQueueTimeoutMS": 1000,
    }


def test_mongo_options_leave_unset_bounds_to_the_driver() -> None:
    options = MongoConnection(url="mongodb://h", database="app").client_options()

    assert options == {"host": "mongodb://h"}


# -------------------------------------------------------------------- ClickHouse


def test_clickhouse_options_send_read_timeout_as_send_receive_timeout() -> None:
    connection = ClickHouseConnection(
        url="clickhouse://h:8123/db",
        username="u",
        password="p",
        connect_timeout=3,
        read_timeout=40,
        executor_threads=2,
        pool=ConnectionPool(size=5),
    )

    # The pool is a driver object and the thread pool an asynchronous option:
    # both are the island's.
    assert connection.client_options() == {
        "dsn": "clickhouse://h:8123/db",
        "username": "u",
        "password": "p",
        "connect_timeout": 3,
        "send_receive_timeout": 40,
    }


def test_clickhouse_options_leave_unset_bounds_to_the_driver() -> None:
    assert ClickHouseConnection(url="clickhouse://h/db").client_options() == {
        "dsn": "clickhouse://h/db"
    }


# -------------------------------------------------------------------- Relational


def test_asyncpg_takes_the_timeouts_as_connect_arguments() -> None:
    config = DatabaseConfig(url="postgresql+asyncpg://h/db", connect_timeout=3, read_timeout=30)

    assert config.client_options() == {"timeout": 3, "command_timeout": 30}


def test_a_database_without_timeouts_adds_no_connect_argument() -> None:
    assert DatabaseConfig(url="sqlite+aiosqlite:///:memory:").client_options() == {}


def test_the_options_are_the_complete_connect_arguments() -> None:
    config = DatabaseConfig(
        url="postgresql+asyncpg://h/db", connect_timeout=3, connect_args={"ssl": "require"}
    )

    assert config.client_options() == {"ssl": "require", "timeout": 3}


@pytest.mark.parametrize(
    ("url", "field"),
    [
        ("sqlite+aiosqlite:///:memory:", "connect_timeout"),
        ("postgresql+psycopg://h/db", "read_timeout"),
    ],
)
def test_a_timeout_for_another_driver_points_to_connect_args(url: str, field: str) -> None:
    timeout: dict[str, Any] = {field: 3}
    with pytest.raises(ValueError, match=rf"{field} applies to postgresql\+asyncpg URLs only"):
        DatabaseConfig(url=url, **timeout)


def test_a_timeout_also_set_in_connect_args_is_refused() -> None:
    with pytest.raises(ValueError, match="connect_args sets command_timeout"):
        DatabaseConfig(
            url="postgresql+asyncpg://h/db", read_timeout=30, connect_args={"command_timeout": 10}
        )


def test_a_decoded_section_names_its_path_when_a_timeout_does_not_apply() -> None:
    section = {"url": "sqlite+aiosqlite:///:memory:", "connect_timeout": 3}

    with pytest.raises(ConfigError, match=r"'database\.main'.*connect_timeout applies"):
        decode_database_config(section, path="database.main")


def test_the_database_repr_hides_the_url_credentials() -> None:
    config = DatabaseConfig(url="postgresql+asyncpg://app:s3cr3t@h/db")

    shown = repr(config) + repr([config])

    assert "s3cr3t" not in shown
    assert "postgresql+asyncpg://***@h/db" in shown


def test_the_database_repr_hides_a_password_inside_connect_args() -> None:
    config = DatabaseConfig(
        url="postgresql+asyncpg://h/db", connect_args={"password": "s3cr3t", "ssl": "require"}
    )

    shown = repr(config) + repr([config])

    assert "s3cr3t" not in shown
    assert "connect_args='***'" in shown
