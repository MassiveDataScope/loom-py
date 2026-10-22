"""Which keys a SQLAlchemy table sink section reads."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.model import LoomStruct
from loom.core.persistence.database import PoolConfig
from loom.streaming.nodes._table import IntoTable, SqlAlchemySinkConfig
from loom.streaming.nodes._table.config import (
    ResolvedSqlAlchemyTableConfig,
    resolve_sqlalchemy_table_config,
)


class _Row(LoomStruct):
    id: int


def test_an_inline_url_sink_accepts_the_database_keys() -> None:
    config = SqlAlchemySinkConfig.from_config(
        {"url": "sqlite+aiosqlite:///:memory:", "pool": {"size": 5}, "echo": False},
        default_table="orders",
    )

    assert config.url == "sqlite+aiosqlite:///:memory:"


def test_an_inline_url_sink_accepts_the_standard_timeouts() -> None:
    resolved = _resolve(
        {
            "streaming": {
                "sinks": {
                    "orders_sink": {
                        "url": "postgresql+asyncpg://h/db",
                        "connect_timeout": 3,
                        "read_timeout": 30,
                    }
                }
            }
        }
    )

    assert resolved.database.client_options() == {"timeout": 3, "command_timeout": 30}


def test_an_inline_url_sink_refuses_the_schema_it_does_not_read() -> None:
    with pytest.raises(ValueError, match="unknown key 'schema'"):
        SqlAlchemySinkConfig.from_config(
            {"url": "sqlite+aiosqlite:///", "schema": {}}, default_table="orders"
        )


def test_a_shared_database_sink_refuses_the_pool_it_does_not_read() -> None:
    with pytest.raises(ValueError, match="unknown key 'pool'"):
        SqlAlchemySinkConfig.from_config(
            {"database": "warehouse", "pool": {"size": 5}}, default_table="orders"
        )


def _resolve(config: dict[str, object]) -> ResolvedSqlAlchemyTableConfig:
    node = IntoTable(payload=_Row, table="orders", name="orders_sink")
    return resolve_sqlalchemy_table_config(node, ConfigContext.from_dict(config))


def test_an_inline_sink_reads_its_pool_through_the_database_contract() -> None:
    resolved = _resolve(
        {
            "streaming": {
                "sinks": {"orders_sink": {"url": "sqlite+aiosqlite:///", "pool": {"size": 3}}}
            }
        }
    )

    assert resolved.database.pool == PoolConfig(size=3)


def test_a_shared_database_reads_its_pool_through_the_database_contract() -> None:
    resolved = _resolve(
        {
            "database": {"warehouse": {"url": "sqlite+aiosqlite:///", "pool": {"size": 4}}},
            "streaming": {"sinks": {"orders_sink": {"database": "warehouse"}}},
        }
    )

    assert resolved.database.pool == PoolConfig(size=4)


def test_a_retired_flat_key_inline_names_its_replacement() -> None:
    with pytest.raises(ConfigError, match=r"streaming\.sinks\.orders_sink\.pool\.size"):
        _resolve(
            {
                "streaming": {
                    "sinks": {"orders_sink": {"url": "sqlite+aiosqlite:///", "pool_size": 3}}
                }
            }
        )


def test_a_retired_flat_key_in_a_shared_database_names_its_replacement() -> None:
    with pytest.raises(ConfigError, match=r"database\.warehouse\.pool\.size"):
        _resolve(
            {
                "database": {"warehouse": {"url": "sqlite+aiosqlite:///", "pool_size": 4}},
                "streaming": {"sinks": {"orders_sink": {"database": "warehouse"}}},
            }
        )
