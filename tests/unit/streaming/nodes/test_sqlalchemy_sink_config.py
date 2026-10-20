"""Which keys a SQLAlchemy table sink section reads."""

from __future__ import annotations

import pytest

from loom.streaming.nodes._table import SqlAlchemySinkConfig


def test_an_inline_url_sink_accepts_the_pool_keys() -> None:
    config = SqlAlchemySinkConfig.from_config(
        {"url": "sqlite+aiosqlite:///:memory:", "pool_size": 5}, default_table="orders"
    )

    assert config.url == "sqlite+aiosqlite:///:memory:"


def test_a_shared_database_sink_refuses_the_pool_keys_it_does_not_read() -> None:
    with pytest.raises(ValueError, match="unknown key 'pool_size'"):
        SqlAlchemySinkConfig.from_config(
            {"database": "warehouse", "pool_size": 5}, default_table="orders"
        )
