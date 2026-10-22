"""The single ``database`` section: decoding, validation and pool resolution."""

from __future__ import annotations

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.persistence.database import (
    DatabaseConfig,
    PoolConfig,
    decode_database_config,
    load_database_config,
)
from loom.core.persistence.profile import SERVER_PROFILE, WORKER_PROFILE, PoolDefaults

_URL = "postgresql+asyncpg://app@db/app"


def test_the_contract_section_decodes() -> None:
    config = decode_database_config(
        {
            "url": _URL,
            "echo": False,
            "connect_args": {"command_timeout": 5},
            "pool": {
                "size": 5,
                "max_overflow": 5,
                "timeout": 10,
                "recycle": 1800,
                "pre_ping": True,
            },
            "schema": {"mode": "external"},
        },
        path="database",
    )

    assert config == DatabaseConfig(
        url=_URL,
        echo=False,
        connect_args={"command_timeout": 5},
        pool=PoolConfig(size=5, max_overflow=5, timeout=10, recycle=1800, pre_ping=True),
        schema=config.schema,
    )
    assert config.schema.mode == "external"


def test_an_absent_pool_declares_nothing() -> None:
    config = decode_database_config({"url": _URL}, path="database")

    assert config.pool == PoolConfig()
    assert config.connect_args == {}


def test_an_unknown_pool_key_is_refused_naming_it() -> None:
    with pytest.raises(ConfigError, match="sise"):
        decode_database_config({"url": _URL, "pool": {"sise": 2}}, path="database")


def test_keys_other_readers_own_are_tolerated() -> None:
    config = decode_database_config(
        {"url": _URL, "schema": {"mode": "external", "database_users": {}}, "orders": {}},
        path="database",
    )

    assert config.schema.mode == "external"


@pytest.mark.parametrize(
    ("pool", "named"),
    [
        ({"size": 0}, "size"),
        ({"max_overflow": -1}, "max_overflow"),
        ({"timeout": 0}, "timeout"),
        ({"recycle": 0}, "recycle"),
    ],
)
def test_a_pool_value_out_of_range_is_refused_naming_it(pool: dict[str, int], named: str) -> None:
    with pytest.raises(ConfigError, match=named):
        decode_database_config({"url": _URL, "pool": pool}, path="database")


def test_an_empty_url_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"'database'.*url must not be empty"):
        decode_database_config({"url": "  "}, path="database")


@pytest.mark.parametrize(
    ("retired", "replacement"),
    [
        ("pool_pre_ping", "database.pool.pre_ping"),
        ("pool_size", "database.pool.size"),
        ("max_overflow", "database.pool.max_overflow"),
        ("pool_timeout", "database.pool.timeout"),
        ("pool_recycle", "database.pool.recycle"),
    ],
)
def test_a_retired_flat_key_names_its_replacement(retired: str, replacement: str) -> None:
    with pytest.raises(ConfigError, match=replacement.replace(".", r"\.")):
        decode_database_config({"url": _URL, retired: 1}, path="database")


def test_the_replacement_is_named_under_the_section_it_was_read_from() -> None:
    with pytest.raises(ConfigError, match=r"database\.orders\.pool\.size"):
        decode_database_config({"url": _URL, "pool_size": 3}, path="database.orders")


def test_declared_pool_keys_win_one_by_one_over_the_profile() -> None:
    resolved = PoolConfig(size=1).resolve(SERVER_PROFILE)

    assert resolved == PoolDefaults(
        size=1, max_overflow=5, timeout_s=10, recycle_s=1800, pre_ping=True
    )


def test_an_empty_pool_takes_every_value_from_the_profile() -> None:
    assert PoolConfig().resolve(WORKER_PROFILE) == WORKER_PROFILE.pool


def test_a_fully_declared_pool_ignores_the_profile() -> None:
    pool = PoolConfig(size=3, max_overflow=1, timeout=2, recycle=60, pre_ping=False)

    assert pool.resolve(SERVER_PROFILE) == pool.resolve(WORKER_PROFILE)


def test_load_reads_the_section_from_the_context() -> None:
    ctx = ConfigContext.from_dict({"database": {"url": _URL, "pool": {"size": 4}}})

    assert load_database_config(ctx).pool.size == 4


def test_load_reports_retired_keys_from_the_context_too() -> None:
    ctx = ConfigContext.from_dict({"database": {"url": _URL, "pool_size": 4}})

    with pytest.raises(ConfigError, match=r"database\.pool\.size"):
        load_database_config(ctx)
