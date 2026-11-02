"""Table sinks name a store of the catalog: ``clickhouse`` and ``sqlalchemy`` side by side."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from omegaconf import DictConfig, OmegaConf

from loom.core.config import ConfigContext, ConfigError
from loom.core.config.overrides import load_application_config
from loom.core.environment import Environment
from loom.core.persistence.database import DatabaseConfig
from loom.core.persistence.stores import load_catalog
from loom.streaming import Backend, FromTopic, IntoTable, Process, Router, StreamFlow, msg
from loom.streaming.compiler import CompilationError, StreamingErrorCode, compile_flow
from loom.streaming.nodes._table import ClickHouseSinkConfig
from loom.streaming.nodes._table.config import (
    resolve_clickhouse_table_config,
    resolve_sqlalchemy_table_config,
)
from tests.helpers.persistence import record_catalogs

from .cases import Order

_CLICKHOUSE_STORE = {"backend": "clickhouse", "url": "clickhouse://ch:8123/analytics"}
_SQLITE_STORE = {"backend": "sqlalchemy", "url": "sqlite+aiosqlite:///:memory:"}


def _context(stores: dict[str, Any], sink: dict[str, Any]) -> ConfigContext:
    return ConfigContext.from_dict({"stores": stores, "streaming": {"sinks": {"s": sink}}})


def _node(backend: Backend) -> IntoTable[Order]:
    return IntoTable(payload=Order, table="orders", backend=backend, name="s")


def test_a_clickhouse_sink_takes_its_connection_from_the_store_it_names() -> None:
    ctx = _context({"events": _CLICKHOUSE_STORE}, {"store": "events", "insert_timeout": 3})

    sink = resolve_clickhouse_table_config(_node(Backend.CLICKHOUSE), ctx, load_catalog(ctx)).sink

    assert isinstance(sink, ClickHouseSinkConfig)
    assert str(sink.connection.url) == "clickhouse://ch:8123/analytics"
    assert (sink.table, sink.insert_timeout) == ("orders", 3.0)


def test_a_clickhouse_sink_refuses_a_sqlalchemy_store() -> None:
    ctx = _context({"warehouse": _SQLITE_STORE}, {"store": "warehouse"})

    node = _node(Backend.CLICKHOUSE)
    catalog = load_catalog(ctx)
    with pytest.raises(ConfigError, match=r"stores\.warehouse\.backend is 'sqlalchemy'"):
        resolve_clickhouse_table_config(node, ctx, catalog)


def test_a_sqlalchemy_sink_refuses_a_clickhouse_store() -> None:
    ctx = _context({"events": _CLICKHOUSE_STORE}, {"store": "events"})

    node = _node(Backend.SQLALCHEMY)
    catalog = load_catalog(ctx)
    with pytest.raises(ConfigError, match=r"stores\.events\.backend is 'clickhouse'"):
        resolve_sqlalchemy_table_config(node, ctx, catalog)


def test_a_clickhouse_sink_names_the_renamed_database_key() -> None:
    ctx = _context({"events": _CLICKHOUSE_STORE}, {"database": "events"})

    node = _node(Backend.CLICKHOUSE)
    catalog = load_catalog(ctx)
    with pytest.raises(ValueError, match="'database' was renamed to 'store'"):
        resolve_clickhouse_table_config(node, ctx, catalog)


def test_a_flow_with_a_clickhouse_and_a_sqlalchemy_sink_compiles_from_a_file(
    tmp_path: Path, streaming_kafka_config: DictConfig
) -> None:
    document = cast(dict[str, Any], OmegaConf.to_container(streaming_kafka_config))
    document["stores"] = {
        "events": _CLICKHOUSE_STORE,
        "warehouse": {**_SQLITE_STORE, "pool": {"pre_ping": True}},
    }
    document["streaming"] = {
        **document.get("streaming", {}),
        "sinks": {
            "clicks": {"store": "events", "table": "clicks"},
            "orders": {"store": "warehouse", "table": "orders", "chunk_size": 50},
        },
    }
    path = tmp_path / "base.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    flow = StreamFlow(
        name="mixed_sinks",
        source=FromTopic("in", payload=Order),
        process=Process(
            Router.by(
                msg.payload.order_id,
                routes={
                    "a": Process(
                        cast(
                            Any, IntoTable(payload=Order, backend=Backend.CLICKHOUSE, name="clicks")
                        )
                    )
                },
                default=Process(
                    cast(Any, IntoTable(payload=Order, backend=Backend.SQLALCHEMY, name="orders"))
                ),
            )
        ),
    )

    plan = compile_flow(
        flow, config=load_application_config(str(path), environment=Environment.TEST)
    )

    sinks = list(plan.terminal_storage_sinks.values())
    assert {type(sink.config).__name__ for sink in sinks} == {
        "ClickHouseSinkConfig",
        "SqlAlchemySinkConfig",
    }
    databases = [sink.database_config for sink in sinks if sink.database_config is not None]
    assert [type(database) for database in databases] == [DatabaseConfig]
    assert databases[0].pool.pre_ping is True


def test_a_flow_with_three_sinks_over_two_stores_loads_the_catalog_once(
    streaming_kafka_config: DictConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    document = cast(dict[str, Any], OmegaConf.to_container(streaming_kafka_config))
    document["stores"] = {"events": _CLICKHOUSE_STORE, "warehouse": _SQLITE_STORE}
    document["streaming"] = {
        **document.get("streaming", {}),
        "sinks": {
            "clicks": {"store": "events", "table": "clicks"},
            "orders": {"store": "warehouse", "table": "orders"},
            "refunds": {"store": "warehouse", "table": "refunds"},
        },
    }

    def sink(backend: Backend, name: str) -> Process[Order, Any]:
        return Process(cast(Any, IntoTable(payload=Order, backend=backend, name=name)))

    flow = StreamFlow(
        name="three_sinks",
        source=FromTopic("in", payload=Order),
        process=Process(
            Router.by(
                msg.payload.order_id,
                routes={
                    "a": sink(Backend.CLICKHOUSE, "clicks"),
                    "b": sink(Backend.SQLALCHEMY, "refunds"),
                },
                default=sink(Backend.SQLALCHEMY, "orders"),
            )
        ),
    )
    catalogs = record_catalogs(monkeypatch)

    plan = compile_flow(flow, config=ConfigContext.from_dict(document))

    assert len(plan.terminal_storage_sinks) == 3
    assert len(catalogs) == 1


@pytest.mark.parametrize(
    ("stores", "message"),
    [
        ({}, r"needs stores\.events, which is not declared"),
        ({"events": _SQLITE_STORE}, r"stores\.events\.backend is 'sqlalchemy'"),
    ],
    ids=["undeclared", "wrong-backend"],
)
def test_a_sink_naming_a_store_it_cannot_use_is_a_compile_issue(
    streaming_kafka_config: DictConfig, stores: dict[str, Any], message: str
) -> None:
    document = cast(dict[str, Any], OmegaConf.to_container(streaming_kafka_config))
    document["stores"] = stores
    document["streaming"] = {
        **document.get("streaming", {}),
        "sinks": {"s": {"store": "events"}},
    }
    flow = StreamFlow(
        name="one_sink",
        source=FromTopic("in", payload=Order),
        process=Process(cast(Any, _node(Backend.CLICKHOUSE))),
    )

    config = ConfigContext.from_dict(document)
    with pytest.raises(CompilationError) as caught:
        compile_flow(flow, config=config)

    (issue,) = caught.value.issues
    assert issue.code is StreamingErrorCode.SINK_CONFIG_INVALID
    assert re.search(message, issue.message)
