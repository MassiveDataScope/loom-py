"""Async bridge and process walker compiler validation."""

from __future__ import annotations

from typing import Any, cast

from omegaconf import DictConfig, OmegaConf

from loom.core.config import ConfigContext
from loom.streaming import (
    Backend,
    Fork,
    ForkRoute,
    FromTopic,
    IntoTable,
    IntoTopic,
    Process,
    Route,
    Router,
    SqlAlchemyDatabaseConfig,
    SqlAlchemySinkConfig,
    StreamFlow,
    WithAsync,
    msg,
)
from loom.streaming.compiler import compile_flow
from tests.unit.streaming.compiler.cases import Order, Result


def _flow_with_sqlalchemy_into_table_inside_router() -> StreamFlow[Order, Result]:
    into_table: Any = IntoTable(
        payload=Result,
        table="results",
        backend=Backend.SQLALCHEMY,
        name="results_sink",
    )
    return StreamFlow(
        name="test_async_in_router",
        source=FromTopic("in", payload=Order),
        process=Process(
            Router.when(
                routes=[Route(when=msg.payload.order_id != "", process=Process(into_table))],
            )
        ),
    )


def _with_results_sink(config: DictConfig) -> ConfigContext:
    merged = OmegaConf.merge(
        config,
        OmegaConf.create({"streaming": {"sinks": {"results_sink": {"url": "sqlite:///:memory:"}}}}),
    )
    return ConfigContext(cast(DictConfig, merged))


def _flow_with_sqlalchemy_into_table() -> StreamFlow[Order, Result]:
    into_table = IntoTable(
        payload=Result,
        table="results",
        backend=Backend.SQLALCHEMY,
        name="results_sink",
    )
    return StreamFlow(
        name="test_async_in_into_table",
        source=FromTopic("in", payload=Order),
        process=Process(into_table),
    )


class TestAsyncBridgeDetection:
    def test_needs_async_bridge_is_true_when_sqlalchemy_into_table_is_inside_router(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow = _flow_with_sqlalchemy_into_table_inside_router()
        plan = compile_flow(flow, config=_with_results_sink(streaming_kafka_config))

        assert plan.needs_async_bridge is True

    def test_needs_async_bridge_is_false_when_no_with_async_present(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(IntoTopic("out", payload=Result)),
        )
        plan = compile_flow(flow, config=ConfigContext(streaming_kafka_config))

        assert plan.needs_async_bridge is False

    def test_needs_async_bridge_is_true_when_with_async_is_inside_fork(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test_async_in_fork",
            source=FromTopic("in", payload=Order),
            process=Process(
                Fork.when(
                    routes=[
                        ForkRoute(
                            when=msg.payload.order_id != "",
                            process=Process(
                                WithAsync(
                                    process=Process(IntoTopic("out", payload=Result)),
                                ),
                            ),
                        )
                    ],
                )
            ),
        )
        plan = compile_flow(flow, config=ConfigContext(streaming_kafka_config))

        assert plan.needs_async_bridge is True

    def test_needs_async_bridge_is_true_when_sqlalchemy_into_table_is_present(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        config = OmegaConf.merge(
            streaming_kafka_config,
            OmegaConf.create(
                {
                    "streaming": {
                        "sinks": {
                            "results_sink": {
                                "url": "sqlite:///:memory:",
                            }
                        }
                    }
                }
            ),
        )
        flow = _flow_with_sqlalchemy_into_table()
        plan = compile_flow(flow, config=ConfigContext(cast(DictConfig, config)))

        assert plan.needs_async_bridge is True

    def test_sqlalchemy_into_table_resolves_shared_database_config(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        config = OmegaConf.merge(
            streaming_kafka_config,
            OmegaConf.create(
                {
                    "database": {
                        "warehouse": {
                            "url": "sqlite+aiosqlite:///:memory:",
                            "pool_pre_ping": True,
                        }
                    },
                    "streaming": {
                        "sinks": {
                            "results_sink": {
                                "database": "warehouse",
                                "table": "results",
                                "chunk_size": 250,
                            }
                        }
                    },
                }
            ),
        )
        flow = _flow_with_sqlalchemy_into_table()
        plan = compile_flow(flow, config=ConfigContext(cast(DictConfig, config)))

        compiled = plan.terminal_storage_sinks[(0,)]
        expected_sink = SqlAlchemySinkConfig.from_config(
            {
                "database": "warehouse",
                "table": "results",
                "chunk_size": 250,
            },
            default_table="results",
        )
        expected_db = SqlAlchemyDatabaseConfig.from_config(
            {
                "url": "sqlite+aiosqlite:///:memory:",
                "pool_pre_ping": True,
            }
        )
        assert compiled.config == expected_sink
        assert compiled.database_config == expected_db
