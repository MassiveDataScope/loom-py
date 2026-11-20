"""A ClickHouse sink is validated at compile time, like every other backend."""

from __future__ import annotations

from typing import Any, cast

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.core.config import ConfigContext
from loom.streaming import (
    FromTopic,
    IntoTable,
    Process,
    StreamFlow,
    TableEngine,
)
from loom.streaming.compiler import CompilationError, compile_flow

from .cases import Order, Result


def _flow() -> StreamFlow[Order, Result]:
    """A flow whose only destination is a ClickHouse table."""
    sink = IntoTable(
        payload=Result,
        table="results",
        engine=TableEngine.CLICKHOUSE,
        name="results_sink",
    )
    return StreamFlow(
        name="clickhouse_sink_flow",
        source=FromTopic("in", payload=Order),
        process=Process(cast(Any, sink)),
    )


class TestClickHouseSinkValidation:
    def test_compiles_when_the_sinks_database_is_declared(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """With the sink and its store declared, compilation succeeds."""
        config = OmegaConf.merge(
            streaming_kafka_config,
            OmegaConf.create(
                {
                    "stores": {
                        "clickhouse": {
                            "backend": "clickhouse",
                            "url": "clickhouse://localhost:8123/default",
                        }
                    },
                    "streaming": {
                        "sinks": {"results_sink": {"store": "clickhouse", "table": "results"}}
                    },
                }
            ),
        )

        plan = compile_flow(_flow(), config=ConfigContext(cast(DictConfig, config)))

        assert plan.name == "clickhouse_sink_flow"

    def test_compilation_fails_when_the_sinks_database_is_missing(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """A missing ``stores.<name>`` section is a compilation issue, not a runtime crash."""
        config = OmegaConf.merge(
            streaming_kafka_config,
            OmegaConf.create(
                {"streaming": {"sinks": {"results_sink": {"store": "clickhouse", "table": "r"}}}}
            ),
        )

        flow = _flow()
        context = ConfigContext(cast(DictConfig, config))

        with pytest.raises(CompilationError) as failure:
            compile_flow(flow, config=context)

        assert any("results_sink" in issue.message for issue in failure.value.issues)
