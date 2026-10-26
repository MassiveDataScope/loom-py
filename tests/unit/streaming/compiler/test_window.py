"""Window strategy compiler validation."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig

from loom.streaming import (
    CollectBatch,
    Drain,
    ForEach,
    FromTopic,
    Process,
    StreamFlow,
    WindowStrategy,
)
from loom.streaming.compiler import CompilationError, compile_flow
from tests.unit.streaming.compiler.cases import Order, Result

pytestmark = pytest.mark.integration


class TestWindowStrategyValidation:
    """Compiler must reject unimplemented WindowStrategy values at compile time."""

    @pytest.mark.parametrize(
        ("strategy", "error_fragment"),
        [
            (WindowStrategy.COLLECT, None),
            (WindowStrategy.TUMBLING, "tumbling"),
            (WindowStrategy.SESSION, "session"),
        ],
    )
    def test_window_strategy_validation(
        self,
        streaming_kafka_config: DictConfig,
        strategy: WindowStrategy,
        error_fragment: str | None,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                CollectBatch(max_records=10, timeout_ms=500, window=strategy),
                ForEach(),
                Drain(),
            ),
        )

        if error_fragment is None:
            plan = compile_flow(flow, config=streaming_kafka_config)
            batch_node = plan.nodes[0].node
            assert isinstance(batch_node, CollectBatch)
            assert batch_node.window is WindowStrategy.COLLECT
            return

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        assert error_fragment in str(exc_info.value)
