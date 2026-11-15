"""Behavioral tests for Bytewax collect batching."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.streaming import (  # noqa: E402
    CollectBatch,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    ResourceScope,
    StreamFlow,
    With,
)
from loom.streaming.testing import StreamingTestRunner  # noqa: E402
from tests.unit.streaming.bytewax.cases import AcmeOrder, AcmeResult, build_message  # noqa: E402

pytestmark = pytest.mark.bytewax


class TestCollectBatch:
    def test_collect_batch_emits_after_timeout_even_without_reaching_max_records(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[AcmeOrder, AcmeResult] = StreamFlow(
            name="collect_timeout_flow",
            source=FromTopic("orders.in", payload=AcmeOrder),
            process=Process(
                CollectBatch(max_records=2, timeout_ms=1),
                With(
                    process=Process(_EchoRecordStep(), IntoTopic("orders.out", payload=AcmeResult)),
                    client=object(),
                    scope=ResourceScope.WORKER,
                ),
            ),
        )
        runner = StreamingTestRunner.from_flow(
            flow,
            config=streaming_kafka_config,
        ).with_messages([build_message(AcmeOrder(order_id="o-1"))])

        runner.run()

        assert [message.payload.value for message in runner.output] == ["o-1"]
        assert len(runner.output) == 1


class _EchoRecordStep(RecordStep[AcmeOrder, AcmeResult]):
    def execute(self, message: Message[AcmeOrder], **kwargs: object) -> AcmeResult:
        del kwargs
        return AcmeResult(value=message.payload.order_id)
