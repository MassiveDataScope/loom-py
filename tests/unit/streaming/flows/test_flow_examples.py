"""Bytewax execution tests for reusable public DSL flow examples."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
from omegaconf import DictConfig

from tests.helpers.version_limited_extras import require_bytewax

require_bytewax()

from loom.core.model import LoomStruct  # noqa: E402
from loom.streaming import (  # noqa: E402
    BatchStep,
    ErrorEnvelope,
    ErrorKind,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    RecordStep,
    Route,
    Router,
    StreamFlow,
    WithAsync,
    msg,
)
from loom.streaming.testing import StreamingTestRunner  # noqa: E402
from tests.unit.streaming.doubles import AcmeAmountOrder  # noqa: E402
from tests.unit.streaming.flows.cases import StreamFlowCase  # noqa: E402

pytestmark = pytest.mark.integration


class TestBytewaxFlowExamples:
    """Bytewax execution coverage for shared public DSL flow examples."""

    def test_runs_flow_examples(
        self,
        flow_case: StreamFlowCase,
    ) -> None:
        results = _run_flow_case(flow_case)

        assert tuple(message.payload for message in results) == flow_case.expected_payloads
        if flow_case.resource_events is not None:
            _RESOURCE_EVENT_ASSERTIONS[flow_case.flow.name](flow_case)

    def test_routes_record_step_errors_to_task_error_sink(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Any, Any] = StreamFlow(
            name="orders_fail",
            source=FromTopic("orders.raw", payload=AcmeAmountOrder),
            process=Process(
                _BoomStep(), IntoTopic("orders.validated", payload=AcmeFlowValidatedOrder)
            ),
            errors={ErrorKind.TASK: IntoTopic("orders.task.errors")},
        )
        runner = (
            StreamingTestRunner.from_flow(
                flow,
                config=streaming_kafka_config,
            )
            .with_messages([_message(AcmeAmountOrder(order_id="o-1", amount=10))])
            .capture_errors(ErrorKind.TASK)
        )

        runner.run()

        assert runner.output == []
        errors = runner.errors[ErrorKind.TASK]
        assert len(errors) == 1
        envelope = errors[0]
        assert isinstance(envelope, ErrorEnvelope)
        assert envelope.kind is ErrorKind.TASK
        assert envelope.reason == "ValueError"
        assert envelope.original_message is not None
        assert envelope.original_message.payload == AcmeAmountOrder(order_id="o-1", amount=10)

    def test_with_async_task_timeout_captures_timed_out_messages_as_errors(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """task_timeout_ms cancels slow inner-process tasks and routes them to error envelopes."""
        flow: StreamFlow[AcmeAmountOrder, AcmeFlowValidatedOrder] = StreamFlow(
            name="timeout_flow",
            source=FromTopic("orders.raw", payload=AcmeAmountOrder),
            process=Process(
                WithAsync(
                    process=Process(
                        _SlowAsyncStep(),
                        IntoTopic("orders.validated", payload=AcmeFlowValidatedOrder),
                    ),
                    task_timeout_ms=50,
                ),
            ),
        )
        runner = StreamingTestRunner.from_flow(flow, config=streaming_kafka_config)
        runner.capture_errors(ErrorKind.TASK)
        msg = _message(AcmeAmountOrder(order_id="ORD-1", amount=1))

        runner.with_messages([msg]).run()

        assert len(runner.output) == 0
        assert len(runner.errors[ErrorKind.TASK]) == 1
        envelope = runner.errors[ErrorKind.TASK][0]
        assert isinstance(envelope, ErrorEnvelope)
        assert envelope.kind == ErrorKind.TASK

    def test_router_executes_batch_step_branch_as_singleton_batch(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        """BatchStep inside a Router branch must execute as a singleton batch."""
        flow: StreamFlow[AcmeAmountOrder, AcmeFlowValidatedOrder] = StreamFlow(
            name="router_batch_flow",
            source=FromTopic("orders.raw", payload=AcmeAmountOrder),
            process=Process(
                Router.when(
                    routes=[
                        Route(
                            when=msg.payload.order_id != "",
                            process=Process(UppercasingFlowBatchStep()),
                        )
                    ],
                )
            ),
            output=IntoTopic("orders.out", payload=AcmeFlowValidatedOrder),
        )
        runner = StreamingTestRunner.from_flow(flow, config=streaming_kafka_config)
        runner.with_messages([_message(AcmeAmountOrder(order_id="ord-1", amount=10))]).run()

        assert len(runner.output) == 1
        assert runner.output[0].payload == AcmeFlowValidatedOrder(order_id="ORD-1")


def _run_flow_case(flow_case: StreamFlowCase) -> list[Message[Any]]:
    runner = StreamingTestRunner.from_flow(
        flow_case.flow,
        config=flow_case.config,
    ).with_messages(list(flow_case.input_messages))
    runner.run()
    return runner.output


class AcmeFlowValidatedOrder(LoomStruct):
    order_id: str


class _BoomStep(RecordStep[AcmeAmountOrder, AcmeFlowValidatedOrder]):
    def execute(
        self, message: Message[AcmeAmountOrder], **kwargs: object
    ) -> AcmeFlowValidatedOrder:
        del message, kwargs
        raise ValueError("boom")


def _message(payload: AcmeAmountOrder) -> Message[AcmeAmountOrder]:
    return Message(payload=payload, meta=MessageMeta(message_id=payload.order_id))


class _SlowAsyncStep(RecordStep[AcmeAmountOrder, AcmeFlowValidatedOrder]):
    async def execute(
        self, message: Message[AcmeAmountOrder], **kwargs: object
    ) -> AcmeFlowValidatedOrder:
        await asyncio.sleep(10)
        return AcmeFlowValidatedOrder(order_id=message.payload.order_id)


# ---------------------------------------------------------------------------
# Router + BatchStep — Router must execute BatchStep as singleton batch
# ---------------------------------------------------------------------------


class UppercasingFlowBatchStep(BatchStep[AcmeAmountOrder, AcmeFlowValidatedOrder]):
    def execute(
        self, messages: list[Message[AcmeAmountOrder]], **kwargs: object
    ) -> list[AcmeFlowValidatedOrder]:
        return [AcmeFlowValidatedOrder(order_id=m.payload.order_id.upper()) for m in messages]


def _assert_orders_price_batch(case: StreamFlowCase) -> None:
    assert case.resource_events is not None
    opened, closed = case.resource_events.snapshot()
    assert opened == (1,)
    assert closed == (1,)


def _assert_orders_price_batch_scope(case: StreamFlowCase) -> None:
    assert case.resource_events is not None
    opened, closed = case.resource_events.snapshot()
    assert opened == (1, 2)
    assert closed == (1, 2)


def _assert_orders_fork_with(case: StreamFlowCase) -> None:
    assert case.resource_events is not None
    opened, closed = case.resource_events.snapshot()
    assert opened == (1,)
    assert closed == (1,)


_RESOURCE_EVENT_ASSERTIONS: dict[str, Callable[[StreamFlowCase], None]] = {
    "orders_price_batch": _assert_orders_price_batch,
    "orders_price_batch_scope": _assert_orders_price_batch_scope,
    "orders_fork_with": _assert_orders_fork_with,
}
