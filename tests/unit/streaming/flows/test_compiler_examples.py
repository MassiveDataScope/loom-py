"""Compiler tests for reusable public DSL flow examples."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.streaming import Explode, Router, StreamShape, compile_flow
from loom.streaming.compiler import CompiledPlan
from tests.unit.streaming.flows.cases import StreamFlowCase

pytestmark = pytest.mark.integration


class TestCompilerFlowExamples:
    """Compiler coverage for shared public DSL flow examples."""

    def test_compile_flow_examples(
        self,
        flow_case: StreamFlowCase,
    ) -> None:
        plan = compile_flow(flow_case.flow, config=flow_case.config)

        _ASSERTIONS[flow_case.flow.name](plan)


def _assert_simple_validation_case(plan: CompiledPlan) -> None:
    assert plan.name == "orders_validate"
    assert plan.source.payload_type is not None
    assert plan.source.decode_strategy == "record"
    assert plan.output is not None
    assert plan.output.topic == "orders.validated"
    assert plan.nodes[0].input_shape is StreamShape.RECORD
    assert plan.nodes[0].output_shape is StreamShape.RECORD


def _assert_router_case(plan: CompiledPlan) -> None:
    assert plan.name == "orders_route"
    assert plan.output is not None
    assert plan.output.topic == "orders.routed"
    assert plan.nodes[0].input_shape is StreamShape.RECORD
    assert plan.nodes[0].output_shape is StreamShape.RECORD


def _assert_batch_case(plan: CompiledPlan) -> None:
    assert plan.source.decode_strategy == "batch"
    assert plan.output is None
    assert [node.output_shape for node in plan.nodes] == [
        StreamShape.BATCH,
        StreamShape.NONE,
    ]


def _assert_async_case(plan: CompiledPlan) -> None:
    assert plan.source.decode_strategy == "record"
    assert plan.output is None
    assert plan.needs_async_bridge is True
    assert [node.output_shape for node in plan.nodes] == [StreamShape.NONE]


def _assert_explode_case(plan: CompiledPlan) -> None:
    assert plan.output is not None
    assert plan.output.topic == "events.analytics"
    assert len(plan.nodes) == 2
    assert isinstance(plan.nodes[0].node, Explode)
    assert isinstance(plan.nodes[1].node, Router)
    assert plan.nodes[0].input_shape is StreamShape.RECORD
    assert plan.nodes[0].output_shape is StreamShape.RECORD
    assert plan.terminal_sinks == {}


def _assert_fork_case(plan: CompiledPlan) -> None:
    assert plan.output is None
    assert plan.nodes[0].output_shape is StreamShape.NONE
    assert {sink.topic for sink in plan.terminal_sinks.values()} == {
        "orders.fork.vip",
        "orders.fork.standard",
    }


def _assert_fork_with_case(plan: CompiledPlan) -> None:
    assert plan.output is None
    assert {sink.topic for sink in plan.terminal_sinks.values()} == {"orders.fork.vip"}


def _assert_fork_when_case(plan: CompiledPlan) -> None:
    assert plan.output is None
    assert {sink.topic for sink in plan.terminal_sinks.values()} == {
        "orders.fork.vip",
        "orders.fork.standard",
    }


_ASSERTIONS: dict[str, Callable[[CompiledPlan], None]] = {
    "orders_validate": _assert_simple_validation_case,
    "orders_route": _assert_router_case,
    "orders_price_batch": _assert_batch_case,
    "orders_price_batch_scope": _assert_batch_case,
    "orders_score_async_each": _assert_async_case,
    "stores_explode": _assert_explode_case,
    "orders_fork": _assert_fork_case,
    "orders_fork_with": _assert_fork_with_case,
    "orders_fork_when": _assert_fork_when_case,
}
