"""Router branch compiler validation."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.streaming import (
    CollectBatch,
    Drain,
    ExpandStep,
    Explode,
    ForEach,
    FromTopic,
    IntoTopic,
    Message,
    PayloadExpander,
    Process,
    Route,
    Router,
    StreamFlow,
    StreamShape,
    With,
    WithAsync,
    msg,
)
from loom.streaming.compiler import CompilationError, StreamingErrorCode, compile_flow
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result


class TestRouterCompiler:
    def test_validates_router_branch_shapes(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router.when(
                    (
                        Route(
                            when=msg.payload.order_id == "batch",
                            process=Process(CollectBatch(max_records=10, timeout_ms=1000)),
                        ),
                    ),
                    default=Process(FakeStep()),
                ),
                IntoTopic("out", payload=Result),
            ),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        assert "messages continuing after the router have different shapes" in str(exc_info.value)

    def test_accepts_router_with_terminal_branch_output(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router.by(
                    msg.payload.order_id,
                    routes={"vip": Process(FakeStep(), IntoTopic("out", payload=Result))},
                )
            ),
            output=None,
        )

        plan = compile_flow(flow, config=streaming_kafka_config)

        assert plan.nodes[0].output_shape is StreamShape.RECORD
        assert plan.output is None

    def test_accepts_branches_that_end_their_path_in_different_shapes(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router.by(
                    msg.payload.order_id,
                    routes={"vip": Process(FakeStep(), IntoTopic("out", payload=Result))},
                    default=Process(Drain()),
                )
            ),
        )

        plan = compile_flow(flow, config=streaming_kafka_config)

        assert len(plan.terminal_sinks) == 1, "only the IntoTopic branch owns a sink"

    def test_unmatched_messages_keep_their_shape_after_terminal_branches(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router.by(msg.payload.order_id, routes={"drop": Process(Drain())}),
                FakeStep(),
            ),
            output=IntoTopic("out", payload=Result),
        )

        plan = compile_flow(flow, config=streaming_kafka_config)

        assert plan.output is not None

    @pytest.mark.parametrize(
        "router",
        [
            Router.by(msg.payload.order_id, routes={"drop": Process(Drain())}),
            Router.when([Route(when=msg.payload.order_id != "", process=Process(FakeStep()))]),
        ],
        ids=["path_ending_branch", "continuing_branch"],
    )
    def test_rejects_a_batch_before_a_router_with_one_issue(
        self,
        router: Router[Order, Any],
        streaming_kafka_config: DictConfig,
    ) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(CollectBatch(max_records=10, timeout_ms=1000), router),
            output=IntoTopic("out", payload=Result),
        )

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=streaming_kafka_config)

        assert [issue.message for issue in exc_info.value.issues] == [
            "shape mismatch: expected record but got batch before Router"
        ]

    def test_mixed_keyed_and_predicate_router_sinks_have_unique_paths(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        # orders.a and orders.b are registered in the shared config fixture
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router(
                    selector=msg.payload.order_id,
                    routes={"vip": Process(FakeStep(), IntoTopic("orders.a", payload=Result))},
                    predicate_routes=[
                        Route(
                            when=msg.payload.order_id != "",
                            process=Process(FakeStep(), IntoTopic("orders.b", payload=Result)),
                        )
                    ],
                )
            ),
        )

        plan = compile_flow(flow, config=streaming_kafka_config)

        # Each branch must occupy a distinct path key — keyed branch at index 0,
        # predicate branch offset by len(keyed_routes) = 1, not restarting from 0.
        sink_paths = list(plan.terminal_sinks.keys())
        assert len(sink_paths) == 2, "one sink per branch expected"
        assert len(sink_paths) == len(set(sink_paths)), "terminal sink path keys must be unique"
        topics = {s.topic for s in plan.terminal_sinks.values()}
        assert topics == {"orders.a", "orders.b"}

    def test_rejects_expand_step_inside_router_branch(self) -> None:
        class _ExpandFakeStep(ExpandStep[Order, Result]):
            def execute(self, message: Message[Order], **kwargs: object) -> list[Message[Result]]:
                del kwargs
                return []

        flow: StreamFlow[Order, Result] = StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Router.when(
                    routes=[
                        Route(
                            when=msg.payload.order_id != "",
                            process=Process(
                                _ExpandFakeStep(),
                                IntoTopic("out", payload=Result),
                            ),
                        )
                    ],
                )
            ),
        )

        config = OmegaConf.create({})
        with pytest.raises(CompilationError, match="not supported in Router branches"):
            compile_flow(flow, config=config)


class _OrderRows(PayloadExpander[Order]):
    """Expander for the Explode rejection case."""

    @classmethod
    def expand(cls, event: Order) -> dict[type, list[Result]]:
        return {Result: [Result(value=event.order_id)]}


@pytest.mark.parametrize(
    "node",
    [
        With(process=Process(FakeStep())),
        WithAsync(process=Process(FakeStep())),
        Router.by(msg.payload.order_id, routes={"vip": Process(FakeStep())}),
        CollectBatch(max_records=2, timeout_ms=10),
        ForEach(),
        Explode(_OrderRows),
    ],
    ids=lambda node: type(node).__name__,
)
def test_a_router_branch_rejects_a_node_it_cannot_run_inline(
    node: Any, streaming_kafka_config: DictConfig
) -> None:
    """A Router runs its branches inside one map; these nodes need their own operators."""
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(
            Router.when(
                routes=[Route(when=msg.payload.order_id != "", process=Process(node))],
            ),
            IntoTopic("out", payload=Result),
        ),
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, config=streaming_kafka_config)

    unsafe = [
        issue.message
        for issue in exc_info.value.issues
        if issue.code is StreamingErrorCode.ROUTER_BRANCH_UNSAFE_NODE
    ]
    assert unsafe == [
        f"router branch predicate[0]: node {type(node).__name__} is not router-branch safe"
    ]
