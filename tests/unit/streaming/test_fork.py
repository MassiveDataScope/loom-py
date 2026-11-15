"""Tests for terminal Fork branching and branch-index wiring."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import DictConfig

from loom.core.model import LoomStruct
from loom.streaming import Fork, ForkRoute, FromTopic, IntoTopic, Message, Process, StreamFlow
from loom.streaming.compiler import compile_flow
from tests.unit.streaming.doubles import AcmeChannelOrder, AcmeChannelSelector


class _BranchA(LoomStruct):
    order_id: str


class _BranchB(LoomStruct):
    order_id: str


class _BranchDefault(LoomStruct):
    order_id: str


class _HasChannelA:
    def matches(self, message: Message[AcmeChannelOrder]) -> bool:
        return message.payload.channel == "a"


class _HasChannelB:
    def matches(self, message: Message[AcmeChannelOrder]) -> bool:
        return message.payload.channel == "b"


def _fork_when_flow() -> StreamFlow[AcmeChannelOrder, Any]:
    return StreamFlow(
        name="orders_fork_when",
        source=FromTopic("orders.raw", payload=AcmeChannelOrder),
        process=Process(
            Fork.when(
                (
                    ForkRoute(
                        when=_HasChannelA(),
                        process=Process(IntoTopic("orders.a", payload=_BranchA)),
                    ),
                    ForkRoute(
                        when=_HasChannelB(),
                        process=Process(IntoTopic("orders.b", payload=_BranchB)),
                    ),
                ),
                default=Process(IntoTopic("orders.default", payload=_BranchDefault)),
            )
        ),
    )


def _fork_by_flow() -> StreamFlow[AcmeChannelOrder, Any]:
    return StreamFlow(
        name="orders_fork_by",
        source=FromTopic("orders.raw", payload=AcmeChannelOrder),
        process=Process(
            Fork.by(
                selector=AcmeChannelSelector(),
                branches={
                    "a": Process(IntoTopic("orders.a", payload=_BranchA)),
                    "b": Process(IntoTopic("orders.b", payload=_BranchB)),
                },
                default=Process(IntoTopic("orders.default", payload=_BranchDefault)),
            )
        ),
    )


class TestForkDSL:
    def test_fork_by_rejects_empty_branches(self) -> None:
        selector = AcmeChannelSelector()
        with pytest.raises(ValueError, match="Fork.by requires at least one keyed route"):
            Fork.by(selector, {})

    def test_fork_when_rejects_empty_routes(self) -> None:
        with pytest.raises(ValueError, match="Fork.when requires at least one predicate route"):
            Fork.when(())

    def test_fork_by_marks_keyed_kind(self) -> None:
        node = Fork.by(
            AcmeChannelSelector(), {"a": Process(IntoTopic("orders.a", payload=_BranchA))}
        )

        assert node.kind is not None
        assert node.kind.value == "keyed"

    def test_fork_when_marks_predicate_kind(self) -> None:
        node = Fork.when(
            (
                ForkRoute(
                    when=_HasChannelA(),
                    process=Process(IntoTopic("orders.a", payload=_BranchA)),
                ),
            )
        )

        assert node.kind.value == "predicate"


class TestForkCompiler:
    def test_fork_when_compiles_three_terminal_branches(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        plan = compile_flow(_fork_when_flow(), config=streaming_kafka_config)

        topics = {sink.topic for sink in plan.terminal_sinks.values()}
        assert topics == {"orders.a", "orders.b", "orders.default"}

    def test_fork_by_compiles_three_terminal_branches(
        self,
        streaming_kafka_config: DictConfig,
    ) -> None:
        plan = compile_flow(_fork_by_flow(), config=streaming_kafka_config)

        topics = {sink.topic for sink in plan.terminal_sinks.values()}
        assert topics == {"orders.a", "orders.b", "orders.default"}
