"""Shared Bytewax test cases and payload models."""

from __future__ import annotations

from typing import Any, Literal

from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming import IntoTopic
from loom.streaming.compiler import (
    CompiledNode,
    CompiledPlan,
    CompiledSingleSource,
    CompiledSink,
)
from loom.streaming.core._errors import ErrorKind
from loom.streaming.core._message import Message, MessageMeta
from loom.streaming.kafka._config import ConsumerSettings, ProducerSettings
from loom.streaming.nodes._boundary import PartitionPolicy
from loom.streaming.nodes._shape import StreamShape
from loom.streaming.nodes._step import RecordStep

_BROKER = "localhost:9092"
_ORDERS_IN_TOPIC = "orders.in"


class Order(LoomFrozenStruct, frozen=True):
    """Canonical order payload used across Bytewax tests."""

    order_id: str


class Result(LoomFrozenStruct, frozen=True):
    """Canonical result payload used across Bytewax tests."""

    value: str


class DoubleStep(RecordStep[Order, Result]):
    """Duplicate an order id into the output payload."""

    def execute(self, message: Message[Order], **kwargs: object) -> Result:
        del kwargs
        return Result(value=message.payload.order_id * 2)


class Item(LoomStruct):
    """Payload of the end-to-end runtime tests."""

    value: str


class UpperItem(RecordStep[Item, Item]):
    """Uppercase the payload."""

    def execute(self, message: Message[Item], **kwargs: object) -> Item:
        del kwargs
        return Item(value=message.payload.value.upper())


class SuffixStep(RecordStep[Result, Result]):
    """Append a suffix to the transformed payload."""

    def execute(self, message: Message[Result], **kwargs: object) -> Result:
        del kwargs
        return Result(value=f"{message.payload.value}:ok")


def build_message(payload: Order, *, message_id: str = "msg-1") -> Message[Order]:
    """Build a typed Bytewax message for tests."""

    return Message(payload=payload, meta=MessageMeta(message_id=message_id))


def build_compiled_plan(
    *nodes: object,
    output: IntoTopic[Any] | None = None,
    terminal_sinks: dict[tuple[int, ...], CompiledSink] | None = None,
    error_routes: dict[ErrorKind, CompiledSink] | None = None,
) -> CompiledPlan:
    """Build a reusable compiled plan for Bytewax adapter tests."""
    compiled_nodes = [
        CompiledNode(node=node, input_shape=StreamShape.RECORD, output_shape=StreamShape.RECORD)
        for node in nodes
    ]
    compiled_output = None
    if output is not None:
        compiled_output = CompiledSink(
            settings=ProducerSettings(
                brokers=(_BROKER,),
                client_id="test-producer",
                topic=output.name,
            ),
            topic=output.name,
            partition_policy=None,
        )
    return CompiledPlan(
        name="test_flow",
        source=CompiledSingleSource(
            settings=ConsumerSettings(
                brokers=(_BROKER,),
                group_id="test",
                topics=("in",),
            ),
            topics=("in",),
            payload_type=Order,
            shape=StreamShape.RECORD,
            decode_strategy="record",
        ),
        nodes=tuple(compiled_nodes),
        output=compiled_output,
        terminal_sinks=terminal_sinks or {},
        error_routes=error_routes or {},
        needs_async_bridge=False,
    )


def build_compiled_source(
    poll_timeout_ms: int = 100,
    *,
    enable_auto_commit: bool | None = None,
    delivery: Literal["at_least_once", "at_most_once"] | None = None,
    auto_offset_reset: Literal["earliest", "latest"] = "earliest",
    commit_keepalive_ms: int = 1_800_000,
    topics: tuple[str, ...] | None = None,
) -> CompiledSingleSource:
    """Build a reusable compiled source for Bytewax adapter tests."""
    return CompiledSingleSource(
        settings=ConsumerSettings(
            brokers=(_BROKER,),
            group_id="test",
            topics=(_ORDERS_IN_TOPIC,),
            poll_timeout_ms=poll_timeout_ms,
            enable_auto_commit=enable_auto_commit,
            delivery=delivery,
            auto_offset_reset=auto_offset_reset,
            commit_keepalive_ms=commit_keepalive_ms,
        ),
        topics=topics or (_ORDERS_IN_TOPIC,),
        payload_type=Order,
        shape=StreamShape.RECORD,
        decode_strategy="record",
    )


def build_compiled_sink(
    topic: str = "orders.out",
    partition_policy: PartitionPolicy[Any] | None = None,
    dlq_topic: str | None = None,
) -> CompiledSink:
    """Build a reusable compiled sink for Bytewax adapter tests."""
    return CompiledSink(
        settings=ProducerSettings(
            brokers=(_BROKER,),
            client_id="test-producer",
            topic=topic,
        ),
        topic=topic,
        partition_policy=partition_policy,
        dlq_topic=dlq_topic,
    )


def build_order_message(
    order_id: str,
    key: bytes | str | None = None,
    *,
    message_id: str = "msg-1",
    partition: int | None = None,
    offset: int | None = None,
) -> Message[Order]:
    """Build a reusable Bytewax order message with transport metadata."""
    return Message(
        payload=Order(order_id=order_id),
        meta=MessageMeta(
            message_id=message_id,
            topic=_ORDERS_IN_TOPIC,
            key=key,
            partition=partition,
            offset=offset,
        ),
    )
