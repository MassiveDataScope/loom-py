"""Core compiler contracts for streaming flows."""

from __future__ import annotations

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.streaming import (
    BatchExpandStep,
    CollectBatch,
    Drain,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    StreamFlow,
    StreamShape,
)
from loom.streaming.compiler import CompilationError, CompiledSingleSource, compile_flow
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result


def test_compile_success_with_minimal_flow(streaming_kafka_config: DictConfig) -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(IntoTopic("out", payload=Result)),
    )

    plan = compile_flow(flow, config=streaming_kafka_config)

    assert plan.name == "test"
    assert isinstance(plan.source, CompiledSingleSource)
    assert plan.source.payload_type is Order
    assert plan.source.decode_strategy == "record"
    assert plan.output is None


def test_compile_fails_when_binding_path_missing() -> None:
    binding = FakeStep.from_config("tasks.missing")
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(binding, IntoTopic("out", payload=Result)),
    )

    config = OmegaConf.create({})
    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, config=config)

    assert "tasks.missing" in str(exc_info.value)


def test_compile_fails_when_kafka_missing_for_topic_flow() -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(FakeStep(), IntoTopic("out", payload=Result)),
    )

    config = OmegaConf.create({})
    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, config=config)

    assert "kafka" in str(exc_info.value)


def test_compile_succeeds_when_kafka_present(streaming_kafka_config: DictConfig) -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(FakeStep(), IntoTopic("out", payload=Result)),
    )

    plan = compile_flow(flow, config=streaming_kafka_config)

    assert plan.name == "test"


class _EachTwice(BatchExpandStep[Order, Order]):
    def execute(self, messages: list[Message[Order]], **kwargs: object) -> list[Order]:
        del kwargs
        return [message.payload for message in messages for _ in range(2)]


def test_a_record_step_follows_a_batch_expand_step(streaming_kafka_config: DictConfig) -> None:
    """A BatchExpandStep subclass turns the batch back into records."""
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(
            CollectBatch(2, 10), _EachTwice(), FakeStep(), IntoTopic("out", payload=Result)
        ),
    )

    plan = compile_flow(flow, config=streaming_kafka_config)

    assert [node.output_shape for node in plan.nodes[:3]] == [
        StreamShape.BATCH,
        StreamShape.RECORD,
        StreamShape.RECORD,
    ]


def test_compile_fails_on_shape_mismatch(streaming_kafka_config: DictConfig) -> None:
    """A record step after CollectBatch receives a batch."""
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(CollectBatch(2, 10), FakeStep(), IntoTopic("out", payload=Result)),
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, config=streaming_kafka_config)

    assert "shape mismatch" in str(exc_info.value)


def test_compile_fails_without_output(streaming_kafka_config: DictConfig) -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(FakeStep()),
        output=None,
    )

    with pytest.raises(CompilationError) as exc_info:
        compile_flow(flow, config=streaming_kafka_config)

    assert "no terminal output" in str(exc_info.value)


def test_compile_drain_outputs_none_shape(streaming_kafka_config: DictConfig) -> None:
    flow: StreamFlow[Order, Result] = StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(Drain()),
    )

    plan = compile_flow(flow, config=streaming_kafka_config)

    assert plan.nodes[-1].output_shape is StreamShape.NONE
    assert plan.output is None
