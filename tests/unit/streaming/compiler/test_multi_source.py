"""Compiler tests for FromMultiTypeTopic and CompiledMultiSource."""

from __future__ import annotations

from omegaconf import DictConfig

from loom.streaming import (
    FromMultiTypeTopic,
    FromTopic,
    IntoTopic,
    Process,
    StreamFlow,
)
from loom.streaming.compiler import CompiledMultiSource, CompiledSingleSource, compile_flow
from loom.streaming.core._errors import ErrorEnvelope
from loom.streaming.kafka import DecodeError
from tests.unit.streaming.compiler.cases import Order, Result
from tests.unit.streaming.doubles import AcmeOrderEvent, AcmeSkuProductEvent


def _fqn(t: type[object]) -> str:
    return f"{t.__module__}.{t.__qualname__}"


class TestCompileFlowWithMultiSource:
    def test_compiles_multi_source_and_builds_dispatch_tables(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        flow: StreamFlow[
            AcmeOrderEvent | AcmeSkuProductEvent | ErrorEnvelope[AcmeOrderEvent] | DecodeError,
            Result,
        ] = StreamFlow(
            name="multi-consumer",
            source=FromMultiTypeTopic(
                "in",
                payloads=(
                    AcmeOrderEvent,
                    AcmeSkuProductEvent,
                    ErrorEnvelope[AcmeOrderEvent],
                    DecodeError,
                ),
            ),
            process=Process(IntoTopic("out", payload=Result)),
        )

        plan = compile_flow(flow, config=streaming_kafka_config)

        assert isinstance(plan.source, CompiledMultiSource)
        assert _fqn(AcmeOrderEvent) in plan.source.dispatch.plain
        assert _fqn(AcmeSkuProductEvent) in plan.source.dispatch.plain
        assert plan.source.dispatch.plain[_fqn(AcmeOrderEvent)] is AcmeOrderEvent
        assert plan.source.dispatch.plain[_fqn(AcmeSkuProductEvent)] is AcmeSkuProductEvent
        assert _fqn(AcmeOrderEvent) in plan.source.dispatch.error
        assert DecodeError.loom_message_type() in plan.source.dispatch.wire
        assert plan.source.dispatch.wire[DecodeError.loom_message_type()] is DecodeError

    def test_from_topic_compilation_is_unchanged(self, streaming_kafka_config: DictConfig) -> None:
        flow: StreamFlow[Order, Result] = StreamFlow(
            name="single-consumer",
            source=FromTopic("in", payload=Order),
            process=Process(IntoTopic("out", payload=Result)),
        )

        plan = compile_flow(flow, config=streaming_kafka_config)

        assert isinstance(plan.source, CompiledSingleSource)
