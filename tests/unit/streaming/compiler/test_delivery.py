"""Delivery-semantics validation: a declared delivery and what it requires of a flow."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import DictConfig, OmegaConf

from loom.core.config import ConfigContext
from loom.streaming import Drain, Fork, ForkRoute, FromTopic, IntoTopic, Process, StreamFlow, msg
from loom.streaming.compiler import CompilationError, StreamingErrorCode, compile_flow
from loom.streaming.compiler._plan import CompiledSingleSource
from loom.streaming.compiler.phases.validate import validate_delivery
from tests.unit.streaming.compiler.cases import FakeStep, Order, Result

_BROKER = "localhost:9092"


def _flow() -> StreamFlow[Order, Result]:
    return StreamFlow(
        name="test",
        source=FromTopic("in", payload=Order),
        process=Process(FakeStep(), IntoTopic("out", payload=Result)),
    )


def _kafka_config(consumer_overrides: dict[str, object]) -> dict[str, object]:
    return {
        "kafka": {
            "consumer": {
                "brokers": [_BROKER],
                "group_id": "test",
                "topics": ["orders.raw"],
                **consumer_overrides,
            },
            "producer": {"brokers": [_BROKER], "topic": "orders.out"},
        }
    }


class TestValidateDelivery:
    def test_missing_kafka_section_is_not_reported_here(self) -> None:
        assert validate_delivery(_flow(), ConfigContext.from_dict({})) == []


class TestCompileFlowUndeclaredDelivery:
    def test_a_consumer_without_delivery_is_refused(self) -> None:
        config = OmegaConf.create(_kafka_config({}))
        flow = _flow()

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=config)

        issues = [
            issue
            for issue in exc_info.value.issues
            if issue.code is StreamingErrorCode.KAFKA_CONFIG_INVALID
        ]
        assert len(issues) == 1
        assert "missing required field `delivery`" in issues[0].message


class TestUnroutedForkUnderAtLeastOnce:
    def _flow_with_fork(self, default: Process[Any, Any] | None) -> StreamFlow[Order, Result]:
        return StreamFlow(
            name="test",
            source=FromTopic("in", payload=Order),
            process=Process(
                Fork.when(
                    routes=[
                        ForkRoute(
                            when=msg.payload.order_id == "vip",
                            process=Process(IntoTopic("out", payload=Order)),
                        )
                    ],
                    default=default,
                )
            ),
        )

    def test_fork_without_default_is_rejected_under_at_least_once(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        config = OmegaConf.merge(
            streaming_kafka_config,
            {"kafka": {"consumer": {"delivery": "at_least_once"}}},
        )

        flow = self._flow_with_fork(default=None)

        with pytest.raises(CompilationError) as exc_info:
            compile_flow(flow, config=config)

        codes = {issue.code for issue in exc_info.value.issues}
        assert StreamingErrorCode.FORK_UNMATCHED_UNROUTED in codes

    def test_fork_with_default_passes_under_at_least_once(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        config = OmegaConf.merge(
            streaming_kafka_config,
            {"kafka": {"consumer": {"delivery": "at_least_once"}}},
        )

        plan = compile_flow(self._flow_with_fork(default=Process(Drain())), config=config)

        assert plan.name == "test"

    def test_fork_without_default_is_allowed_under_at_most_once(
        self, streaming_kafka_config: DictConfig
    ) -> None:
        plan = compile_flow(self._flow_with_fork(default=None), config=streaming_kafka_config)

        assert plan.name == "test"


class TestConsumerSettingsKey:
    """The compiled source records which consumer section its settings came from."""

    def test_a_source_bound_through_the_default_consumer(self) -> None:
        config = OmegaConf.create(_kafka_config({"delivery": "at_most_once"}))

        plan = compile_flow(_flow(), config=config)

        assert isinstance(plan.source, CompiledSingleSource)
        assert plan.source.settings_key == "kafka.consumer"

    def test_a_source_bound_through_a_named_consumer(self) -> None:
        config = _kafka_config({"delivery": "at_most_once"})
        kafka = config["kafka"]
        assert isinstance(kafka, dict)
        kafka["consumers"] = {"in": kafka.pop("consumer")}

        plan = compile_flow(_flow(), config=OmegaConf.create(config))

        assert isinstance(plan.source, CompiledSingleSource)
        assert plan.source.settings_key == "kafka.consumers.in"
