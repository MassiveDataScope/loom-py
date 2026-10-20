"""Shared fixtures for streaming unit tests."""

from __future__ import annotations

from typing import cast

import pytest
from omegaconf import DictConfig, OmegaConf

from tests.unit.streaming.flows.cases import (
    StreamFlowCase,
    build_async_flow_case,
    build_fork_flow_case,
    build_fork_when_flow_case,
    build_fork_with_flow_case,
    build_router_flow_case,
    build_simple_validation_flow_case,
    build_with_batch_flow_case,
    build_with_batch_scope_flow_case,
)


@pytest.fixture(scope="session")
def streaming_kafka_config() -> DictConfig:
    """Return the shared Kafka runtime config used by streaming flow cases."""
    return OmegaConf.create(_streaming_kafka_config_data())


@pytest.fixture(scope="session")
def streaming_kafka_config_dict(
    streaming_kafka_config: DictConfig,
) -> dict[str, object]:
    """Return the shared Kafka runtime config as a plain dictionary."""
    return cast(dict[str, object], OmegaConf.to_container(streaming_kafka_config, resolve=True))


@pytest.fixture
def simple_validation_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return the simplest public DSL flow case."""
    return build_simple_validation_flow_case(streaming_kafka_config)


@pytest.fixture
def router_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with multiple router branches."""
    return build_router_flow_case(streaming_kafka_config)


@pytest.fixture
def with_batch_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with With and ForEach."""
    return build_with_batch_flow_case(streaming_kafka_config)


@pytest.fixture
def with_batch_scope_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with BATCH-scoped With resources."""
    return build_with_batch_scope_flow_case(streaming_kafka_config)


@pytest.fixture
def async_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with WithAsync and ForEach."""
    return build_async_flow_case(streaming_kafka_config)


@pytest.fixture
def fork_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with Fork terminal branches."""
    return build_fork_flow_case(streaming_kafka_config)


@pytest.fixture
def fork_with_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with Fork branches that open resources."""
    return build_fork_with_flow_case(streaming_kafka_config)


@pytest.fixture
def fork_when_flow_case(streaming_kafka_config: DictConfig) -> StreamFlowCase:
    """Return a public DSL flow case with ordered Fork.when branches."""
    return build_fork_when_flow_case(streaming_kafka_config)


_BROKER = "localhost:9092"
_ORDERS_VALIDATED_TOPIC = "orders.validated"


def _streaming_kafka_config_data() -> dict[str, object]:
    return {
        "kafka": {
            "consumer": {
                "brokers": [_BROKER],
                "group_id": "test",
                "delivery": "at_most_once",
                "topics": ["orders.raw"],
            },
            "producer": {"brokers": [_BROKER], "topic": _ORDERS_VALIDATED_TOPIC},
            "producers": {
                _ORDERS_VALIDATED_TOPIC: {
                    "brokers": [_BROKER],
                    "topic": _ORDERS_VALIDATED_TOPIC,
                },
                "orders.routed": {
                    "brokers": [_BROKER],
                    "topic": "orders.routed",
                },
                "orders.a": {
                    "brokers": [_BROKER],
                    "topic": "orders.a",
                },
                "orders.b": {
                    "brokers": [_BROKER],
                    "topic": "orders.b",
                },
                "orders.default": {
                    "brokers": [_BROKER],
                    "topic": "orders.default",
                },
                "orders.priced": {
                    "brokers": [_BROKER],
                    "topic": "orders.priced",
                },
                "orders.priced.batch_scope": {
                    "brokers": [_BROKER],
                    "topic": "orders.priced.batch_scope",
                },
                "orders.scored": {
                    "brokers": [_BROKER],
                    "topic": "orders.scored",
                },
                "orders.fork.vip": {
                    "brokers": [_BROKER],
                    "topic": "orders.fork.vip",
                },
                "orders.fork.standard": {
                    "brokers": [_BROKER],
                    "topic": "orders.fork.standard",
                },
                "events.analytics": {
                    "brokers": [_BROKER],
                    "topic": "events.analytics",
                },
                "orders.fulfillment": {
                    "brokers": [_BROKER],
                    "topic": "orders.fulfillment",
                },
                "extra_output": {
                    "brokers": [_BROKER],
                    "topic": "extra_output",
                },
            },
        }
    }
