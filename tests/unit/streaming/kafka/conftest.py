"""Shared fixtures for Kafka unit tests."""

from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry

from loom.prometheus import KafkaPrometheusMetrics
from loom.streaming.kafka import (
    MessageDescriptor,
    MessageEnvelope,
    MsgspecCodec,
    SchemaRef,
    build_message,
)
from tests.unit.streaming.kafka.cases import OrderCreated, ProductEvent
from tests.unit.streaming.kafka.doubles import RawConsumerStub, RawProducerStub

_ORDER_CREATED_TYPE = "order.created"


@pytest.fixture
def kafka_registry() -> CollectorRegistry:
    """Return a fresh Prometheus registry for one Kafka test."""
    return CollectorRegistry()


@pytest.fixture
def kafka_metrics(kafka_registry: CollectorRegistry) -> KafkaPrometheusMetrics:
    """Return Kafka metrics bound to the shared test registry."""
    return KafkaPrometheusMetrics(registry=kafka_registry)


@pytest.fixture
def order_created_payload() -> OrderCreated:
    """Return the canonical OrderCreated payload used across Kafka tests."""
    return OrderCreated(order_id="o-1", amount=5)


@pytest.fixture
def product_event_payload() -> ProductEvent:
    """Return the canonical ProductEvent payload used for codec tests."""
    return ProductEvent(sku="sku-1", stock=10)


@pytest.fixture
def order_created_descriptor_v1() -> MessageDescriptor:
    """Return the canonical order.created descriptor used across Kafka tests."""
    return MessageDescriptor(
        message_type=_ORDER_CREATED_TYPE,
        message_version=1,
        schema_ref=SchemaRef(
            namespace="orders",
            name=_ORDER_CREATED_TYPE,
            version="1",
            format="loom-msgpack",
        ),
    )


@pytest.fixture
def order_created_descriptor_v2() -> MessageDescriptor:
    """Return the alternate order.created descriptor used in trace tests."""
    return MessageDescriptor(message_type=_ORDER_CREATED_TYPE, message_version=2)


@pytest.fixture
def product_event_descriptor() -> MessageDescriptor:
    """Return the descriptor for codec round-trip tests."""
    return MessageDescriptor(message_type="product.stock.updated", message_version=1)


@pytest.fixture
def order_created_envelope(
    order_created_payload: OrderCreated,
    order_created_descriptor_v1: MessageDescriptor,
) -> MessageEnvelope[OrderCreated]:
    """Return a canonical order.created envelope used in wire tests."""
    return build_message(order_created_payload, order_created_descriptor_v1, produced_at_ms=100)


@pytest.fixture
def order_created_envelope_with_metadata(
    order_created_payload: OrderCreated,
    order_created_descriptor_v1: MessageDescriptor,
) -> MessageEnvelope[OrderCreated]:
    """Return an order.created envelope with trace and correlation metadata."""
    return build_message(
        order_created_payload,
        order_created_descriptor_v1,
        correlation_id="corr-1",
        parent_trace_id="parent-1",
        causation_id="cause-1",
        trace_id="trace-1",
        produced_at_ms=1234,
    )


@pytest.fixture
def product_event_envelope(
    product_event_payload: ProductEvent,
    product_event_descriptor: MessageDescriptor,
) -> MessageEnvelope[ProductEvent]:
    """Return the canonical product.stock.updated envelope used in codec tests."""
    return build_message(product_event_payload, product_event_descriptor, produced_at_ms=100)


@pytest.fixture
def order_created_encoded_envelope(
    order_created_envelope: MessageEnvelope[OrderCreated],
) -> bytes:
    """Return the encoded canonical order.created envelope."""
    return MsgspecCodec[OrderCreated]().encode(order_created_envelope)


@pytest.fixture
def product_event_encoded_envelope(
    product_event_envelope: MessageEnvelope[ProductEvent],
) -> bytes:
    """Return the encoded canonical product.stock.updated envelope."""
    return MsgspecCodec[ProductEvent]().encode(product_event_envelope)


@pytest.fixture
def order_created_codec() -> MsgspecCodec[OrderCreated]:
    """Return the canonical Msgspec codec for OrderCreated payloads."""
    return MsgspecCodec[OrderCreated]()


@pytest.fixture
def raw_producer_stub() -> RawProducerStub:
    """Return a raw producer stub for message-level Kafka tests."""
    return RawProducerStub()


@pytest.fixture
def raw_consumer_stub() -> RawConsumerStub:
    """Return a raw consumer stub for message-level Kafka tests."""
    return RawConsumerStub()
