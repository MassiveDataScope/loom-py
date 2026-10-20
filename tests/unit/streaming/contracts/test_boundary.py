"""Boundary contract tests for streaming."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.routing import LogicalRef
from loom.streaming import (
    FromMongoCDC,
    FromMultiTypeTopic,
    FromTopic,
    IntoTopic,
    Message,
    PartitionGuarantee,
    PartitionPolicy,
    PartitionStrategy,
    StreamShape,
)
from tests.unit.streaming.contracts.cases import Order


class _OrderPartitionStrategy:
    def partition_key(self, message: Message[Order]) -> bytes | str | None:
        return message.payload.order_id


class TestBoundaryContracts:
    @pytest.mark.parametrize(
        ("boundary_type", "fields"),
        [
            (FromTopic, {"payload": Order}),
            (FromMultiTypeTopic, {"payloads": (Order, Order)}),
            (FromMongoCDC, {}),
            (IntoTopic, {}),
        ],
        ids=lambda value: value.__name__ if isinstance(value, type) else "",
    )
    def test_a_boundary_declares_no_shape(self, boundary_type: Any, fields: dict[str, Any]) -> None:
        """Sources emit and topics take one record per message; CollectBatch makes a batch."""
        with pytest.raises(TypeError, match="shape"):
            boundary_type("orders.in", shape=StreamShape.BATCH, **fields)

    def test_topic_boundaries_use_logical_refs_only(self) -> None:
        source = FromTopic("orders-input", payload=Order)
        target = IntoTopic("validated-orders", payload=Order)

        assert source.logical_ref == LogicalRef("orders-input")
        assert target.logical_ref == LogicalRef("validated-orders")

    def test_into_topic_can_be_used_without_payload_for_error_routes(self) -> None:
        target: IntoTopic[Any] = IntoTopic("orders.dlq")

        assert target.payload is None

    def test_into_topic_can_declare_partitioning_policy(self) -> None:
        strategy: PartitionStrategy[Order] = _OrderPartitionStrategy()
        policy = PartitionPolicy(
            strategy=strategy,
            guarantee=PartitionGuarantee.ENTITY_STABLE,
            allow_repartition=True,
        )
        target = IntoTopic("orders.out", payload=Order, partitioning=policy)

        assert target.partitioning is policy
        assert policy.guarantee is PartitionGuarantee.ENTITY_STABLE
        assert policy.allow_repartition is True
