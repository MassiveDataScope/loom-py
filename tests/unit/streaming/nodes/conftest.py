"""Shared fixtures for Broadcast fan-out tests."""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.model import LoomStruct
from loom.streaming import (
    Broadcast,
    BroadcastRoute,
    FromTopic,
    IntoTopic,
    Message,
    MessageMeta,
    Process,
    RecordStep,
    StreamFlow,
)
from tests.unit.streaming.doubles import AcmeAmountOrder


class _AnalyticsEvent(LoomStruct):
    order_id: str


class _FulfillmentRequest(LoomStruct):
    order_id: str
    quantity: int


class _ToAnalytics(RecordStep[AcmeAmountOrder, _AnalyticsEvent]):
    def execute(self, message: Message[AcmeAmountOrder], **kwargs: object) -> _AnalyticsEvent:
        return _AnalyticsEvent(order_id=message.payload.order_id)


class _ToFulfillment(RecordStep[AcmeAmountOrder, _FulfillmentRequest]):
    def execute(self, message: Message[AcmeAmountOrder], **kwargs: object) -> _FulfillmentRequest:
        return _FulfillmentRequest(
            order_id=message.payload.order_id,
            quantity=message.payload.amount,
        )


@pytest.fixture
def broadcast_flow() -> StreamFlow[AcmeAmountOrder, Any]:
    return StreamFlow(
        name="orders_broadcast",
        source=FromTopic("orders.raw", payload=AcmeAmountOrder),
        process=Process(
            Broadcast(
                BroadcastRoute(
                    process=Process(_ToAnalytics()),
                    output=IntoTopic("events.analytics", payload=_AnalyticsEvent),
                ),
                BroadcastRoute(
                    process=Process(_ToFulfillment()),
                    output=IntoTopic("orders.fulfillment", payload=_FulfillmentRequest),
                ),
            )
        ),
    )


@pytest.fixture
def broadcast_bad_flow() -> StreamFlow[AcmeAmountOrder, AcmeAmountOrder]:
    return StreamFlow(
        name="bad",
        source=FromTopic("orders.raw", payload=AcmeAmountOrder),
        process=Process(
            Broadcast(
                BroadcastRoute(
                    process=Process(_ToAnalytics()),
                    output=IntoTopic("events.analytics", payload=_AnalyticsEvent),
                ),
            )
        ),
        output=IntoTopic("extra_output", payload=AcmeAmountOrder),
    )


@pytest.fixture
def broadcast_messages() -> list[Message[AcmeAmountOrder]]:
    return [
        Message(
            payload=AcmeAmountOrder(order_id=f"ORD-{i}", amount=i),
            meta=MessageMeta(message_id=f"m{i}"),
        )
        for i in range(3)
    ]


@pytest.fixture
def broadcast_message() -> Message[AcmeAmountOrder]:
    return Message(
        payload=AcmeAmountOrder(order_id="ORD-1", amount=3),
        meta=MessageMeta(message_id="m1"),
    )


@pytest.fixture
def broadcast_output_types() -> tuple[type[_AnalyticsEvent], type[_FulfillmentRequest]]:
    return (_AnalyticsEvent, _FulfillmentRequest)


@pytest.fixture
def broadcast_route_pair() -> tuple[
    BroadcastRoute[_AnalyticsEvent], BroadcastRoute[_FulfillmentRequest]
]:
    route_a: BroadcastRoute[_AnalyticsEvent] = BroadcastRoute(
        process=Process(_ToAnalytics()),
        output=IntoTopic("a", payload=_AnalyticsEvent),
    )
    route_b: BroadcastRoute[_FulfillmentRequest] = BroadcastRoute(
        process=Process(_ToFulfillment()),
        output=IntoTopic("b", payload=_FulfillmentRequest),
    )
    return route_a, route_b
