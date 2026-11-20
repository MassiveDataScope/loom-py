"""Batch and async flow-case builders."""

from __future__ import annotations

from typing import Any

from omegaconf import DictConfig

from loom.streaming import (
    CollectBatch,
    ContextSupplier,
    FromTopic,
    IntoTopic,
    Process,
    ResourceScope,
    StreamFlow,
    With,
    WithAsync,
)
from tests.unit.streaming.flows.cases.shared import (
    _ORDERS_PRICED_BATCH_SCOPE_TOPIC,
    _ORDERS_PRICED_TOPIC,
    _ORDERS_RAW_TOPIC,
    _ORDERS_SCORED_TOPIC,
    FakeRiskClient,
    OrderPlaced,
    PricedOrder,
    PriceOrder,
    ResourceEvents,
    RiskScoredOrder,
    ScoreRiskAsync,
    StreamFlowCase,
    _build_case,
    _message,
)


def build_with_batch_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a batch flow whose scoped inner process writes each result."""
    events = ResourceEvents()
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_price_batch",
        source=FromTopic(_ORDERS_RAW_TOPIC, payload=OrderPlaced),
        process=Process(
            CollectBatch(max_records=2, timeout_ms=1000),
            With(
                process=Process(
                    PriceOrder(),
                    IntoTopic(_ORDERS_PRICED_TOPIC, payload=PricedOrder),
                ),
                client=events.create_pricing_client(),
                scope=ResourceScope.WORKER,
            ),
        ),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(
            _message(OrderPlaced(order_id="o-1", amount=100, segment="vip")),
            _message(OrderPlaced(order_id="o-2", amount=50, segment="standard")),
            _message(OrderPlaced(order_id="o-3", amount=500, segment="vip")),
            _message(OrderPlaced(order_id="o-4", amount=20, segment="standard")),
        ),
        expected_payloads=(
            PricedOrder(order_id="o-1", price_band="high", client_id=1),
            PricedOrder(order_id="o-2", price_band="low", client_id=1),
            PricedOrder(order_id="o-3", price_band="high", client_id=1),
            PricedOrder(order_id="o-4", price_band="low", client_id=1),
        ),
        resource_events=events,
    )


def build_with_batch_scope_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a batch flow using a fresh ContextSupplier client per batch."""
    events = ResourceEvents()
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_price_batch_scope",
        source=FromTopic(_ORDERS_RAW_TOPIC, payload=OrderPlaced),
        process=Process(
            CollectBatch(max_records=2, timeout_ms=1000),
            With(
                process=Process(
                    PriceOrder(),
                    IntoTopic(_ORDERS_PRICED_BATCH_SCOPE_TOPIC, payload=PricedOrder),
                ),
                client=ContextSupplier(events.create_pricing_client),
                scope=ResourceScope.BATCH,
            ),
        ),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(
            _message(OrderPlaced(order_id="o-1", amount=100, segment="vip")),
            _message(OrderPlaced(order_id="o-2", amount=50, segment="standard")),
            _message(OrderPlaced(order_id="o-3", amount=500, segment="vip")),
            _message(OrderPlaced(order_id="o-4", amount=20, segment="standard")),
        ),
        expected_payloads=(
            PricedOrder(order_id="o-1", price_band="high", client_id=1),
            PricedOrder(order_id="o-2", price_band="low", client_id=1),
            PricedOrder(order_id="o-3", price_band="high", client_id=2),
            PricedOrder(order_id="o-4", price_band="low", client_id=2),
        ),
        resource_events=events,
    )


def build_async_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build an async flow using WithAsync(process=...) to write results directly."""
    flow: StreamFlow[Any, Any] = StreamFlow(
        name="orders_score_async_each",
        source=FromTopic(_ORDERS_RAW_TOPIC, payload=OrderPlaced),
        process=Process(
            WithAsync(
                process=Process(
                    ScoreRiskAsync(),
                    IntoTopic(_ORDERS_SCORED_TOPIC, payload=RiskScoredOrder),
                ),
                client=FakeRiskClient(),
                max_concurrency=2,
                scope=ResourceScope.WORKER,
            ),
        ),
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=(
            _message(OrderPlaced(order_id="o-1", amount=100, segment="vip")),
            _message(OrderPlaced(order_id="o-2", amount=50, segment="standard")),
        ),
        expected_payloads=(
            RiskScoredOrder(order_id="o-1", risk_band="high"),
            RiskScoredOrder(order_id="o-2", risk_band="low"),
        ),
    )
