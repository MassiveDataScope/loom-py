"""Fork flow-case builders."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from omegaconf import DictConfig

from loom.streaming import (
    CollectBatch,
    ContextSupplier,
    Drain,
    Fork,
    ForkRoute,
    FromTopic,
    IntoTopic,
    Process,
    ResourceScope,
    StreamFlow,
    With,
    msg,
)
from tests.unit.streaming.flows.cases.shared import (
    _ORDERS_FORK_STANDARD_TOPIC,
    _ORDERS_FORK_VIP_TOPIC,
    OrderPlaced,
    PricedOrder,
    PriceOrder,
    ResourceEvents,
    RoutedOrder,
    StreamFlowCase,
    _build_case,
    _message,
    _routed_order_message,
)


@dataclass(frozen=True, slots=True)
class _ForkCaseSpec:
    """Parameters for a reusable fork flow-case builder."""

    name: str
    source_payload: type[Any]
    process: Process[Any, Any]
    input_messages: tuple[Any, ...]
    expected_payloads: tuple[Any, ...]
    resource_events: ResourceEvents | None = None


def build_fork_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a terminal Fork flow with independent branch outputs."""
    return _build_fork_case(
        config=config,
        spec=_ForkCaseSpec(
            name="orders_fork",
            source_payload=RoutedOrder,
            process=_fork_by_process(),
            input_messages=_fork_by_input_messages(),
            expected_payloads=_fork_by_expected_payloads(),
        ),
    )


def build_fork_with_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a Fork flow whose matched branch opens a scoped resource."""
    events = ResourceEvents()
    return _build_fork_case(
        config=config,
        spec=_ForkCaseSpec(
            name="orders_fork_with",
            source_payload=OrderPlaced,
            process=_fork_with_process(events),
            input_messages=_fork_with_input_messages(),
            expected_payloads=_fork_with_expected_payloads(),
            resource_events=events,
        ),
    )


def build_fork_when_flow_case(config: DictConfig) -> StreamFlowCase:
    """Build a Fork.when flow with ordered predicate dispatch."""
    return _build_fork_case(
        config=config,
        spec=_ForkCaseSpec(
            name="orders_fork_when",
            source_payload=OrderPlaced,
            process=_fork_when_process(),
            input_messages=_fork_when_input_messages(),
            expected_payloads=_fork_when_expected_payloads(),
        ),
    )


def _build_fork_case(
    *,
    config: DictConfig,
    spec: _ForkCaseSpec,
) -> StreamFlowCase:
    flow: StreamFlow[Any, Any] = StreamFlow(
        name=spec.name,
        source=FromTopic("orders.raw", payload=spec.source_payload),
        process=spec.process,
    )
    return _build_case(
        flow=flow,
        config=config,
        input_messages=spec.input_messages,
        expected_payloads=spec.expected_payloads,
        resource_events=spec.resource_events,
    )


def _fork_by_process() -> Process[Any, Any]:
    return Process(
        Fork.by(
            msg.payload.lane,
            branches={
                "vip": Process(IntoTopic(_ORDERS_FORK_VIP_TOPIC, payload=RoutedOrder)),
                "standard": Process(IntoTopic(_ORDERS_FORK_STANDARD_TOPIC, payload=RoutedOrder)),
            },
            default=Process(Drain()),
        )
    )


def _fork_by_input_messages() -> tuple[Any, ...]:
    return (
        _routed_order_message("o-1", "vip"),
        _routed_order_message("o-2", "standard"),
        _routed_order_message("o-3", "manual"),
    )


def _fork_by_expected_payloads() -> tuple[Any, ...]:
    return (
        RoutedOrder(order_id="o-1", lane="vip"),
        RoutedOrder(order_id="o-2", lane="standard"),
    )


def _fork_with_process(events: ResourceEvents) -> Process[Any, Any]:
    return Process(
        Fork.when(
            routes=[
                ForkRoute(
                    when=msg.payload.amount >= 100,
                    process=Process(
                        CollectBatch(max_records=1, timeout_ms=1000),
                        With(
                            process=Process(
                                PriceOrder(),
                                IntoTopic(_ORDERS_FORK_VIP_TOPIC, payload=PricedOrder),
                            ),
                            client=ContextSupplier(events.create_pricing_client),
                            scope=ResourceScope.BATCH,
                        ),
                    ),
                )
            ],
            default=Process(Drain()),
        )
    )


def _fork_with_input_messages() -> tuple[Any, ...]:
    return (
        _message(OrderPlaced(order_id="o-1", amount=100, segment="vip")),
        _message(OrderPlaced(order_id="o-2", amount=50, segment="standard")),
    )


def _fork_with_expected_payloads() -> tuple[Any, ...]:
    return (PricedOrder(order_id="o-1", price_band="high", client_id=1),)


def _fork_when_process() -> Process[Any, Any]:
    return Process(
        Fork.when(
            routes=[
                ForkRoute(
                    when=msg.payload.segment == "vip",
                    process=Process(IntoTopic(_ORDERS_FORK_VIP_TOPIC, payload=OrderPlaced)),
                ),
                ForkRoute(
                    when=msg.payload.amount >= 100,
                    process=Process(
                        IntoTopic(
                            _ORDERS_FORK_STANDARD_TOPIC,
                            payload=OrderPlaced,
                        )
                    ),
                ),
            ],
            default=Process(Drain()),
        )
    )


def _fork_when_input_messages() -> tuple[Any, ...]:
    return (
        _message(OrderPlaced(order_id="o-1", amount=100, segment="vip")),
        _message(OrderPlaced(order_id="o-2", amount=50, segment="standard")),
        _message(OrderPlaced(order_id="o-3", amount=500, segment="manual")),
    )


def _fork_when_expected_payloads() -> tuple[Any, ...]:
    return (
        OrderPlaced(order_id="o-1", amount=100, segment="vip"),
        OrderPlaced(order_id="o-3", amount=500, segment="manual"),
    )
