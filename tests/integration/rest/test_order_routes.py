"""ADR 0002 end to end: a shop's order routes, over an in-memory store."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID, uuid4

import msgspec
from fastapi.testclient import TestClient

from loom.core.bootstrap.bootstrap import bootstrap_app
from loom.core.command import Command
from loom.core.di.container import LoomContainer
from loom.core.errors import NotFound, RateLimited
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case import Created, Input, Query
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RouteSources
from loom.rest.fastapi.app import create_fastapi_app
from loom.rest.model import RestInterface, RestRoute

_CUSTOMER = UUID("12345678-1234-5678-1234-567812345678")
_FASTAPI_SCHEMAS = frozenset({"HTTPValidationError", "ValidationError"})


class OrderChannel(StrEnum):
    WEB = "web"
    STORE = "store"


class OrderStatus(StrEnum):
    OPEN = "open"
    SHIPPED = "shipped"
    CANCELLED = "cancelled"


class OrderView(msgspec.Struct, frozen=True, kw_only=True, rename="camel"):
    order_id: UUID
    customer_id: UUID
    channel: OrderChannel
    status: OrderStatus


class OrderPage(msgspec.Struct, frozen=True, kw_only=True, rename="camel"):
    items: list[OrderView]
    next_cursor: str | None = None


class OrderQuery(
    msgspec.Struct, frozen=True, kw_only=True, rename="camel", forbid_unknown_fields=True
):
    customer_id: UUID | None = None
    channel: OrderChannel | None = None
    status: OrderStatus | None = None
    changed_since: Annotated[datetime, msgspec.Meta(tz=True)] | None = msgspec.field(
        default=None, name="changed_since"
    )
    cursor: str | None = None
    limit: Annotated[int, msgspec.Meta(ge=1, le=200)] = 50


class CreateOrderCommand(Command, frozen=True):
    customer_id: UUID
    channel: OrderChannel


class OrderStore:
    def __init__(self) -> None:
        self._orders: dict[UUID, OrderView] = {}

    def place(self, customer_id: UUID, channel: OrderChannel) -> tuple[OrderView, bool]:
        for view in self._orders.values():
            if (view.customer_id, view.channel, view.status) == (
                customer_id,
                channel,
                OrderStatus.OPEN,
            ):
                return view, False
        view = OrderView(
            order_id=uuid4(),
            customer_id=customer_id,
            channel=channel,
            status=OrderStatus.OPEN,
        )
        self._orders[view.order_id] = view
        return view, True

    def cancel(self, order_id: UUID) -> OrderView:
        if order_id not in self._orders:
            raise NotFound("Order", id=order_id)
        view = msgspec.structs.replace(self._orders[order_id], status=OrderStatus.CANCELLED)
        self._orders[order_id] = view
        return view

    def matching(self, query: OrderQuery) -> list[OrderView]:
        return [
            view
            for view in self._orders.values()
            if query.channel in (None, view.channel) and query.status in (None, view.status)
        ][: query.limit]


class OrderBudget:
    def __init__(self, tokens: int) -> None:
        self._tokens = tokens

    def spend(self) -> None:
        if self._tokens == 0:
            raise RateLimited(retry_after=1.5)
        self._tokens -= 1


class ListOrders(UseCase[Any, OrderPage]):
    def __init__(self, store: OrderStore) -> None:
        super().__init__()
        self._store = store

    async def execute(self, query: OrderQuery = Query()) -> OrderPage:
        return OrderPage(items=self._store.matching(query))


class CreateOrder(UseCase[Any, OrderView | Created[OrderView]]):
    def __init__(self, store: OrderStore, budget: OrderBudget) -> None:
        super().__init__()
        self._store = store
        self._budget = budget

    async def execute(
        self, command: CreateOrderCommand = Input()
    ) -> OrderView | Created[OrderView]:
        self._budget.spend()
        view, created = self._store.place(command.customer_id, command.channel)
        return Created(view, location=f"/v1/orders/{view.order_id}") if created else view


class CancelOrder(UseCase[Any, OrderView]):
    def __init__(self, store: OrderStore, budget: OrderBudget) -> None:
        super().__init__()
        self._store = store
        self._budget = budget

    async def execute(self, order_id: UUID) -> OrderView:
        self._budget.spend()
        return self._store.cancel(order_id)


class OrdersInterface(RestInterface[Any]):
    prefix = "/v1/orders"
    tags = ("orders",)
    routes = (
        RestRoute(use_case=ListOrders, method="GET", path="", summary="Orders"),
        RestRoute(use_case=CreateOrder, method="POST", path="", summary="Place an order"),
        RestRoute(
            use_case=CancelOrder, method="DELETE", path="/{order_id}", summary="Cancel an order"
        ),
    )


class _Config:
    debug = False


def _client(tokens: int = 10) -> TestClient:
    def register(container: LoomContainer) -> None:
        container.register_instance(OrderStore, OrderStore())
        container.register_instance(OrderBudget, OrderBudget(tokens))

    modules: list[Callable[[LoomContainer], None]] = [register]
    result = bootstrap_app(
        config=_Config(), use_cases=[ListOrders, CreateOrder, CancelOrder], modules=modules
    )
    app = create_fastapi_app(
        result,
        RouteSources(python=[OrdersInterface]),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    return TestClient(app)


def _order(client: TestClient, channel: str = "web") -> Any:
    return client.post("/v1/orders", json={"customerId": str(_CUSTOMER), "channel": channel})


def test_the_openapi_document_matches_the_golden(assert_output_snapshot: Any) -> None:
    document = _client().get("/openapi.json").json()
    operations = {
        f"{method.upper()} {path}": {
            "parameters": operation.get("parameters", []),
            "requestBody": operation.get("requestBody"),
            "responses": {
                status: response
                for status, response in operation["responses"].items()
                if status.startswith("2")
            },
        }
        for path, methods in document["paths"].items()
        for method, operation in methods.items()
    }
    schemas = {
        name: schema
        for name, schema in document["components"]["schemas"].items()
        if name not in _FASTAPI_SCHEMAS
    }

    assert_output_snapshot(
        {"operations": operations, "schemas": schemas}, "rest_order_routes_openapi"
    )


def test_a_new_order_answers_201_and_a_repeated_one_200() -> None:
    client = _client()

    created = _order(client)
    repeated = _order(client)

    assert (created.status_code, repeated.status_code) == (201, 200)
    assert created.json() == repeated.json()
    assert created.headers["Location"] == f"/v1/orders/{created.json()['orderId']}"


def test_a_spent_budget_answers_429_with_retry_after() -> None:
    response = _order(_client(tokens=0))

    assert (response.status_code, response.headers["Retry-After"]) == (429, "2")
    assert response.json()["detail"]["code"] == "rate_limited"
