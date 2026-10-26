"""The success status a REST route answers follows the use case's result."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case import Created
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.fastapi.app import _register_openapi_components
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestInterface, RestRoute


class OrderView(msgspec.Struct, frozen=True):
    order_id: str


class CreateOrder(UseCase[Any, OrderView | Created[OrderView]]):
    async def execute(self, order_id: str) -> OrderView | Created[OrderView]:
        view = OrderView(order_id=order_id)
        return Created(view) if order_id.startswith("fresh") else view


class ReadOrder(UseCase[Any, OrderView]):
    async def execute(self, order_id: str) -> OrderView:
        return OrderView(order_id=order_id)


class Opaque:
    pass


class ReturnsAnything(UseCase[Any, Any]):
    async def execute(self) -> Any:
        return {}


class ReturnsOpaque(UseCase[Any, Any]):
    async def execute(self) -> Opaque | Created[Opaque]:
        return Opaque()


class Undocumented(RestInterface[Any]):
    prefix = "/undocumented"
    routes = (
        RestRoute(use_case=ReturnsAnything, method="GET", path="/anything"),
        RestRoute(use_case=ReturnsOpaque, method="GET", path="/opaque"),
    )


class Orders(RestInterface[Any]):
    prefix = "/orders"
    routes = (
        RestRoute(use_case=CreateOrder, method="PUT", path="/{order_id}"),
        RestRoute(use_case=ReadOrder, method="GET", path="/{order_id}"),
    )


class AcceptedOrders(RestInterface[Any]):
    prefix = "/accepted"
    routes = (RestRoute(use_case=CreateOrder, method="PUT", path="/{order_id}", status_code=202),)


def _client(interface: type[RestInterface[Any]]) -> TestClient:
    use_cases = [route.use_case for route in interface.routes]
    compiler = UseCaseCompiler()
    factory = UseCaseFactory(LoomContainer())
    for use_case in use_cases:
        compiler.compile(use_case)
        factory.register(use_case)
    app = FastAPI()
    register_error_handlers(app)
    registry = bind_interfaces(
        app,
        RestInterfaceCompiler(compiler).compile(interface),
        factory,
        RuntimeExecutor(compiler),
        ObservabilityRuntime.noop(),
    )
    _register_openapi_components(app, registry)
    return TestClient(app)


def test_a_created_result_without_location_sends_no_location() -> None:
    response = _client(Orders).put("/orders/fresh-1")

    assert response.status_code == 201
    assert "Location" not in response.headers


@pytest.mark.parametrize(
    ("interface", "url", "status"),
    [(Orders, "/orders/old-1", 200), (AcceptedOrders, "/accepted/old-1", 202)],
)
def test_any_other_result_answers_the_route_status(
    interface: type[RestInterface[Any]], url: str, status: int
) -> None:
    response = _client(interface).put(url)

    assert (response.status_code, response.json()) == (status, {"order_id": "old-1"})


def test_openapi_documents_both_statuses_with_the_same_schema() -> None:
    responses = (
        _client(Orders)
        .get("/openapi.json")
        .json()["paths"]["/orders/{order_id}"]["put"]["responses"]
    )

    expected = {"$ref": "#/components/schemas/OrderView"}
    assert responses["200"]["content"]["application/json"]["schema"] == expected
    assert responses["201"]["content"]["application/json"]["schema"] == expected
    assert responses["201"]["headers"] == {
        "Location": {
            "description": "URI of the created resource, when the use case gives one.",
            "schema": {"type": "string", "format": "uri-reference"},
        }
    }
    assert "headers" not in responses["200"]


def test_openapi_of_a_route_without_created_is_unchanged() -> None:
    responses = (
        _client(Orders)
        .get("/openapi.json")
        .json()["paths"]["/orders/{order_id}"]["get"]["responses"]
    )

    assert "201" not in responses
    assert responses["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/OrderView"
    }


@pytest.mark.parametrize("path", ["/undocumented/anything", "/undocumented/opaque"])
def test_a_result_without_json_schema_documents_no_success_body(path: str) -> None:
    responses = _client(Undocumented).get("/openapi.json").json()["paths"][path]["get"]["responses"]

    assert set(responses) == {"200"}
