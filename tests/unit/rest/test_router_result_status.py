"""The success status a REST route answers follows the use case's result."""

from __future__ import annotations

from typing import Any

import msgspec
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
from loom.rest.fastapi.app import _register_openapi_components
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestInterface, RestRoute


class GrantView(msgspec.Struct, frozen=True):
    grant_id: str


class CreateGrant(UseCase[Any, GrantView | Created[GrantView]]):
    async def execute(self, grant_id: str) -> GrantView | Created[GrantView]:
        view = GrantView(grant_id=grant_id)
        return Created(view) if grant_id.startswith("new") else view


class ReadGrant(UseCase[Any, GrantView]):
    async def execute(self, grant_id: str) -> GrantView:
        return GrantView(grant_id=grant_id)


class Grants(RestInterface[Any]):
    prefix = "/grants"
    routes = (
        RestRoute(use_case=CreateGrant, method="PUT", path="/{grant_id}"),
        RestRoute(use_case=ReadGrant, method="GET", path="/{grant_id}"),
    )


class AcceptedGrants(RestInterface[Any]):
    prefix = "/accepted"
    routes = (RestRoute(use_case=CreateGrant, method="PUT", path="/{grant_id}", status_code=202),)


def _client(interface: type[RestInterface[Any]]) -> TestClient:
    use_cases = [route.use_case for route in interface.routes]
    compiler = UseCaseCompiler()
    factory = UseCaseFactory(LoomContainer())
    for use_case in use_cases:
        compiler.compile(use_case)
        factory.register(use_case)
    app = FastAPI()
    registry = bind_interfaces(
        app,
        RestInterfaceCompiler(compiler).compile(interface),
        factory,
        RuntimeExecutor(compiler),
        ObservabilityRuntime.noop(),
    )
    _register_openapi_components(app, registry)
    return TestClient(app)


def test_a_created_result_answers_201_with_its_value() -> None:
    response = _client(Grants).put("/grants/new-1")

    assert response.status_code == 201
    assert response.json() == {"grant_id": "new-1"}


def test_any_other_result_answers_the_route_status() -> None:
    response = _client(Grants).put("/grants/old-1")

    assert response.status_code == 200
    assert response.json() == {"grant_id": "old-1"}


def test_the_route_status_applies_whatever_it_is() -> None:
    client = _client(AcceptedGrants)

    assert client.put("/accepted/old-1").status_code == 202
    assert client.put("/accepted/new-1").status_code == 201


def test_openapi_documents_both_statuses_with_the_same_schema() -> None:
    responses = (
        _client(Grants)
        .get("/openapi.json")
        .json()["paths"]["/grants/{grant_id}"]["put"]["responses"]
    )

    expected = {"$ref": "#/components/schemas/GrantView"}
    assert responses["200"]["content"]["application/json"]["schema"] == expected
    assert responses["201"]["content"]["application/json"]["schema"] == expected


def test_openapi_of_a_route_without_created_is_unchanged() -> None:
    responses = (
        _client(Grants)
        .get("/openapi.json")
        .json()["paths"]["/grants/{grant_id}"]["get"]["responses"]
    )

    assert "201" not in responses
    assert responses["200"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/GrantView"
    }
