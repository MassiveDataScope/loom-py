"""The success status a REST route answers follows the use case's result."""

from __future__ import annotations

from typing import Any

import msgspec
import pydantic
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import RateLimited
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case import Created
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi._errors import register_error_handlers
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


class RevokeGrant(UseCase[Any, GrantView]):
    async def execute(self, grant_id: str) -> GrantView:
        raise RateLimited(1.2)


class ModelGrantView(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    grant_id: str


class CreateModelGrant(UseCase[Any, ModelGrantView | Created[ModelGrantView]]):
    async def execute(self, grant_id: str) -> ModelGrantView | Created[ModelGrantView]:
        return Created(ModelGrantView(grant_id=grant_id))


class ModelGrants(RestInterface[Any]):
    prefix = "/model-grants"
    routes = (RestRoute(use_case=CreateModelGrant, method="PUT", path="/{grant_id}"),)


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


class Grants(RestInterface[Any]):
    prefix = "/grants"
    routes = (
        RestRoute(use_case=CreateGrant, method="PUT", path="/{grant_id}"),
        RestRoute(use_case=ReadGrant, method="GET", path="/{grant_id}"),
        RestRoute(use_case=RevokeGrant, method="DELETE", path="/{grant_id}"),
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


def test_a_rate_limited_use_case_answers_429_with_retry_after() -> None:
    response = _client(Grants).delete("/grants/old-1")

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "2"
    assert response.json() == {
        "detail": {"code": "rate_limited", "message": "Too many requests.", "trace_id": None}
    }


@pytest.mark.parametrize("path", ["/undocumented/anything", "/undocumented/opaque"])
def test_a_result_without_json_schema_documents_no_success_body(path: str) -> None:
    responses = _client(Undocumented).get("/openapi.json").json()["paths"][path]["get"]["responses"]

    assert set(responses) == {"200"}
    assert responses["200"]["content"]["application/json"]["schema"] == {}


def test_a_pydantic_result_is_one_component_shared_by_both_statuses() -> None:
    document = _client(ModelGrants).get("/openapi.json").json()
    responses = document["paths"]["/model-grants/{grant_id}"]["put"]["responses"]

    expected = {"$ref": "#/components/schemas/ModelGrantView"}
    assert responses["200"]["content"]["application/json"]["schema"] == expected
    assert responses["201"]["content"]["application/json"]["schema"] == expected
    assert document["components"]["schemas"]["ModelGrantView"]["properties"] == {
        "grant_id": {"title": "Grant Id", "type": "string"}
    }
