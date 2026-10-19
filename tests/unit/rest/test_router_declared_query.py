"""A ``Query()`` route accepts, validates and publishes exactly its declared parameters."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any
from uuid import UUID

import msgspec
import pydantic
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic.alias_generators import to_camel

from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case import Query
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.fastapi.app import _register_openapi_components
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestInterface, RestRoute

_MEMBERSHIP = "12345678-1234-5678-1234-567812345678"


class GrantState(StrEnum):
    LIVE = "live"
    REVOKED = "revoked"


class GrantQuery(
    msgspec.Struct, frozen=True, kw_only=True, rename="camel", forbid_unknown_fields=True
):
    membership_id: UUID | None = None
    state: GrantState | None = None
    changed_since: Annotated[datetime, msgspec.Meta(tz=True)] | None = msgspec.field(
        default=None, name="changed_since"
    )
    cursor: str | None = None
    limit: Annotated[int, msgspec.Meta(ge=1, le=200, description="Rows per page.")] = 50
    flag: bool = False


class ListGrants(UseCase[Any, GrantQuery]):
    async def execute(self, query: GrantQuery = Query()) -> GrantQuery:
        return query


class ModelGrantQuery(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid", alias_generator=to_camel)

    membership_id: UUID | None = None
    state: GrantState | None = None
    changed_since: pydantic.AwareDatetime | None = pydantic.Field(
        default=None, alias="changed_since"
    )
    limit: Annotated[int, pydantic.Field(ge=1, le=200)] = 50
    flag: bool = False


class ListGrantsFromModel(UseCase[Any, list[Any]]):
    async def execute(self, query: ModelGrantQuery = Query()) -> list[Any]:
        return [query.state, query.limit]


class ListGrantsByProfile(UseCase[Any, list[Any]]):
    async def execute(self, profile: str, query: GrantQuery = Query()) -> list[Any]:
        return [profile, query.limit]


class Grants(RestInterface[Any]):
    prefix = "/grants"
    routes = (
        RestRoute(use_case=ListGrants, method="GET", path=""),
        RestRoute(use_case=ListGrantsFromModel, method="GET", path="/from-model"),
        RestRoute(
            use_case=ListGrantsByProfile,
            method="GET",
            path="/by-profile",
            expose_profile=True,
            allowed_profiles=("default", "detail"),
        ),
    )


class _RecordingFactory(UseCaseFactory):
    def __init__(self) -> None:
        super().__init__(LoomContainer())
        self.built: list[type[Any]] = []

    def build(self, use_case_type: type[Any]) -> Any:
        self.built.append(use_case_type)
        return super().build(use_case_type)


@pytest.fixture
def factory() -> _RecordingFactory:
    return _RecordingFactory()


@pytest.fixture
def client(factory: _RecordingFactory) -> TestClient:
    compiler = UseCaseCompiler()
    for route in Grants.routes:
        compiler.compile(route.use_case)
        factory.register(route.use_case)
    app = FastAPI()
    register_error_handlers(app)
    registry = bind_interfaces(
        app,
        RestInterfaceCompiler(compiler).compile(Grants),
        factory,
        RuntimeExecutor(compiler),
        ObservabilityRuntime.noop(),
    )
    _register_openapi_components(app, registry)
    return TestClient(app)


def test_declared_values_reach_execute_typed(client: TestClient) -> None:
    response = client.get(
        "/grants",
        params={
            "membershipId": _MEMBERSHIP,
            "state": "live",
            "changed_since": "2026-09-15T10:00:00+02:00",
            "cursor": "abc",
            "limit": "7",
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "membershipId": _MEMBERSHIP,
        "state": "live",
        "changed_since": "2026-09-15T10:00:00+02:00",
        "cursor": "abc",
        "limit": 7,
        "flag": False,
    }


def test_defaults_apply_when_absent(client: TestClient) -> None:
    response = client.get("/grants")

    assert response.json() == {
        "membershipId": None,
        "state": None,
        "changed_since": None,
        "cursor": None,
        "limit": 50,
        "flag": False,
    }


@pytest.mark.parametrize(
    "query", ["changed_sinse=2026-01-01T00:00:00Z", "membership_id=x", "limit=5&limit=6"]
)
def test_an_unknown_or_repeated_parameter_is_unsupported(client: TestClient, query: str) -> None:
    response = client.get(f"/grants?{query}")

    detail = response.json()["detail"]
    assert (response.status_code, detail["code"]) == (400, "unsupported_query")
    assert detail["message"] == "The request has a query parameter this route does not accept."


def test_an_exposed_profile_is_accepted_next_to_the_declared_parameters(
    client: TestClient,
) -> None:
    response = client.get("/grants/by-profile?profile=detail&limit=5")

    assert (response.status_code, response.json()) == (200, ["detail", 5])


@pytest.mark.parametrize(
    ("query", "field"),
    [
        ("limit=500", "limit"),
        ("state=gone", "state"),
        ("membershipId=not-a-uuid", "membershipId"),
        ("changed_since=2026-09-15T10:00:00", "changed_since"),
    ],
)
def test_a_bad_value_is_a_query_validation_error(
    client: TestClient, query: str, field: str
) -> None:
    response = client.get(f"/grants?{query}")

    detail = response.json()["detail"]
    assert (response.status_code, detail["code"]) == (400, "query_validation")
    assert [violation["field"] for violation in detail["violations"]] == [field]


def test_openapi_publishes_exactly_the_declared_parameters(client: TestClient) -> None:
    document = client.get("/openapi.json").json()

    parameters = document["paths"]["/grants"]["get"]["parameters"]
    assert parameters == [
        {
            "name": "membershipId",
            "in": "query",
            "required": False,
            "schema": {"type": "string", "format": "uuid"},
        },
        {
            "name": "state",
            "in": "query",
            "required": False,
            "schema": {"$ref": "#/components/schemas/GrantState"},
        },
        {
            "name": "changed_since",
            "in": "query",
            "required": False,
            "schema": {"type": "string", "format": "date-time"},
        },
        {"name": "cursor", "in": "query", "required": False, "schema": {"type": "string"}},
        {
            "name": "limit",
            "in": "query",
            "required": False,
            "schema": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50},
            "description": "Rows per page.",
        },
        {
            "name": "flag",
            "in": "query",
            "required": False,
            "schema": {"type": "boolean", "default": False},
        },
    ]
    assert document["components"]["schemas"]["GrantState"]["enum"] == ["live", "revoked"]


def test_a_pydantic_query_is_accepted_validated_and_published(client: TestClient) -> None:
    accepted = client.get("/grants/from-model?state=revoked&limit=3")
    refused = client.get("/grants/from-model?limit=0")
    unknown = client.get("/grants/from-model?page=2")
    operation = client.get("/openapi.json").json()["paths"]["/grants/from-model"]["get"]

    assert (accepted.status_code, accepted.json()) == (200, ["revoked", 3])
    assert (refused.status_code, refused.json()["detail"]["code"]) == (400, "query_validation")
    assert (unknown.status_code, unknown.json()["detail"]["code"]) == (400, "unsupported_query")
    assert all("title" not in parameter["schema"] for parameter in operation["parameters"])
    assert [parameter["name"] for parameter in operation["parameters"]] == [
        "membershipId",
        "state",
        "changed_since",
        "limit",
        "flag",
    ]


@pytest.mark.parametrize("path", ["/grants", "/grants/from-model"])
@pytest.mark.parametrize(
    "query",
    [
        "limit=5.0",
        "limit=1e2",
        "changed_since=1700000000",
        "changed_since=null",
        "membershipId=null",
        "flag=1",
    ],
)
def test_a_value_not_spelled_exactly_is_refused(client: TestClient, path: str, query: str) -> None:
    response = client.get(f"{path}?{query}")

    assert (response.status_code, response.json()["detail"]["code"]) == (
        400,
        "query_validation",
    )


@pytest.mark.parametrize("path", ["/grants", "/grants/from-model"])
@pytest.mark.parametrize(
    "since", ["2026-09-15T10:00:00Z", "2026-09-15T10:00:00+05:30", "2026-09-15T10:00:00-03:00"]
)
def test_exact_values_are_accepted(client: TestClient, path: str, since: str) -> None:
    response = client.get(
        path, params={"limit": "50", "membershipId": _MEMBERSHIP, "changed_since": since}
    )

    assert response.status_code == 200


@pytest.mark.parametrize(
    "url", ["/grants?profile=detail", "/grants/by-profile?profile=detail&profile=default"]
)
def test_a_profile_the_route_does_not_expose_or_repeats_is_unsupported(
    client: TestClient, url: str
) -> None:
    response = client.get(url)

    assert (response.status_code, response.json()["detail"]["code"]) == (
        400,
        "unsupported_query",
    )


@pytest.mark.parametrize("query", ["page=2", "limit=500"])
def test_a_refused_query_does_not_build_the_use_case(
    client: TestClient, factory: _RecordingFactory, query: str
) -> None:
    response = client.get(f"/grants?{query}")

    assert response.status_code == 400
    assert factory.built == []


@pytest.mark.parametrize("path", ["/grants", "/grants/from-model"])
def test_a_bad_enum_value_is_not_echoed(client: TestClient, path: str) -> None:
    response = client.get(path, params={"state": "secret-input-value"})

    assert (response.status_code, response.json()["detail"]["code"]) == (400, "query_validation")
    assert "secret-input-value" not in response.text


@pytest.mark.parametrize("path", ["/grants", "/grants/from-model"])
@pytest.mark.parametrize("limit", [str(2**64), "9" * 5000])
def test_a_huge_integer_is_a_query_validation_error(
    client: TestClient, path: str, limit: str
) -> None:
    response = client.get(path, params={"limit": limit})

    detail = response.json()["detail"]
    assert (response.status_code, detail["code"]) == (400, "query_validation")
    assert [violation["field"] for violation in detail["violations"]] == ["limit"]
    assert "sys." not in response.text
