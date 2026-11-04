"""``LambdaTestClient`` calls a real Lambda HTTP handler with API Gateway events, in process."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from loom.composition import create_app
from loom.serverless.aws.lambda_ import LambdaHTTPAdapter, LambdaHTTPHandler
from loom.testing import LambdaTestClient
from loom.testing.lambda_http import EventFormat
from tests.unit.serverless.aws.lambda_.http._contract_app import bearer, write_contract_project

pytestmark = pytest.mark.timeout(60)


@pytest.fixture(scope="module")
def config(tmp_path_factory: pytest.TempPathFactory) -> str:
    functions: dict[str, Any] = {
        "functions": {
            "items": {"http": ["ItemInterface"]},
            "ops": {"http": ["OpsInterface", "PublicInterface"]},
        }
    }
    return write_contract_project(tmp_path_factory.mktemp("lambda-client"), **{"lambda": functions})


@pytest.fixture(scope="module")
def handler(config: str) -> Iterator[LambdaHTTPHandler]:
    served = create_app(config, adapter=LambdaHTTPAdapter(), environment="test")
    yield served
    served.close()


@pytest.fixture(params=["v2", "v1"])
def client(request: pytest.FixtureRequest, handler: LambdaHTTPHandler) -> LambdaTestClient:
    event_format: EventFormat = request.param
    return LambdaTestClient(handler, event_format=event_format)


@pytest.fixture
def alice() -> dict[str, str]:
    return bearer("alice")


def test_a_get_answers_the_use_case_result_as_json(
    client: LambdaTestClient, alice: dict[str, str]
) -> None:
    response = client.get("/items/7", headers=alice)

    assert (response.status_code, response.json()) == (200, {"id": 7, "type": "int"})
    assert response.headers["Content-Type"] == "application/json"
    assert response.text == '{"id":7,"type":"int"}'


def test_a_json_body_reaches_the_use_case(client: LambdaTestClient, alice: dict[str, str]) -> None:
    response = client.post("/items/", headers=alice, json={"name": "lamp"})

    assert (response.status_code, response.json()) == (200, {"name": "lamp"})


@pytest.mark.parametrize("url", ["/items/7?profile=full", "/items/7"])
def test_query_parameters_reach_the_route(
    client: LambdaTestClient, alice: dict[str, str], url: str
) -> None:
    response = client.get(url, headers=alice, params={"profile": "full"})

    assert (response.status_code, response.json()["detail"]["code"]) == (400, "bad_request")


def test_a_request_without_a_token_answers_401_with_a_challenge(client: LambdaTestClient) -> None:
    response = client.get("/items/7")

    assert (response.status_code, response.json()["detail"]["code"]) == (401, "unauthenticated")
    assert response.headers["www-authenticate"] == "Bearer"


def test_a_caller_without_the_route_role_answers_403(
    client: LambdaTestClient, alice: dict[str, str]
) -> None:
    response = client.get("/ops/admin", headers=alice)

    assert (response.status_code, response.json()["detail"]["code"]) == (403, "forbidden")


def test_a_client_built_from_the_configuration_serves_only_its_function(config: str) -> None:
    alice = bearer("alice")
    with LambdaTestClient(config, function="items", environment="test") as client:
        served = client.get("/items/7", headers=alice).status_code
        unserved = client.get("/ops/admin", headers=alice).status_code

        assert client.application is not None
    assert (served, unserved) == (200, 404)


def test_a_handler_takes_no_create_app_options(handler: LambdaHTTPHandler) -> None:
    with pytest.raises(TypeError, match="need a configuration path"):
        LambdaTestClient(handler, function="items")


def test_closing_a_client_leaves_a_handler_passed_in_open(
    handler: LambdaHTTPHandler, alice: dict[str, str]
) -> None:
    LambdaTestClient(handler).close()

    assert LambdaTestClient(handler).get("/items/7", headers=alice).status_code == 200
