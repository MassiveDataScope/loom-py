"""``LambdaTestClient`` calls a real Lambda HTTP handler with API Gateway events, in process."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from loom.composition import create_app
from loom.serverless.aws.lambda_ import LambdaRuntime
from loom.serverless.aws.lambda_.handler import LambdaHandler
from loom.testing import LambdaTestClient
from loom.testing.lambda_http import EventFormat
from tests.unit.serverless.aws.lambda_.http._contract_app import bearer, write_contract_project

pytestmark = pytest.mark.timeout(60)


@pytest.fixture(scope="module")
def config(tmp_path_factory: pytest.TempPathFactory) -> str:
    functions: dict[str, Any] = {
        "functions": {
            "items": {"trigger": {"source": "http", "interfaces": ["ItemInterface"]}},
            "ops": {
                "trigger": {"source": "http", "interfaces": ["OpsInterface", "PublicInterface"]}
            },
        }
    }
    return write_contract_project(tmp_path_factory.mktemp("lambda-client"), **{"lambda": functions})


@pytest.fixture(scope="module")
def handler(config: str) -> Iterator[LambdaHandler]:
    served = create_app(config, runtime=LambdaRuntime(), environment="test")
    yield served
    served.close()


@pytest.fixture(params=["v2", "v1"])
def client(request: pytest.FixtureRequest, handler: LambdaHandler) -> LambdaTestClient:
    event_format: EventFormat = request.param
    return LambdaTestClient(handler, event_format=event_format)


@pytest.fixture
def alice() -> dict[str, str]:
    return bearer("alice")


class _Spy:
    """Passes each event on to the real handler, keeping it."""

    def __init__(self, handler: LambdaHandler) -> None:
        self.handler = handler
        self.events: list[dict[str, Any]] = []

    def __call__(self, event: dict[str, Any], context: Any) -> dict[str, Any]:
        self.events.append(event)
        return self.handler(event, context)


class TestLambdaTestClient:
    def test_a_get_answers_the_use_case_result_as_json(
        self, client: LambdaTestClient, alice: dict[str, str]
    ) -> None:
        response = client.get("/items/7", headers=alice)

        assert (response.status_code, response.json()) == (200, {"id": 7, "type": "int"})
        assert response.headers["Content-Type"] == "application/json"
        assert response.text == '{"id":7,"type":"int"}'

    def test_a_json_body_reaches_the_use_case(
        self, client: LambdaTestClient, alice: dict[str, str]
    ) -> None:
        response = client.post("/items/", headers=alice, json={"name": "lamp"})

        assert (response.status_code, response.json()) == (200, {"name": "lamp"})

    @pytest.mark.parametrize("url", ["/items/7?profile=full", "/items/7"])
    def test_query_parameters_reach_the_route(
        self, client: LambdaTestClient, alice: dict[str, str], url: str
    ) -> None:
        response = client.get(url, headers=alice, params={"profile": "full"})

        assert (response.status_code, response.json()["detail"]["code"]) == (400, "bad_request")

    def test_the_function_sees_the_decoded_path(self, client: LambdaTestClient) -> None:
        assert client.post("/public/who%61mi").json() == {"subject": ""}

    @pytest.mark.parametrize(
        ("event_format", "path", "query"),
        [
            ("v2", ("requestContext", "http", "path"), ("rawQueryString", "tag=a&tag=b")),
            ("v1", ("path",), ("multiValueQueryStringParameters", {"tag": ["a", "b"]})),
        ],
    )
    def test_the_event_carries_a_path_with_a_space_and_repeated_parameters(
        self,
        handler: LambdaHandler,
        alice: dict[str, str],
        event_format: EventFormat,
        path: tuple[str, ...],
        query: tuple[str, Any],
    ) -> None:
        spy = _Spy(handler)

        response = LambdaTestClient(spy, event_format=event_format).get(
            "/items/a b", headers=alice, params=[("tag", "a"), ("tag", "b")]
        )

        (event,) = spy.events
        for key in path:
            event = event[key]
        assert (event, spy.events[0][query[0]], response.status_code) == (
            "/items/a b",
            query[1],
            422,
        )

    def test_a_request_without_a_token_answers_401_with_a_challenge(
        self, client: LambdaTestClient
    ) -> None:
        response = client.get("/items/7")

        assert (response.status_code, response.json()["detail"]["code"]) == (401, "unauthenticated")
        assert response.headers["www-authenticate"] == "Bearer"

    def test_a_caller_without_the_route_role_answers_403(
        self, client: LambdaTestClient, alice: dict[str, str]
    ) -> None:
        response = client.get("/ops/admin", headers=alice)

        assert (response.status_code, response.json()["detail"]["code"]) == (403, "forbidden")

    def test_a_client_built_from_the_configuration_serves_only_its_function(
        self, config: str
    ) -> None:
        alice = bearer("alice")
        with LambdaTestClient(config, function="items", environment="test") as client:
            served = client.get("/items/7", headers=alice).status_code
            unserved = client.get("/ops/admin", headers=alice).status_code

            assert client.application is not None
        assert (served, unserved) == (200, 404)

    @pytest.mark.parametrize("source", ["variable", "local"])
    def test_a_client_without_a_path_loads_the_default_config_directory(
        self, config: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source: str
    ) -> None:
        directory = tmp_path / ("elsewhere" if source == "variable" else "config")
        shutil.copytree(Path(config).parent, directory)
        (directory / "base.yaml").write_bytes(Path(config).read_bytes())
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("LOOM_CONFIG_DIR", raising=False)
        if source == "variable":
            monkeypatch.setenv("LOOM_CONFIG_DIR", str(directory))
        with LambdaTestClient(function="items", environment="test") as client:
            served = client.get("/items/7", headers=bearer("alice")).status_code

        assert served == 200

    def test_a_handler_takes_no_create_app_options(self, handler: LambdaHandler) -> None:
        with pytest.raises(TypeError, match="need a configuration path"):
            LambdaTestClient(handler, function="items")

    def test_closing_a_client_leaves_a_handler_passed_in_open(
        self, handler: LambdaHandler, alice: dict[str, str]
    ) -> None:
        LambdaTestClient(handler).close()

        assert LambdaTestClient(handler).get("/items/7", headers=alice).status_code == 200
