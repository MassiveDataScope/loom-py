"""One contract application served by FastAPI and by Lambda, behind the same client interface."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Protocol

import pytest
from fastapi.testclient import TestClient

from loom.aws_lambda import LambdaHTTPAdapter, LambdaHTTPHandler
from loom.composition import create_app
from loom.rest.fastapi.adapter import FastAPIAdapter
from tests.helpers.composition import application_openings
from tests.unit.aws_lambda.http._contract_app import http_api_event, write_contract_project

__all__ = ["application_openings"]

TRACE_ID = "contract-trace-0001"
"""Sent on every request, so bodies carrying a trace id compare exactly."""

LAMBDA_CONTEXT = SimpleNamespace(function_name="contract", aws_request_id="contract-request")


@dataclass(frozen=True)
class Reply:
    """What a caller sees: status, headers by lowercase name, and body."""

    status: int
    headers: Mapping[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)


class Client(Protocol):
    def __call__(
        self,
        method: str,
        path: str,
        *,
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
    ) -> Reply: ...


def _with_trace(headers: Mapping[str, str] | None) -> dict[str, str]:
    return {"x-request-id": TRACE_ID, **(headers or {})}


def fastapi_client(test_client: TestClient) -> Client:
    """Drive a FastAPI application through its test client."""

    def call(
        method: str, path: str, *, headers: Mapping[str, str] | None = None, body: bytes = b""
    ) -> Reply:
        response = test_client.request(method, path, headers=_with_trace(headers), content=body)
        return Reply(
            response.status_code,
            {name.lower(): value for name, value in response.headers.items()},
            response.content,
        )

    return call


def lambda_client(handler: LambdaHTTPHandler) -> Client:
    """Drive a Lambda handler with API Gateway HTTP API events."""

    def call(
        method: str, path: str, *, headers: Mapping[str, str] | None = None, body: bytes = b""
    ) -> Reply:
        result = handler(http_api_event(method, path, _with_trace(headers), body), LAMBDA_CONTEXT)
        raw = result["body"]
        payload = base64.b64decode(raw) if result["isBase64Encoded"] else raw.encode()
        return Reply(
            result["statusCode"],
            {name.lower(): value for name, value in result["headers"].items()},
            payload,
        )

    return call


@pytest.fixture(scope="module")
def contract_project(tmp_path_factory: pytest.TempPathFactory) -> Callable[..., str]:
    """Write the contract project under a fresh module name; return its config path."""

    def write(**options: Any) -> str:
        return write_contract_project(tmp_path_factory.mktemp("contract"), **options)

    return write


@pytest.fixture(scope="module")
def fastapi_served(contract_project: Callable[..., str]) -> Iterator[Client]:
    app = create_app(contract_project(), adapter=FastAPIAdapter(), environment="test")
    with TestClient(app) as test_client:
        yield fastapi_client(test_client)


@pytest.fixture(scope="module")
def lambda_served(contract_project: Callable[..., str]) -> Iterator[Client]:
    handler = create_app(contract_project(), adapter=LambdaHTTPAdapter(), environment="test")
    yield lambda_client(handler)
    handler.close()


@pytest.fixture(params=["fastapi_served", "lambda_served"], ids=["fastapi", "lambda"])
def served(request: pytest.FixtureRequest) -> Client:
    """The contract application, served by each adapter in turn."""
    return request.getfixturevalue(request.param)


@pytest.fixture
def lambda_handler(contract_project: Callable[..., str]) -> Iterator[LambdaHTTPHandler]:
    """A Lambda handler of its own, closed after the test."""
    handler = create_app(contract_project(), adapter=LambdaHTTPAdapter(), environment="test")
    yield handler
    handler.close()
