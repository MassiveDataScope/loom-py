"""Drive FastAPI and Lambda through one client interface, comparing what a caller sees."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, NamedTuple, Protocol

from fastapi.testclient import TestClient

from loom.aws_lambda import LambdaHTTPHandler
from tests.unit.aws_lambda.http._contract_app import http_api_event

TRACE_ID = "contract-trace-0001"
"""Sent on every request, so bodies carrying a trace id compare exactly."""

LAMBDA_CONTEXT = SimpleNamespace(function_name="contract", aws_request_id="contract-request")


class LambdaProject(NamedTuple):
    """A contract project's configuration path and the Lambda handler serving it."""

    config: str
    handler: LambdaHTTPHandler


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
