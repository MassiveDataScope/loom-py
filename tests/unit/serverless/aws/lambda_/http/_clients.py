"""Drive FastAPI and Lambda through one client interface, comparing what a caller sees."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, NamedTuple, Protocol

from fastapi.testclient import TestClient

from loom.serverless.aws.lambda_.triggers.http import LambdaHTTPHandler
from loom.testing import LambdaTestClient

TRACE_ID = "contract-trace-0001"
"""Sent on every request, so bodies carrying a trace id compare exactly."""


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


def driven_by(test_client: TestClient | LambdaTestClient) -> Client:
    """Drive an application through a test client: FastAPI's, or loom's for Lambda."""

    def call(
        method: str, path: str, *, headers: Mapping[str, str] | None = None, body: bytes = b""
    ) -> Reply:
        response = test_client.request(method, path, headers=_with_trace(headers), content=body)
        return Reply(response.status_code, dict(response.headers.items()), response.content)

    return call


def lambda_client(handler: LambdaHTTPHandler) -> Client:
    """Drive a Lambda handler with API Gateway HTTP API events."""
    return driven_by(LambdaTestClient(handler))
