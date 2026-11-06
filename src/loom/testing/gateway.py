"""The API Gateway event of one HTTP request, in the HTTP API (v2) or REST (v1) format."""

from __future__ import annotations

import base64
from collections.abc import Mapping
from typing import Any, Literal
from urllib.parse import parse_qsl

__all__ = ["EventFormat", "api_gateway_event"]

EventFormat = Literal["v1", "v2"]


def api_gateway_event(
    method: str,
    path: str,
    query: str,
    headers: Mapping[str, str],
    body: bytes,
    event_format: EventFormat,
) -> dict[str, Any]:
    """The API Gateway event of one request; *path* decoded, as API Gateway passes it."""
    event: dict[str, Any] = {
        "headers": dict(headers),
        "body": base64.b64encode(body).decode("ascii"),
        "isBase64Encoded": True,
    }
    if event_format == "v2":
        http = {"method": method, "path": path, "protocol": "HTTP/1.1", "sourceIp": "127.0.0.1"}
        return event | {
            "version": "2.0",
            "routeKey": "$default",
            "rawPath": path,
            "rawQueryString": query,
            "requestContext": {"http": http, "requestId": "test", "stage": "$default"},
        }
    multi: dict[str, list[str]] = {}
    for name, value in parse_qsl(query, keep_blank_values=True):
        multi.setdefault(name, []).append(value)
    # A REST event has "resource" and no "version" key: that is how Mangum tells them apart.
    return event | {
        "resource": "/{proxy+}",
        "path": path,
        "httpMethod": method,
        "multiValueQueryStringParameters": multi or None,
        "requestContext": {"identity": {"sourceIp": "127.0.0.1"}, "requestId": "test"},
    }
