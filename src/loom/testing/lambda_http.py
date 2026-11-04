"""Call a Lambda HTTP function in process, as API Gateway would, with a test client's API.

:class:`LambdaTestClient` turns ``get``/``post``/... calls into API Gateway
events, HTTP API payload 2.0 by default or REST API (``event_format="v1"``),
and answers with what the real handler returned: Mangum, the middleware and
the authentication all run. Needs the ``lambda`` extra.

Usage::

    with LambdaTestClient("config.yaml", function="orders", environment="test") as client:
        response = client.get("/orders/7", headers={"authorization": "Bearer ..."})
        assert response.status_code == 200
"""

from __future__ import annotations

import base64
import json as jsonlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Literal
from urllib.parse import parse_qsl, urlencode, urlsplit

from starlette.datastructures import Headers

from loom.composition import Application, build_application
from loom.serverless.aws.lambda_.http import LambdaHTTPAdapter, LambdaHTTPHandler

EventFormat = Literal["v1", "v2"]
LambdaHandler = Callable[[dict[str, Any], Any], dict[str, Any]]

_HOST = "testserver"
_SOURCE_IP = "127.0.0.1"


@dataclass(frozen=True)
class LambdaResponse:
    """What the function answered: status, headers (case-insensitive) and body."""

    status_code: int
    headers: Headers
    content: bytes

    @property
    def text(self) -> str:
        """The body decoded as UTF-8."""
        return self.content.decode()

    def json(self) -> Any:
        """The body decoded as JSON."""
        return jsonlib.loads(self.content)


class LambdaTestClient:
    """Drive a Lambda HTTP function with ``starlette.testclient.TestClient``'s calls.

    Args:
        app: The handler ``create_app`` returned, which the caller closes; or a
            configuration path, composed here as ``create_app`` would with
            ``LambdaHTTPAdapter(function=function)`` and closed by :meth:`close`.
        function: The function of ``lambda.functions`` to serve; with a
            configuration path only.
        event_format: ``"v2"`` for HTTP API events, ``"v1"`` for REST API ones.
        **options: Keyword arguments of ``create_app`` (``environment``,
            ``modules``, ...); with a configuration path only.

    Attributes:
        application: The application composed from a configuration path, to
            reach its container; ``None`` for a handler built elsewhere.

    Raises:
        TypeError: When *function* or *options* come with a handler.
    """

    def __init__(
        self,
        app: LambdaHandler | str,
        *,
        function: str | None = None,
        event_format: EventFormat = "v2",
        **options: Any,
    ) -> None:
        self._event_format = event_format
        self.application: Application | None = None
        self._owned: LambdaHTTPHandler | None = None
        if not isinstance(app, str):
            if function is not None or options:
                raise TypeError("function and create_app options need a configuration path")
            self._handler = app
            return
        adapter = LambdaHTTPAdapter(function=function)
        self.application = build_application(app, adapter=adapter, **options)
        self._handler = self._owned = adapter.bind(self.application)

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        json: Any = None,
        params: Mapping[str, str] | None = None,
        content: bytes | str | None = None,
    ) -> LambdaResponse:
        """Send one request to the function and return its response."""
        sent = {name.lower(): value for name, value in (headers or {}).items()}
        body = content.encode() if isinstance(content, str) else content or b""
        if json is not None:
            body = jsonlib.dumps(json).encode()
            sent.setdefault("content-type", "application/json")
        target = urlsplit(url)
        query = [*parse_qsl(target.query, keep_blank_values=True), *(params or {}).items()]
        build = _rest_event if self._event_format == "v1" else _http_api_event
        event = build(method.upper(), target.path or "/", query, sent, body)
        return _response(self._handler(event, None))

    def get(self, url: str, **kwargs: Any) -> LambdaResponse:
        """Send a ``GET``; *kwargs* as :meth:`request`."""
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> LambdaResponse:
        """Send a ``POST``; *kwargs* as :meth:`request`."""
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> LambdaResponse:
        """Send a ``PUT``; *kwargs* as :meth:`request`."""
        return self.request("PUT", url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> LambdaResponse:
        """Send a ``PATCH``; *kwargs* as :meth:`request`."""
        return self.request("PATCH", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> LambdaResponse:
        """Send a ``DELETE``; *kwargs* as :meth:`request`."""
        return self.request("DELETE", url, **kwargs)

    def close(self) -> None:
        """Close the handler this client composed; a handler passed in stays open."""
        if self._owned is not None:
            self._owned.close()

    def __enter__(self) -> LambdaTestClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()


def _with_length(headers: dict[str, str], body: bytes) -> dict[str, str]:
    """API Gateway forwards the ``content-length`` of a request with a body."""
    length = {"content-length": str(len(body))} if body else {}
    return {"host": _HOST, **length, **headers}


def _http_api_event(
    method: str, path: str, query: list[tuple[str, str]], headers: dict[str, str], body: bytes
) -> dict[str, Any]:
    return {
        "version": "2.0",
        "routeKey": "$default",
        "rawPath": path,
        "rawQueryString": urlencode(query),
        "headers": _with_length(headers, body),
        "requestContext": {
            "domainName": _HOST,
            "http": {
                "method": method,
                "path": path,
                "protocol": "HTTP/1.1",
                "sourceIp": _SOURCE_IP,
            },
            "requestId": "test-request",
            "routeKey": "$default",
            "stage": "$default",
        },
        "body": base64.b64encode(body).decode("ascii"),
        "isBase64Encoded": True,
    }


def _rest_event(
    method: str, path: str, query: list[tuple[str, str]], headers: dict[str, str], body: bytes
) -> dict[str, Any]:
    """A REST API event: ``resource`` and no ``version`` key, as Mangum tells it apart."""
    multi: dict[str, list[str]] = {}
    for name, value in query:
        multi.setdefault(name, []).append(value)
    return {
        "resource": "/{proxy+}",
        "path": path,
        "httpMethod": method,
        "headers": _with_length(headers, body),
        "queryStringParameters": {name: values[-1] for name, values in multi.items()} or None,
        "multiValueQueryStringParameters": multi or None,
        "requestContext": {
            "httpMethod": method,
            "identity": {"sourceIp": _SOURCE_IP},
            "path": path,
            "requestId": "test-request",
            "stage": "test",
        },
        "body": base64.b64encode(body).decode("ascii"),
        "isBase64Encoded": True,
    }


def _response(result: Mapping[str, Any]) -> LambdaResponse:
    raw = [(name, value) for name, value in (result.get("headers") or {}).items()]
    for name, values in (result.get("multiValueHeaders") or {}).items():
        raw.extend((name, value) for value in values)
    raw.extend(("set-cookie", cookie) for cookie in result.get("cookies") or ())
    body: str = result.get("body") or ""
    return LambdaResponse(
        status_code=result["statusCode"],
        headers=Headers(raw=[(name.lower().encode(), value.encode()) for name, value in raw]),
        content=base64.b64decode(body) if result.get("isBase64Encoded") else body.encode(),
    )
