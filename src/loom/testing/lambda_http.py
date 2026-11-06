"""Call a Lambda HTTP function in process, as API Gateway would, with an ``httpx`` client.

:class:`LambdaTestClient` is an ``httpx.Client`` whose transport turns each
request into an API Gateway event, HTTP API payload 2.0 by default or REST API
(``event_format="v1"``), and answers with what the real handler returned:
Mangum, the middleware and the authentication all run. Needs the ``lambda``
and ``testing`` extras.

Usage::

    with LambdaTestClient("config.yaml", function="orders", environment="test") as client:
        response = client.get("/orders/7", headers={"authorization": "Bearer ..."})
        assert response.status_code == 200
"""

from __future__ import annotations

import base64
from collections.abc import Callable
from typing import Any, cast

import httpx

from loom.composition import Application, build_application
from loom.serverless.aws.lambda_.handler import LambdaAdapter, LambdaHandler
from loom.testing.serverless import EventFormat, LambdaEvent

HttpHandler = Callable[[dict[str, Any], Any], dict[str, Any]]


class LambdaTestClient(httpx.Client):
    """An ``httpx.Client`` served by a Lambda HTTP function, like Starlette's ``TestClient``.

    Args:
        app: The handler ``create_app`` returned, which the caller closes; or a
            configuration path, composed here as ``create_app`` would with
            ``LambdaAdapter(function=function)`` and closed with the client.
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
        app: HttpHandler | str,
        *,
        function: str | None = None,
        event_format: EventFormat = "v2",
        **options: Any,
    ) -> None:
        self.application: Application | None = None
        owned: LambdaHandler | None = None
        if isinstance(app, str):
            adapter = LambdaAdapter(function=function)
            self.application = build_application(app, adapter=adapter, **options)
            owned = adapter.bind(self.application)
            app = cast(HttpHandler, owned)
        elif function is not None or options:
            raise TypeError("function and create_app options need a configuration path")
        transport = _LambdaTransport(app, event_format, owned)
        super().__init__(transport=transport, base_url="http://testserver")


class _LambdaTransport(httpx.BaseTransport):
    """Answers each request with *handler*; closing it closes *owned*, the handler composed."""

    def __init__(
        self, handler: HttpHandler, event_format: EventFormat, owned: LambdaHandler | None
    ) -> None:
        self._handler = handler
        self._event_format: EventFormat = event_format
        self._owned = owned

    def close(self) -> None:
        if self._owned is not None:
            self._owned.close()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        result = self._handler(_event(request, self._event_format), None)
        headers = [*(result.get("headers") or {}).items()]
        for name, values in (result.get("multiValueHeaders") or {}).items():
            headers.extend((name, value) for value in values)
        headers.extend(("set-cookie", cookie) for cookie in result.get("cookies") or ())
        body: str = result.get("body") or ""
        content = base64.b64decode(body) if result.get("isBase64Encoded") else body.encode()
        return httpx.Response(result["statusCode"], headers=headers, content=content)


def _event(request: httpx.Request, event_format: EventFormat) -> dict[str, Any]:
    """The API Gateway event of *request*."""
    headers: dict[str, str] = {}
    for name, value in request.headers.multi_items():
        headers[name] = f"{headers[name]},{value}" if name in headers else value
    return LambdaEvent.http(
        request.method,
        request.url.path,
        query=request.url.query.decode(),
        headers=headers,
        body=request.read(),
        event_format=event_format,
    )
