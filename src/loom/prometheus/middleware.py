"""Prometheus HTTP metrics middleware.

Pure ASGI middleware — no FastAPI or Starlette dependency.  Compatible
with any ASGI-compliant framework and server.

Install the optional dependency with::

    pip install "loom-py[prometheus]"

Usage::

    from loom.prometheus import PrometheusMiddleware

    app = create_fastapi_app(
        result,
        RouteSources(python=[OrderInterface]),
        middleware=[TraceIdMiddleware, PrometheusMiddleware],
    )
    # Expose the /metrics scrape endpoint separately as an exact route.
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from loom.prometheus._instruments import cached_instruments, counter_spec, histogram_spec

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]
_ASGIApp = Callable[[_Scope, _Receive, _Send], Awaitable[None]]
_UNMATCHED_PATH_TEMPLATE = "__unmatched__"

_REQUESTS_TOTAL = counter_spec(
    "http_requests",
    "Total HTTP requests by method, path template, and status code.",
    "method",
    "path_template",
    "status_code",
)
_DURATION_SECONDS = histogram_spec(
    "http_request_duration_seconds",
    "HTTP request duration in seconds by method and path template.",
    "method",
    "path_template",
)
_INSTRUMENTS = (_REQUESTS_TOTAL, _DURATION_SECONDS)


class PrometheusMiddleware:
    """Pure ASGI middleware that records HTTP-level Prometheus metrics.

    Instruments every HTTP request with:

    - ``http_requests_total`` — Counter, labels: ``method``,
      ``path_template``, ``status_code``.
    - ``http_request_duration_seconds`` — Histogram, labels: ``method``,
      ``path_template``.

    ``path_template`` uses the route template (e.g. ``/products/{id}``)
    rather than the actual URL (e.g. ``/products/42``) to prevent
    cardinality explosion.  Falls back to the raw path when no route
    template is available.

    Non-HTTP scopes (WebSocket, lifespan) are passed through unchanged.

    **Scrape endpoint:** This middleware does not expose ``/metrics``
    automatically.  Register an exact route in your ASGI app that returns
    ``prometheus_client.generate_latest(...)`` for the registry in use.

    Args:
        app: The ASGI application to wrap.
        registry: Optional ``CollectorRegistry``.  Defaults to the global
            registry.  Pass a custom registry in tests to avoid pollution.

    Example::

        app = create_fastapi_app(
            result,
            RouteSources(python=[OrderInterface]),
            middleware=[PrometheusMiddleware],
        )
    """

    def __init__(
        self,
        app: _ASGIApp,
        registry: CollectorRegistry | None = None,
    ) -> None:
        self._app = app
        instruments = cached_instruments(registry, _INSTRUMENTS)
        self._requests_total = instruments[_REQUESTS_TOTAL]
        self._duration_seconds = instruments[_DURATION_SECONDS]

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        method = str(scope.get("method", "UNKNOWN")).upper()
        path_template = _get_path_template(scope)
        start = time.perf_counter()
        status_code: list[int] = [200]

        async def send_wrapper(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                status_code[0] = int(message.get("status", 200))
            await send(message)

        try:
            await self._app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - start
            self._requests_total.labels(
                method=method,
                path_template=path_template,
                status_code=str(status_code[0]),
            ).inc()
            self._duration_seconds.labels(
                method=method,
                path_template=path_template,
            ).observe(elapsed)


def _get_path_template(scope: _Scope) -> str:
    """Extract the route path template from the ASGI scope.

    Starlette/FastAPI expose the matched route under ``scope["route"]``.
    Falls back to a fixed ``"__unmatched__"`` label when unavailable to keep
    metric cardinality bounded.

    Args:
        scope: ASGI connection scope dict.

    Returns:
        Path template string, e.g. ``"/products/{product_id}"``. Returns
        ``"__unmatched__"`` when no route template is available.
    """
    route = scope.get("route")
    if route is not None:
        path: str = str(getattr(route, "path", "") or "")
        if path:
            return path
    return _UNMATCHED_PATH_TEMPLATE
