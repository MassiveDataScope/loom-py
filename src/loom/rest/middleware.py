"""ASGI middleware for the Loom REST layer.

All middleware in this module is **framework-agnostic** — pure ASGI
callables that work with FastAPI, Starlette, Litestar, Django ASGI, or
any ASGI server (uvicorn, hypercorn, daphne).

No FastAPI or Starlette types are imported here.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from typing import Any

from loom.core.logger import log_context
from loom.core.tracing import generate_trace_id, reset_trace_id, set_trace_id

# ASGI type aliases
_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]
_ASGIApp = Callable[[_Scope, _Receive, _Send], Awaitable[None]]

# The trace id is echoed back and lands in every structured log line, so its
# charset is an allowlist: ``re.ASCII`` keeps ``\w`` from also accepting Unicode
# word characters, which would let a caller forge look-alike identifiers.
_TRACE_ID_RE = re.compile(r"^[\w.-]{1,128}$", re.ASCII)


class TraceIdMiddleware:
    """ASGI middleware that propagates a trace identifier per request.

    On each HTTP request:

    1. Reads the configured header (default ``x-request-id``).
    2. Uses its value as the trace-id when it matches the accepted charset
       (``[A-Za-z0-9._-]``, 1-128 chars); generates a UUID4 otherwise. A
       client-supplied identifier is echoed back and reaches every log line,
       so an unvalidated one is a log-forging primitive.
    3. Activates the trace-id via :func:`~loom.core.tracing.set_trace_id` and
       binds it onto every log record with :func:`~loom.core.logger.log_context`.
    4. Injects the trace-id into the response headers so clients can
       correlate logs.
    5. Resets the context after the response is sent.

    Non-HTTP scopes (WebSocket, lifespan) are passed through unchanged.

    Args:
        app: The ASGI application to wrap.
        header: HTTP header name to read/write (case-insensitive,
            stored as lowercase bytes internally).
            Defaults to ``"x-request-id"``.

    Example — FastAPI::

        from loom.rest.middleware import TraceIdMiddleware

        app = create_fastapi_app(result, RouteSources(python=[...]))
        app.add_middleware(TraceIdMiddleware)

    Example — plain ASGI composition::

        app = TraceIdMiddleware(your_asgi_app)
    """

    def __init__(self, app: _ASGIApp, *, header: str = "x-request-id") -> None:
        self._app = app
        self._header_bytes = header.lower().encode()

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        tid = _accepted_trace_id(_extract_header(scope.get("headers", []), self._header_bytes))

        header_injected = False

        async def send_with_trace(message: dict[str, Any]) -> None:
            nonlocal header_injected
            if message["type"] == "http.response.start" and not header_injected:
                header_injected = True
                headers: list[tuple[bytes, bytes]] = list(message.get("headers", []))
                headers.append((self._header_bytes, tid.encode()))
                message = {**message, "headers": headers}
            await send(message)

        token = set_trace_id(tid)
        try:
            with log_context(trace_id=tid):
                await self._app(scope, receive, send_with_trace)
        finally:
            reset_trace_id(token)


def _accepted_trace_id(candidate: str | None) -> str:
    """Return the caller's trace id when it is safe to echo, else a fresh one.

    Args:
        candidate: Raw header value, or ``None`` when absent.

    Returns:
        The candidate when it matches the accepted charset, otherwise a newly
        generated identifier.
    """
    if candidate and _TRACE_ID_RE.match(candidate):
        return candidate
    return generate_trace_id()


def _extract_header(
    headers: list[tuple[bytes, bytes]],
    name: bytes,
) -> str | None:
    """Return the first header value matching *name* (lowercase bytes).

    Args:
        headers: Raw ASGI headers list of ``(name_bytes, value_bytes)`` pairs.
        name: Lowercase header name bytes to look up.

    Returns:
        Decoded string value, or ``None`` if the header is absent or empty.
    """
    for key, value in headers:
        if key.lower() == name:
            decoded = value.decode(errors="replace").strip()
            return decoded if decoded else None
    return None
