"""Request-body guards: the size cap for the whole application and the
standard error of a body that is not well-formed JSON.

Neither uvicorn nor Starlette caps the size of a request body: an endpoint that
calls ``await request.body()`` will happily buffer whatever the client keeps
sending, and a chunked upload with no end takes the worker down with it.

The cap lives in an ASGI middleware rather than in a handler so it also covers
routes the application mounted by hand.  Endpoints with a stricter budget — the
SQL endpoint, for instance — apply theirs on top.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from http import HTTPStatus
from typing import Any

import msgspec

from loom.core.tracing import get_trace_id

# ASGI type aliases
_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]
_ASGIApp = Callable[[_Scope, _Receive, _Send], Awaitable[None]]

DEFAULT_MAX_BODY_BYTES = 1024 * 1024
"""Body budget applied to every route unless the application raises it."""

PAYLOAD_TOO_LARGE_CODE = "payload_too_large"
MALFORMED_BODY_CODE = "bad_request"
MALFORMED_BODY_MESSAGE = "Request body is not valid JSON"
MALFORMED_BODY_ERRORS: tuple[type[Exception], ...] = (
    msgspec.DecodeError,
    UnicodeDecodeError,
    RecursionError,
)
"""Decoder errors that mean the body is not well-formed JSON."""
_HTTP_SCOPE = "http"
_REQUEST_MESSAGE = "http.request"
_CONTENT_LENGTH = b"content-length"


class BodyTooLarge(Exception):
    """Raised while reading a request body that exceeds the configured cap.

    Handlers must let it propagate: :class:`BodySizeLimitMiddleware` turns it
    into the ``413`` response, and swallowing it would report a ``500`` for a
    perfectly diagnosable client error.

    Args:
        max_bytes: Cap that was exceeded.
    """

    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes
        super().__init__(payload_too_large_message(max_bytes))


def payload_too_large_message(max_bytes: int) -> str:
    """Return the message every ``413`` of the framework carries."""
    return f"Request body exceeds the maximum accepted size ({max_bytes} bytes)"


def payload_too_large_detail(max_bytes: int) -> dict[str, Any]:
    """Return the standard error body of a ``413``, trace id included."""
    return {
        "code": PAYLOAD_TOO_LARGE_CODE,
        "message": payload_too_large_message(max_bytes),
        "trace_id": get_trace_id(),
    }


def malformed_body_detail() -> dict[str, Any]:
    """Return the standard error body of a ``400`` for a body that is not well-formed JSON.

    The message is fixed, so the offending input is never echoed back.
    """
    return {
        "code": MALFORMED_BODY_CODE,
        "message": MALFORMED_BODY_MESSAGE,
        "trace_id": get_trace_id(),
    }


class BodySizeLimitMiddleware:
    """Reject request bodies larger than *max_bytes*, without buffering them.

    Two layers, because a cap that trusts the client is not a cap:

    1. A declared ``Content-Length`` above the budget is refused before the
       application runs at all.
    2. The ``receive`` channel is wrapped and counts what actually arrives, so
       a chunked body or a lying header is cut as soon as it crosses the line.

    Non-HTTP scopes (WebSocket, lifespan) are passed through unchanged.

    Args:
        app: The ASGI application to wrap.
        max_bytes: Maximum accepted body size.

    Example::

        app.add_middleware(BodySizeLimitMiddleware, max_bytes=2 * 1024 * 1024)
    """

    def __init__(self, app: _ASGIApp, *, max_bytes: int = DEFAULT_MAX_BODY_BYTES) -> None:
        self._app = app
        self._max_bytes = max_bytes

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        """Cap the body of one HTTP request, or pass the scope through."""
        if scope["type"] != _HTTP_SCOPE:
            await self._app(scope, receive, send)
            return

        if _declared_too_large(scope.get("headers", []), self._max_bytes):
            await send_payload_too_large(send, self._max_bytes)
            return

        capped = _CappedRequest(receive, send, self._max_bytes)
        try:
            await self._app(scope, capped.receive, capped.send)
        except BodyTooLarge:
            await capped.answer_if_unanswered()


class _CappedRequest:
    """Counts the body of one request and owns the ``413`` that replaces it.

    Raising from ``receive`` alone is not enough: frameworks wrap body-parsing
    failures in their own error — FastAPI turns any of them into a ``400`` —
    so the refusal would reach the caller mislabelled.  Once the cap is
    crossed, whatever the application decides to answer is dropped in favour of
    the ``413``.
    """

    def __init__(self, receive: _Receive, send: _Send, max_bytes: int) -> None:
        self._receive = receive
        self._send = send
        self._max_bytes = max_bytes
        self._received = 0
        self._exceeded = False
        self._answered = False

    async def receive(self) -> dict[str, Any]:
        """Return the next ASGI message, refusing to read past the cap."""
        message = await self._receive()
        if message["type"] != _REQUEST_MESSAGE:
            return message
        self._received += len(message.get("body", b""))
        if self._received > self._max_bytes:
            self._exceeded = True
            raise BodyTooLarge(self._max_bytes)
        return message

    async def send(self, message: dict[str, Any]) -> None:
        """Forward *message*, or replace the response once the cap was crossed."""
        if not self._exceeded:
            await self._send(message)
            return
        if message["type"] == "http.response.start":
            await self.answer_if_unanswered()

    async def answer_if_unanswered(self) -> None:
        """Emit the ``413`` unless it was already sent for this request."""
        if self._answered:
            return
        self._answered = True
        await send_payload_too_large(self._send, self._max_bytes)


def _declared_too_large(headers: list[tuple[bytes, bytes]], max_bytes: int) -> bool:
    """Report whether the declared ``Content-Length`` already exceeds the cap."""
    raw = next((value for key, value in headers if key.lower() == _CONTENT_LENGTH), None)
    if raw is None:
        return False
    try:
        return int(raw) > max_bytes
    except ValueError:
        return False


async def send_payload_too_large(send: _Send, max_bytes: int) -> None:
    """Send a ``413`` using the framework's standard error body shape.

    Args:
        send: ASGI send callable of the request being refused.
        max_bytes: Cap that was exceeded, reported to the caller.
    """
    body = msgspec.json.encode({"detail": payload_too_large_detail(max_bytes)})
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(body)).encode("ascii")),
        (b"connection", b"close"),
    ]
    status = int(HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})
