"""Unit tests for loom.rest.middleware.TraceIdMiddleware."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.core.logger import LogIdentity
from loom.core.logger.filter import LogFilter
from loom.core.logger.install import install_record_factory
from loom.core.tracing import get_trace_id
from loom.rest.middleware import TraceIdMiddleware, _extract_header
from tests.helpers.loggers import CapturingHandler

# ---------------------------------------------------------------------------
# ASGI test helpers
# ---------------------------------------------------------------------------


def _make_http_scope(headers: list[tuple[bytes, bytes]] | None = None) -> dict[str, Any]:
    return {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": headers or [],
    }


def _make_lifespan_scope() -> dict[str, Any]:
    return {"type": "lifespan"}


async def _null_receive() -> dict[str, Any]:
    return {}


def _make_async_sink(target: list[Any]) -> Any:
    async def _sink(message: Any) -> None:
        target.append(message)

    return _sink


class _AppCapture:
    """Minimal ASGI app that records trace_id during its call."""

    def __init__(self) -> None:
        self.seen_trace_id: str | None = None
        self.response_started = False

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        self.seen_trace_id = get_trace_id()
        logging.getLogger("tests.middleware").info("rest.request_seen")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b""})


# ---------------------------------------------------------------------------
# _extract_header
# ---------------------------------------------------------------------------


class TestExtractHeader:
    @pytest.mark.parametrize(
        ("key", "header", "expected"),
        [
            pytest.param(b"x-request-id", b"abc123", "abc123", id="returns_value_when_present"),
            pytest.param(b"X-Request-ID", b"tid", "tid", id="case_insensitive_key"),
        ],
    )
    def test_returns_value_when_present(self, key: bytes, header: bytes, expected: str) -> None:
        headers = [(key, header)]
        assert _extract_header(headers, b"x-request-id") == expected

    def test_returns_none_when_absent(self) -> None:
        assert _extract_header([], b"x-request-id") is None

    def test_returns_none_for_empty_value(self) -> None:
        headers = [(b"x-request-id", b"  ")]
        assert _extract_header(headers, b"x-request-id") is None


# ---------------------------------------------------------------------------
# TraceIdMiddleware
# ---------------------------------------------------------------------------


class TestTraceIdMiddlewarePassThrough:
    @pytest.mark.asyncio
    async def test_non_http_scope_passed_through(self) -> None:
        called: list[bool] = []

        async def inner_app(scope: Any, receive: Any, send: Any) -> None:
            called.append(True)

        mw = TraceIdMiddleware(inner_app)
        await mw(_make_lifespan_scope(), _null_receive, _null_receive)  # type: ignore[arg-type]
        assert called == [True]

    @pytest.mark.asyncio
    async def test_non_http_does_not_set_trace_id(self) -> None:
        async def inner_app(scope: Any, receive: Any, send: Any) -> None:
            pass

        mw = TraceIdMiddleware(inner_app)
        await mw(_make_lifespan_scope(), _null_receive, _null_receive)  # type: ignore[arg-type]
        assert get_trace_id() is None


class TestTraceIdMiddlewareExtraction:
    @pytest.mark.asyncio
    async def test_uses_existing_header(self) -> None:
        app = _AppCapture()
        mw = TraceIdMiddleware(app, header="x-request-id")
        scope = _make_http_scope([(b"x-request-id", b"existing-tid")])

        sent: list[dict[str, Any]] = []
        await mw(scope, _null_receive, _make_async_sink(sent))

        assert app.seen_trace_id == "existing-tid"

    @pytest.mark.asyncio
    async def test_generates_id_when_header_missing(self) -> None:
        app = _AppCapture()
        mw = TraceIdMiddleware(app)
        scope = _make_http_scope()

        sent: list[dict[str, Any]] = []
        await mw(scope, _null_receive, _make_async_sink(sent))

        assert app.seen_trace_id is not None
        assert len(app.seen_trace_id) == 32

    @pytest.mark.asyncio
    async def test_custom_header_name(self) -> None:
        app = _AppCapture()
        mw = TraceIdMiddleware(app, header="x-correlation-id")
        scope = _make_http_scope([(b"x-correlation-id", b"corr-001")])

        sent: list[dict[str, Any]] = []
        await mw(scope, _null_receive, _make_async_sink(sent))

        assert app.seen_trace_id == "corr-001"


class TestTraceIdMiddlewareResponseHeader:
    @pytest.mark.asyncio
    async def test_injects_trace_id_in_response(self) -> None:
        app = _AppCapture()
        mw = TraceIdMiddleware(app, header="x-request-id")
        scope = _make_http_scope([(b"x-request-id", b"resp-tid")])

        sent: list[dict[str, Any]] = []
        await mw(scope, _null_receive, _make_async_sink(sent))

        start_msg = next(m for m in sent if m["type"] == "http.response.start")
        headers = dict(start_msg["headers"])
        assert headers.get(b"x-request-id") == b"resp-tid"

    @pytest.mark.asyncio
    async def test_generated_id_also_in_response(self) -> None:
        app = _AppCapture()
        mw = TraceIdMiddleware(app)
        scope = _make_http_scope()

        sent: list[dict[str, Any]] = []
        await mw(scope, _null_receive, _make_async_sink(sent))

        start_msg = next(m for m in sent if m["type"] == "http.response.start")
        header_dict = dict(start_msg["headers"])
        assert b"x-request-id" in header_dict
        assert len(header_dict[b"x-request-id"]) == 32


class TestTraceIdMiddlewareLogs:
    @pytest.mark.asyncio
    async def test_a_record_emitted_during_the_request_carries_its_trace_id(self) -> None:
        install_record_factory(LogIdentity(environment="test"))
        stamped = CapturingHandler()
        stamped.addFilter(LogFilter())
        root = logging.getLogger()
        root.addHandler(stamped)
        root.setLevel(logging.INFO)
        scope = _make_http_scope([(b"x-request-id", b"log-tid")])

        await TraceIdMiddleware(_AppCapture())(scope, _null_receive, _make_async_sink([]))

        root.removeHandler(stamped)
        [record] = stamped.records
        assert record.__dict__["trace_id"] == "log-tid"


class TestTraceIdMiddlewareContextReset:
    @pytest.mark.asyncio
    async def test_trace_id_cleared_after_request(self) -> None:
        app = _AppCapture()
        mw = TraceIdMiddleware(app)
        scope = _make_http_scope()

        sent: list[dict[str, Any]] = []
        await mw(scope, _null_receive, _make_async_sink(sent))

        assert get_trace_id() is None

    @pytest.mark.asyncio
    async def test_trace_id_cleared_even_on_exception(self) -> None:
        async def failing_app(scope: Any, receive: Any, send: Any) -> None:
            raise RuntimeError("boom")

        mw = TraceIdMiddleware(failing_app)
        scope = _make_http_scope()

        with pytest.raises(RuntimeError, match="boom"):
            await mw(scope, _null_receive, _null_receive)  # type: ignore[arg-type]

        assert get_trace_id() is None
