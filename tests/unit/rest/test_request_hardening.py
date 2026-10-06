"""Request-edge hardening: exclusions, body size, query bounds and trace ids.

Each test here pins one way a request used to reach further than it should:
an authentication exclusion captured by a templated route, an unbounded body,
an unbounded page size, or a trace identifier the caller wrote themselves.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from http import HTTPStatus
from typing import Any

import httpx
import msgspec
import pydantic
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.config.errors import ConfigError
from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.repository.abc.query import QuerySpec
from loom.core.tracing import get_trace_id
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.rest._body import DEFAULT_MAX_BODY_BYTES, BodySizeLimitMiddleware, send_payload_too_large
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi._errors import register_error_handlers
from loom.rest.fastapi._exclusions import verify_exclusion_paths
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.middleware import TraceIdMiddleware, _accepted_trace_id
from loom.rest.model import RestApiDefaults, RestInterface, RestRoute

_METRICS = "/metrics"
_TRACE_HEADER = "x-request-id"
_LIST_PATH = "/items/"

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]


# ---------------------------------------------------------------------------
# Exclusion paths captured by a templated route
# ---------------------------------------------------------------------------


def _app_with_tenant_route() -> FastAPI:
    app = FastAPI()

    @app.get("/{tenant}")
    async def tenant(tenant: str) -> dict[str, str]:
        return {"tenant": tenant}

    return app


def test_an_exclusion_captured_by_a_templated_route_aborts_startup() -> None:
    """``/metrics`` excluded while ``GET /{tenant}`` exists serves a route anonymously."""
    app = _app_with_tenant_route()
    with pytest.raises(ConfigError, match="business route"):
        verify_exclusion_paths(app, (_METRICS,))


def test_the_error_names_the_route_that_captures_the_exclusion() -> None:
    """The operator must be told which declaration to fix."""
    app = _app_with_tenant_route()
    with pytest.raises(ConfigError, match=r"/\{tenant\}"):
        verify_exclusion_paths(app, (_METRICS,))


def test_an_exclusion_matching_only_its_own_literal_route_is_accepted() -> None:
    """A real metrics endpoint is exactly what exclusions are for."""
    app = FastAPI()

    @app.get(_METRICS)
    async def metrics() -> str:
        return "ok"

    verify_exclusion_paths(app, (_METRICS,))


def test_an_exclusion_matching_no_route_is_accepted() -> None:
    """Excluding a path nothing serves is useless but not dangerous."""
    verify_exclusion_paths(FastAPI(), ("/nothing-here",))


# ---------------------------------------------------------------------------
# Request body size
# ---------------------------------------------------------------------------


def _echo_app(max_bytes: int) -> FastAPI:
    app = FastAPI()

    @app.post("/echo")
    async def echo(payload: dict[str, Any]) -> dict[str, int]:
        return {"size": len(payload.get("blob", ""))}

    app.add_middleware(BodySizeLimitMiddleware, max_bytes=max_bytes)
    return app


def test_a_declared_oversized_body_is_refused_before_the_route_runs() -> None:
    """The Content-Length fast path answers honest clients without buffering."""
    client = TestClient(_echo_app(64), raise_server_exceptions=False)
    response = client.post("/echo", json={"blob": "x" * 4096})
    assert response.status_code == 413


def test_the_413_uses_the_standard_error_body() -> None:
    """Size refusals carry code, message and trace id like every other error."""
    client = TestClient(_echo_app(64), raise_server_exceptions=False)
    detail = client.post("/echo", json={"blob": "x" * 4096}).json()["detail"]
    assert (detail["code"], set(detail)) == (
        "payload_too_large",
        {"code", "message", "trace_id"},
    )


async def test_the_raw_413_carries_a_plain_int_status_and_json_body() -> None:
    """The ASGI start message carries the int ``413`` the protocol specifies."""
    sent: list[dict[str, Any]] = []

    async def _send(message: dict[str, Any]) -> None:
        sent.append(message)

    await send_payload_too_large(_send, 64)

    start, body = sent
    assert (type(start["status"]), start["status"]) == (int, 413)
    assert msgspec.json.decode(body["body"])["detail"]["code"] == "payload_too_large"


async def test_a_chunked_body_is_cut_when_it_crosses_the_cap() -> None:
    """A lying or absent Content-Length must not buy unbounded memory."""

    async def _endless() -> Any:
        for _ in range(1000):
            yield b"x" * 1024

    transport = httpx.ASGITransport(app=_echo_app(4096), raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post("/echo", content=_endless())

    assert response.status_code == 413


def test_a_body_within_the_cap_is_served_normally() -> None:
    """The cap is a ceiling, not a policy change for ordinary requests."""
    client = TestClient(_echo_app(DEFAULT_MAX_BODY_BYTES))
    response = client.post("/echo", json={"blob": "x" * 10})
    assert response.json() == {"size": 10}


async def test_non_http_scopes_pass_through_the_body_cap() -> None:
    """Lifespan and websocket scopes carry no body to measure."""
    seen: list[str] = []

    async def _inner(scope: _Scope, receive: _Receive, send: _Send) -> None:
        seen.append(scope["type"])

    middleware = BodySizeLimitMiddleware(_inner, max_bytes=1)
    await middleware({"type": "lifespan"}, _unused_receive, _unused_send)

    assert seen == ["lifespan"]


async def _unused_receive() -> dict[str, Any]:
    return {"type": "lifespan.startup"}  # pragma: no cover - never awaited


async def _unused_send(message: dict[str, Any]) -> None:
    """Discard outbound ASGI messages."""


# ---------------------------------------------------------------------------
# Query bounds
# ---------------------------------------------------------------------------


class ListItemsUseCase(UseCase[Any, dict[str, int]]):
    """Reports the pagination the router resolved."""

    read_only = True

    async def execute(self, query: QuerySpec) -> dict[str, int]:
        await asyncio.sleep(0)
        return {"limit": query.limit, "page": query.page}


class ItemsInterface(RestInterface[str]):
    prefix = "/items"
    routes = (RestRoute(use_case=ListItemsUseCase, method="GET", path="/"),)


def _items_client(max_limit: int = 1000) -> TestClient:
    compiler = UseCaseCompiler()
    compiler.compile(ListItemsUseCase)
    factory = UseCaseFactory(LoomContainer())
    factory.register(ListItemsUseCase)
    app = FastAPI()
    register_error_handlers(app)
    bind_interfaces(
        app,
        RestInterfaceCompiler(compiler, defaults=RestApiDefaults(max_limit=max_limit)).compile(
            ItemsInterface
        ),
        factory,
        RuntimeExecutor(compiler),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    return TestClient(app, raise_server_exceptions=False)


def test_an_oversized_limit_is_clamped_to_the_configured_maximum() -> None:
    """One request must never be able to ask for the whole table."""
    response = _items_client(max_limit=200).get(f"{_LIST_PATH}?limit=100000000")
    assert response.json()["limit"] == 200


def test_a_limit_within_the_maximum_is_honoured() -> None:
    """Clamping is a ceiling, not a fixed page size."""
    response = _items_client(max_limit=200).get(f"{_LIST_PATH}?limit=25")
    assert response.json()["limit"] == 25


@pytest.mark.parametrize(
    "query",
    ["limit=abc", "page=abc", "limit=0", "page=0", "page=-3"],
    ids=["limit-nan", "page-nan", "limit-zero", "page-zero", "page-negative"],
)
def test_an_unusable_pagination_parameter_answers_400(query: str) -> None:
    """A client error must read as one instead of exploding into a 500."""
    response = _items_client().get(f"{_LIST_PATH}?{query}")
    assert response.status_code == 400


def test_the_400_carries_the_standard_error_body() -> None:
    """Validation refusals are normalised like every other framework error."""
    detail = _items_client().get(f"{_LIST_PATH}?limit=abc").json()["detail"]
    assert (detail["code"], "trace_id" in detail) == ("bad_request", True)


@pytest.mark.parametrize(
    ("query", "message"),
    [
        ("pagination=sideways", "Invalid pagination mode: 'sideways'."),
        ("sort=id&direction=sideways", "direction must be 'ASC' or 'DESC'."),
        ("__eq=1", "Invalid filter field: '__eq'."),
    ],
    ids=["pagination-mode", "sort-direction", "filter-without-field"],
)
def test_an_unusable_query_parameter_answers_the_standard_400(query: str, message: str) -> None:
    """Each query-parsing refusal is a 400 with the standard body and its own message."""
    response = _items_client().get(f"{_LIST_PATH}?{query}")
    detail = response.json()["detail"]
    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert (detail["code"], detail["message"]) == ("bad_request", message)


# ---------------------------------------------------------------------------
# FastAPI request validation
# ---------------------------------------------------------------------------


class _Payload(pydantic.BaseModel):
    name: str


def _validated_body_client() -> TestClient:
    app = FastAPI()
    register_error_handlers(app)

    @app.post("/payload")
    async def payload(body: _Payload) -> dict[str, str]:
        return {"name": body.name}

    return TestClient(app)


def test_a_missing_body_is_a_violation_of_the_body_itself() -> None:
    """A failure located at the body root names ``body``, not an empty field."""
    response = _validated_body_client().post("/payload")
    detail = response.json()["detail"]
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert detail["code"] == "rule_violations"
    assert [violation["field"] for violation in detail["violations"]] == ["body"]


# ---------------------------------------------------------------------------
# Trace id
# ---------------------------------------------------------------------------


def _trace_client() -> TestClient:
    app = FastAPI()

    @app.get("/trace")
    async def trace() -> dict[str, str | None]:
        return {"trace_id": get_trace_id()}

    app.add_middleware(TraceIdMiddleware)
    return TestClient(app)


def test_a_well_formed_client_trace_id_is_honoured() -> None:
    """Correlating with an upstream system is the point of accepting the header."""
    response = _trace_client().get("/trace", headers={_TRACE_HEADER: "req-42_ab.cd"})
    assert response.json()["trace_id"] == "req-42_ab.cd"


@pytest.mark.parametrize(
    "candidate",
    ["<script>alert(1)</script>", "a" * 129, "with space", "new\nline"],
    ids=["markup", "too-long", "space", "newline"],
)
def test_a_forged_trace_id_is_replaced_by_a_generated_one(candidate: str) -> None:
    """The trace id reaches every log line: a caller must not be able to write it."""
    response = _trace_client().get("/trace", headers={_TRACE_HEADER: candidate})
    assert response.json()["trace_id"] != candidate


def test_a_non_ascii_trace_id_is_replaced() -> None:
    """HTTP clients refuse to send these, but an ASGI server may still deliver them."""
    assert _accepted_trace_id("unicóde") != "unicóde"


def test_a_rejected_trace_id_is_not_echoed_in_the_response_header() -> None:
    """The response header must carry the accepted id, not the caller's."""
    response = _trace_client().get("/trace", headers={_TRACE_HEADER: "bad value"})
    assert response.headers[_TRACE_HEADER] != "bad value"
