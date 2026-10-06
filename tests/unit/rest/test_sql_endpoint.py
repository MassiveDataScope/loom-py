"""Tests for the optional per-connection SQL endpoint (spec §3/§7.7)."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.errors import SystemError as LoomSystemError
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql.abc import SqlExecutionError
from loom.core.sql.config import SqlConfig, SqlConnectionConfig, SqlEndpointConfig
from loom.core.sql.service import SqlQueryService
from loom.rest.fastapi.sql import bind_sql_endpoints
from tests.helpers.deep_json import assert_too_deep_for_msgspec, fixed_thread_stack, nested_array
from tests.unit.core.sql._fakes import (
    FakeSqlExecutor,
    make_connection_config,
    make_sql_config,
)


def _endpoint_connection(**overrides: Any) -> SqlConnectionConfig:
    """Connection with its endpoint enabled in the single-role 'external' shape.

    These tests cover the envelope, the input edge and the error bodies; role
    binding to verified claims has its own suite
    (``test_sql_endpoint_roles_claim.py``), so the connection here uses the
    only shape ``auth: external`` admits: empty allowlist plus ``default_role``.
    """
    params: dict[str, Any] = {
        "allowed_roles": (),
        "sql_endpoint": SqlEndpointConfig(enabled=True, auth="external"),
    }
    params.update(overrides)
    return make_connection_config(**params)


def _make_app(config: SqlConfig, executors: dict[str, FakeSqlExecutor]) -> FastAPI:
    app = FastAPI()
    service = SqlQueryService(executors=dict(executors), config=config)
    bind_sql_endpoints(app, service=service, config=config)
    return app


def _client(executor: FakeSqlExecutor, **conn_overrides: Any) -> TestClient:
    config = make_sql_config(analytics=_endpoint_connection(**conn_overrides))
    app = _make_app(config, {"analytics": executor})
    return TestClient(app, raise_server_exceptions=False)


async def _asgi_post(
    app: FastAPI,
    path: str,
    chunks: list[bytes],
    headers: list[tuple[bytes, bytes]],
) -> tuple[int, int]:
    """POST raw body *chunks* to *app* over ASGI; return (status, chunks consumed)."""
    messages: list[dict[str, Any]] = [
        {"type": "http.request", "body": chunk, "more_body": True} for chunk in chunks
    ]
    messages.append({"type": "http.request", "body": b"", "more_body": False})
    consumed = 0
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        nonlocal consumed
        message = messages[consumed]
        consumed += 1
        return message

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": headers,
        "client": ("testclient", 50000),
        "server": ("testserver", 80),
    }
    await app(scope, receive, send)
    status = next(m["status"] for m in sent if m["type"] == "http.response.start")
    return status, consumed


class _RecordingObserver:
    """Captures every lifecycle event emitted through the observability runtime."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def on_event(self, event: LifecycleEvent) -> None:
        self.events.append(event)


def test_returns_200_with_the_standard_tabular_envelope() -> None:
    """A successful SELECT responds the single envelope with columns, rows, paging."""
    client = _client(FakeSqlExecutor())
    response = client.post("/sql/analytics", json={"sql": "SELECT id, name FROM t"})
    assert (response.status_code, response.json()) == (
        200,
        {
            "columns": [
                {"name": "id", "type": "UInt64"},
                {"name": "name", "type": "String"},
            ],
            "rows": [[1, "alpha"], [2, "beta"]],
            "row_count": 2,
            "limit": 1000,
            "offset": 0,
            "has_more": False,
            "elapsed_ms": 1.5,
        },
    )


def test_mounts_on_the_custom_path_when_declared() -> None:
    """``sql_endpoint.path`` replaces the ``/sql/{name}`` default."""
    client = _client(
        FakeSqlExecutor(),
        sql_endpoint=SqlEndpointConfig(enabled=True, auth="external", path="/query/analytics"),
    )
    response = client.post("/query/analytics", json={"sql": "SELECT 1"})
    assert response.status_code == 200


def test_does_not_mount_connections_with_endpoint_disabled() -> None:
    """A connection without ``enabled: true`` exposes no HTTP route (double opt-in)."""
    config = make_sql_config(
        analytics=_endpoint_connection(),
        reporting=make_connection_config(),
    )
    app = _make_app(config, {"analytics": FakeSqlExecutor(), "reporting": FakeSqlExecutor()})
    client = TestClient(app, raise_server_exceptions=False)
    response = client.post("/sql/reporting", json={"sql": "SELECT 1"})
    assert response.status_code == 404


def test_does_not_mount_enabled_connections_without_auth_field() -> None:
    """``enabled: true`` without an explicit ``auth`` does NOT mount (B2 resolved)."""
    config = make_sql_config(
        analytics=_endpoint_connection(),
        reporting=make_connection_config(
            allowed_roles=(), sql_endpoint=SqlEndpointConfig(enabled=True)
        ),
    )
    app = _make_app(config, {"analytics": FakeSqlExecutor(), "reporting": FakeSqlExecutor()})
    client = TestClient(app, raise_server_exceptions=False)
    response = client.post("/sql/reporting", json={"sql": "SELECT 1"})
    assert response.status_code == 404


def test_returns_403_with_standard_body_when_role_is_not_allowed() -> None:
    """A role outside the allowlist responds 403 with the HttpErrorMapper body."""
    client = _client(FakeSqlExecutor())
    response = client.post("/sql/analytics", json={"sql": "SELECT 1", "roles": ["role_intruder"]})
    detail = response.json()["detail"]
    assert (response.status_code, detail["code"], "trace_id" in detail) == (
        403,
        "forbidden",
        True,
    )


def test_single_role_endpoint_always_applies_the_pinned_default_role() -> None:
    """With no allowlist every query runs with the one pinned ``default_role``."""
    executor = FakeSqlExecutor()
    client = _client(executor)
    client.post("/sql/analytics", json={"sql": "SELECT 1"})
    assert executor.calls[0].options.roles == ("role_viz_reader",)


def test_returns_422_with_standard_body_when_backend_rejects_the_sql() -> None:
    """A backend SqlExecutionError responds 422 with the rule_violation code."""
    client = _client(
        FakeSqlExecutor(error=SqlExecutionError("Code: 62. DB::Exception: Syntax error"))
    )
    response = client.post("/sql/analytics", json={"sql": "SELEC 1"})
    detail = response.json()["detail"]
    assert (response.status_code, detail["code"]) == (422, "rule_violation")


def test_returns_generic_500_when_the_backend_is_unreachable() -> None:
    """A system failure responds a standard 500 without leaking internals."""
    client = _client(FakeSqlExecutor(error=LoomSystemError("SQL backend unavailable")))
    response = client.post("/sql/analytics", json={"sql": "SELECT 1"})
    detail = response.json()["detail"]
    assert (response.status_code, detail["code"], "trace_id" in detail) == (
        500,
        "system_error",
        True,
    )


def test_an_unexpected_error_answers_the_generic_500_body() -> None:
    """An unhandled failure answers the router's generic 500, byte for byte."""
    client = _client(FakeSqlExecutor(error=RuntimeError("boom")))
    response = client.post("/sql/analytics", json={"sql": "SELECT 1"})
    assert (response.status_code, response.content) == (
        HTTPStatus.INTERNAL_SERVER_ERROR,
        b'{"code":"internal_error","message":"An unexpected error occurred","trace_id":""}',
    )


def test_rejects_sql_exceeding_max_sql_bytes_without_executing_it() -> None:
    """SQL larger than ``max_sql_bytes`` is rejected at the input edge (413/422)."""
    executor = FakeSqlExecutor()
    client = _client(executor, max_sql_bytes=64)
    response = client.post("/sql/analytics", json={"sql": "SELECT '" + "x" * 200 + "'"})
    assert (response.status_code in (413, 422), executor.calls) == (True, [])


def test_the_413_body_is_the_standard_payload_too_large_body() -> None:
    """The 413 carries the same bytes as the application-wide body cap."""
    client = _client(FakeSqlExecutor(), max_sql_bytes=64)
    response = client.post(
        "/sql/analytics",
        content=b"x" * (128 * 1024),
        headers={"content-type": "application/json"},
    )
    assert response.content == (
        b'{"detail":{"code":"payload_too_large","message":"Request body exceeds the maximum'
        b' accepted size (65600 bytes)","trace_id":null}}'
    )


def test_rejects_a_giant_body_with_413_without_invoking_the_executor() -> None:
    """A body over ``max_sql_bytes`` plus the fixed overhead is rejected 413 up front."""
    executor = FakeSqlExecutor()
    client = _client(executor, max_sql_bytes=64)
    response = client.post(
        "/sql/analytics",
        content=b"x" * (128 * 1024),
        headers={"content-type": "application/json"},
    )
    detail = response.json()["detail"]
    assert (response.status_code, detail["code"], "trace_id" in detail, executor.calls) == (
        413,
        "payload_too_large",
        True,
        [],
    )


async def test_lying_content_length_is_still_capped_by_the_stream_read() -> None:
    """A small Content-Length cannot bypass the cap: the capped stream read is authoritative."""
    executor = FakeSqlExecutor()
    config = make_sql_config(analytics=_endpoint_connection(max_sql_bytes=64))
    app = _make_app(config, {"analytics": executor})
    chunks = [b"x" * (16 * 1024) for _ in range(8)]  # 128 KiB stream vs a 10-byte header
    status, _ = await _asgi_post(
        app,
        "/sql/analytics",
        chunks,
        headers=[(b"content-type", b"application/json"), (b"content-length", b"10")],
    )
    assert (status, executor.calls) == (413, [])


async def test_chunked_body_without_content_length_is_capped_cutting_the_read() -> None:
    """Without Content-Length (chunked) the read stops as soon as the cap is crossed."""
    executor = FakeSqlExecutor()
    config = make_sql_config(analytics=_endpoint_connection(max_sql_bytes=64))
    app = _make_app(config, {"analytics": executor})
    chunks = [b"x" * (16 * 1024) for _ in range(32)]  # 512 KiB total; cap crossed early
    status, consumed = await _asgi_post(
        app, "/sql/analytics", chunks, headers=[(b"content-type", b"application/json")]
    )
    assert (status, executor.calls) == (413, [])
    assert consumed < len(chunks)  # the read was cut, never buffered to the end


@pytest.mark.parametrize(
    "body",
    [
        b"\x1f\x8b\x08\x00" + b"\x00" * 128,
        b"{not json",
        b"",
        b'{"sql": "SELECT \xff"}',
    ],
    ids=["gzip_like", "syntax", "empty", "not_utf8_in_string"],
)
def test_malformed_body_returns_the_standard_400(body: bytes) -> None:
    """A body that is not well-formed JSON → 400 ``bad_request``, never executed."""
    executor = FakeSqlExecutor()
    client = _client(executor)
    response = client.post(
        "/sql/analytics", content=body, headers={"content-type": "application/json"}
    )
    detail = response.json()["detail"]
    assert (response.status_code, executor.calls) == (HTTPStatus.BAD_REQUEST, [])
    assert detail.keys() == {"code", "message", "trace_id"}
    assert (detail["code"], detail["message"]) == ("bad_request", "Request body is not valid JSON")


def test_malformed_body_is_not_echoed() -> None:
    """The 400 carries a fixed message, not the decoder's view of the input."""
    client = _client(FakeSqlExecutor())
    response = client.post(
        "/sql/analytics",
        content=b'{"sql": "echo-marker',
        headers={"content-type": "application/json"},
    )
    assert response.status_code == HTTPStatus.BAD_REQUEST
    assert "echo-marker" not in response.text


def test_too_deeply_nested_body_returns_the_standard_400() -> None:
    """Parameters nested past msgspec's recursion bound → 400, never executed."""
    executor = FakeSqlExecutor()
    client = _client(executor)
    body = b'{"sql": "SELECT 1", "parameters": {"p": ' + nested_array() + b"}}"
    assert_too_deep_for_msgspec(body)

    with fixed_thread_stack():
        response = client.post(
            "/sql/analytics", content=body, headers={"content-type": "application/json"}
        )

    detail = response.json()["detail"]
    assert (response.status_code, executor.calls) == (HTTPStatus.BAD_REQUEST, [])
    assert detail["message"] == "Request body is not valid JSON"


def test_well_formed_body_of_wrong_type_stays_422() -> None:
    """Valid JSON that does not fit the schema keeps the ``rule_violation`` 422."""
    executor = FakeSqlExecutor()
    client = _client(executor)
    response = client.post("/sql/analytics", json={"sql": 1})
    detail = response.json()["detail"]
    assert (response.status_code, detail["code"], executor.calls) == (
        HTTPStatus.UNPROCESSABLE_ENTITY,
        "rule_violation",
        [],
    )


def test_rejects_body_with_settings_without_executing_it() -> None:
    """The body never accepts ``settings``: the schema rejects it with 422."""
    executor = FakeSqlExecutor()
    client = _client(executor)
    response = client.post(
        "/sql/analytics",
        json={"sql": "SELECT 1", "settings": {"role": "role_admin_evil"}},
    )
    assert (response.status_code, executor.calls) == (422, [])


def test_forwards_sql_parameters_limit_and_offset_to_the_service() -> None:
    """The allowed body fields reach the SQL service untouched."""
    executor = FakeSqlExecutor()
    client = _client(executor)
    client.post(
        "/sql/analytics",
        json={
            "sql": "SELECT {x:Int64}",
            "parameters": {"x": 1},
            "limit": 5,
            "offset": 2,
        },
    )
    call = executor.calls[0]
    assert (
        call.sql,
        dict(call.parameters or {}),
        call.options.roles,
        call.options.limit,
        call.options.offset,
    ) == ("SELECT {x:Int64}", {"x": 1}, ("role_viz_reader",), 5, 2)


def test_endpoint_stays_out_of_openapi_by_default() -> None:
    """``include_in_schema`` defaults to False: the endpoint is absent from OpenAPI."""
    client = _client(FakeSqlExecutor())
    paths = client.get("/openapi.json").json()["paths"]
    assert "/sql/analytics" not in paths


def test_endpoint_enters_openapi_when_opted_in() -> None:
    """``include_in_schema: true`` publishes the endpoint in the OpenAPI schema."""
    client = _client(
        FakeSqlExecutor(),
        sql_endpoint=SqlEndpointConfig(enabled=True, auth="external", include_in_schema=True),
    )
    paths = client.get("/openapi.json").json()["paths"]
    assert "/sql/analytics" in paths


def test_emits_the_router_runtime_equivalent_span_per_request() -> None:
    """Each request emits one START/END span with the router runtime labels (spec §3)."""
    observer = _RecordingObserver()
    config = make_sql_config(analytics=_endpoint_connection())
    app = FastAPI()
    service = SqlQueryService(executors={"analytics": FakeSqlExecutor()}, config=config)
    bind_sql_endpoints(
        app, service=service, config=config, observability_runtime=ObservabilityRuntime([observer])
    )
    client = TestClient(app, raise_server_exceptions=False)

    response = client.post("/sql/analytics", json={"sql": "SELECT 1"})

    assert response.status_code == 200
    assert [event.kind for event in observer.events] == [EventKind.START, EventKind.END]
    start = observer.events[0]
    assert (start.scope, start.name) == (Scope.USE_CASE, "execute_sql_analytics")
    assert (start.meta["route"], start.meta["method"], start.meta["read_only"]) == (
        "/sql/analytics",
        "POST",
        True,
    )
