"""The mechanism-agnostic authentication middleware.

The middleware knows nothing about tokens: it asks an :class:`Authenticator`
for an identity, refuses the request when it gets none, and guarantees the
identity is torn down afterwards — including when the handler raises, because
a worker task that inherits the previous caller is the leak this design exists
to prevent.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.errors import SystemError
from loom.core.identity import Identity, current_identity
from loom.core.tracing import active_trace_id
from loom.rest.auth import AuthenticationMiddleware, AuthenticationUnavailable, RequestCredentials
from loom.rest.fastapi.auto import create_app
from loom.rest.middleware import TraceIdMiddleware
from tests.unit.rest._fixture_app import write_project

_SUBJECT_HEADER = "x-subject"
_MECHANISM = "test-header"
_PROTECTED = "/who"
_OPEN = "/health"

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]


class _HeaderAuthenticator:
    """Authenticates from a plain header — deliberately not a JWT."""

    name = _MECHANISM
    provides_roles = True

    def __init__(self, *, roles: tuple[str, ...] = ()) -> None:
        self._roles = roles
        self.seen: list[RequestCredentials] = []

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        self.seen.append(credentials)
        subject = credentials.header(_SUBJECT_HEADER)
        if subject is None:
            return None
        return Identity(
            subject=subject,
            roles=self._roles,
            attributes={"email": f"{subject}@example.com"},
            mechanism=self.name,
        )


class _FailingAuthenticator:
    """Raises instead of deciding, as a mechanism whose dependencies are down does."""

    name = _MECHANISM
    provides_roles = True

    def __init__(self, error: Exception) -> None:
        self._error = error

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        raise self._error


def _app(
    authenticator: _HeaderAuthenticator | _FailingAuthenticator,
    *,
    exclude_paths: tuple[str, ...] = (),
) -> FastAPI:
    app = FastAPI()

    @app.get(_PROTECTED)
    async def who() -> dict[str, Any]:
        identity = current_identity()
        return {
            "subject": identity.subject,
            "roles": list(identity.roles),
            "mechanism": identity.mechanism,
            "email": identity.attribute("email"),
        }

    @app.get(_OPEN)
    async def health() -> dict[str, str]:
        return {"subject": current_identity().subject}

    @app.get("/boom")
    async def boom() -> dict[str, str]:
        raise RuntimeError("handler exploded")

    app.add_middleware(
        AuthenticationMiddleware,
        authenticator=authenticator,
        exclude_paths=exclude_paths,
    )
    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
        base_url="http://testserver",
    )


async def _get(app: FastAPI, path: str, subject: str | None = None) -> httpx.Response:
    headers = {_SUBJECT_HEADER: subject} if subject is not None else {}
    async with _client(app) as client:
        return await client.get(path, headers=headers)


# ---------------------------------------------------------------------------
# Authentication outcome
# ---------------------------------------------------------------------------


async def test_an_accepted_caller_reaches_the_route_with_their_identity() -> None:
    """The identity the authenticator returned is what the request context carries."""
    response = await _get(_app(_HeaderAuthenticator(roles=("reader",))), _PROTECTED, "alice")
    assert response.json() == {
        "subject": "alice",
        "roles": ["reader"],
        "mechanism": _MECHANISM,
        "email": "alice@example.com",
    }


async def test_a_rejected_caller_never_reaches_the_route() -> None:
    """``None`` from the authenticator is a refusal, whatever the mechanism."""
    response = await _get(_app(_HeaderAuthenticator()), _PROTECTED)
    assert response.status_code == 401


async def test_the_401_carries_the_standard_error_body() -> None:
    """Refusals reuse the framework body so clients get code and trace_id."""
    detail = (await _get(_app(_HeaderAuthenticator()), _PROTECTED)).json()["detail"]
    assert {"code", "message", "trace_id"} <= set(detail)


async def test_the_401_carries_the_authentication_challenge() -> None:
    """RFC 9110 §11.6.1: a 401 must tell the client how to authenticate."""
    response = await _get(_app(_HeaderAuthenticator()), _PROTECTED)
    assert response.headers["www-authenticate"] == "Bearer"


async def test_excluded_paths_bypass_authentication() -> None:
    """Excluded paths are served without an identity, never with a forged one."""
    app = _app(_HeaderAuthenticator(), exclude_paths=(_OPEN,))
    response = await _get(app, _OPEN)
    assert (response.status_code, response.json()) == (200, {"subject": ""})


async def test_an_excluded_path_does_not_reach_the_authenticator() -> None:
    """Bypass means bypass: the mechanism is not consulted at all."""
    authenticator = _HeaderAuthenticator()
    await _get(_app(authenticator, exclude_paths=(_OPEN,)), _OPEN)
    assert authenticator.seen == []


# ---------------------------------------------------------------------------
# Credentials handed to the mechanism
# ---------------------------------------------------------------------------


async def test_the_authenticator_receives_the_request_path() -> None:
    """A mechanism may scope itself per path, so it must see which one it is."""
    authenticator = _HeaderAuthenticator()
    await _get(_app(authenticator), _PROTECTED, "alice")
    assert authenticator.seen[0].path == _PROTECTED


async def test_header_lookup_is_case_insensitive() -> None:
    """HTTP header names are case-insensitive; credentials must not pretend otherwise."""
    credentials = RequestCredentials(headers={"authorization": "Bearer x"}, path="/")
    assert credentials.header("AuThOrIzAtIoN") == "Bearer x"


async def test_absent_headers_read_as_none() -> None:
    """A missing header is ``None``, never an empty string to compare against."""
    credentials = RequestCredentials(headers={}, path="/")
    assert credentials.header("authorization") is None


# ---------------------------------------------------------------------------
# Teardown — the leak this middleware must not have
# ---------------------------------------------------------------------------


async def test_the_identity_is_reset_even_when_the_handler_raises() -> None:
    """Without the ``finally`` a reused task would inherit the previous caller."""
    app = _app(_HeaderAuthenticator())
    async with _client(app) as client:
        await client.get("/boom", headers={_SUBJECT_HEADER: "alice"})
    assert current_identity().subject == ""


async def test_the_handler_exception_is_not_swallowed() -> None:
    """Resetting the identity must not turn a crash into a silent success."""
    app = _app(_HeaderAuthenticator())
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=True)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        with pytest.raises(RuntimeError, match="handler exploded"):
            await client.get("/boom", headers={_SUBJECT_HEADER: "alice"})


async def test_concurrent_callers_never_cross() -> None:
    """Two requests in flight must each keep their own identity."""
    app = _app(_HeaderAuthenticator())
    async with _client(app) as client:
        alice, bob = await asyncio.gather(
            client.get(_PROTECTED, headers={_SUBJECT_HEADER: "alice"}),
            client.get(_PROTECTED, headers={_SUBJECT_HEADER: "bob"}),
        )
    assert (alice.json()["subject"], bob.json()["subject"]) == ("alice", "bob")


async def test_non_http_scopes_are_passed_through_untouched() -> None:
    """Lifespan and websocket scopes carry no credentials to authenticate."""
    seen: list[str] = []

    async def _inner(scope: _Scope, receive: _Receive, send: _Send) -> None:
        seen.append(scope["type"])

    middleware = AuthenticationMiddleware(_inner, authenticator=_HeaderAuthenticator())
    await middleware({"type": "lifespan"}, _noop_receive, _noop_send)

    assert seen == ["lifespan"]


async def _noop_receive() -> dict[str, Any]:
    return {"type": "lifespan.startup"}  # pragma: no cover - never awaited


async def _noop_send(message: dict[str, Any]) -> None:
    """Discard outbound ASGI messages."""


# ---------------------------------------------------------------------------
# Authentication unavailable
# ---------------------------------------------------------------------------

_TRACE_ID = "trace-auth-1"


async def _raw_call(
    authenticator: _HeaderAuthenticator | _FailingAuthenticator,
) -> list[dict[str, Any]]:
    """Drive the middleware directly and return every ASGI message it sends."""
    sent: list[dict[str, Any]] = []

    async def _send(message: dict[str, Any]) -> None:
        sent.append(message)

    async def _inner(scope: _Scope, receive: _Receive, send: _Send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})

    middleware = AuthenticationMiddleware(_inner, authenticator=authenticator)
    scope = {"type": "http", "path": _PROTECTED, "method": "GET", "headers": []}
    with active_trace_id(_TRACE_ID):
        await middleware(scope, _noop_receive, _send)
    return sent


async def test_an_unavailable_mechanism_answers_503() -> None:
    """An outage is neither a refusal (401) nor a bug (500)."""
    app = _app(_FailingAuthenticator(AuthenticationUnavailable("issuer keys unreachable")))
    response = await _get(app, _PROTECTED, "alice")
    assert response.status_code == 503


async def test_the_503_carries_the_standard_error_body_with_the_trace_id() -> None:
    """Clients get the framework body, and operators the trace to correlate the outage."""
    app = _app(_FailingAuthenticator(AuthenticationUnavailable("issuer keys unreachable")))
    app.add_middleware(TraceIdMiddleware)
    async with _client(app) as client:
        response = await client.get(_PROTECTED, headers={"x-request-id": _TRACE_ID})
    assert response.json() == {
        "detail": {
            "code": "service_unavailable",
            "message": "Authentication is temporarily unavailable.",
            "trace_id": _TRACE_ID,
        }
    }


def test_create_app_answers_an_outage_with_503_and_the_trace_id(tmp_path: Path) -> None:
    """Through the real middleware stack the 503 still carries the request's trace."""
    no_docs = {"docs_url": None, "redoc_url": None, "openapi_url": None}
    authenticator = _FailingAuthenticator(AuthenticationUnavailable("issuer keys unreachable"))
    app = create_app(write_project(tmp_path, rest=no_docs), authenticator=authenticator)
    with TestClient(app) as client:
        response = client.get("/ping/", headers={"x-request-id": _TRACE_ID})
    assert (response.status_code, response.json()["detail"]["trace_id"]) == (503, _TRACE_ID)


async def test_the_503_does_not_leak_the_mechanism_message() -> None:
    """The reason is for the operator's log, never for the caller."""
    app = _app(_FailingAuthenticator(AuthenticationUnavailable("https://idp.internal/jwks")))
    response = await _get(app, _PROTECTED, "alice")
    assert "idp.internal" not in response.text


async def test_the_503_carries_no_authentication_challenge() -> None:
    """A challenge would invite the client to discard credentials that are still valid."""
    app = _app(_FailingAuthenticator(AuthenticationUnavailable("issuer keys unreachable")))
    response = await _get(app, _PROTECTED, "alice")
    assert "www-authenticate" not in response.headers


async def test_an_unavailable_mechanism_never_reaches_the_route() -> None:
    """No identity was verified, so the handler must not run."""
    sent = await _raw_call(_FailingAuthenticator(AuthenticationUnavailable("down")))
    assert [message.get("status") for message in sent] == [503, None]


async def test_an_unavailable_mechanism_is_logged_with_its_cause(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The response hides the reason, so the log is the only place it survives."""
    error = AuthenticationUnavailable("issuer keys unreachable")
    with caplog.at_level(logging.WARNING, logger="loom.rest.auth.middleware"):
        await _raw_call(_FailingAuthenticator(error))
    assert [record.exc_info[1] for record in caplog.records if record.exc_info] == [error]


async def test_any_other_authenticator_error_still_propagates() -> None:
    """Only the declared outage is mapped; a bug keeps reaching the server error path."""
    authenticator = _FailingAuthenticator(RuntimeError("authenticator bug"))
    with pytest.raises(RuntimeError, match="authenticator bug"):
        await _raw_call(authenticator)


async def test_a_loom_system_error_from_the_authenticator_still_propagates() -> None:
    """The mapping keys on the declared type, not on the framework's error hierarchy."""
    authenticator = _FailingAuthenticator(SystemError("database down"))
    with pytest.raises(SystemError, match="database down"):
        await _raw_call(authenticator)


async def test_any_other_authenticator_error_still_answers_500() -> None:
    """Behind the server error middleware an undeclared failure stays a 500."""
    response = await _get(_app(_FailingAuthenticator(RuntimeError("bug"))), _PROTECTED, "alice")
    assert response.status_code == 500


async def test_the_401_response_is_byte_for_byte_unchanged() -> None:
    """The refusal's wire format is a contract clients already depend on."""
    body = (
        b'{"detail":{"code":"unauthenticated",'
        b'"message":"Authentication required: missing or invalid credentials.",'
        b'"trace_id":"trace-auth-1"}}'
    )
    assert await _raw_call(_HeaderAuthenticator()) == [
        {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode("ascii")),
                (b"www-authenticate", b"Bearer"),
            ],
        },
        {"type": "http.response.body", "body": body},
    ]
