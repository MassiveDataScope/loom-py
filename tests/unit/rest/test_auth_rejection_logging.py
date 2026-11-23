"""A rejected request has to leave a trace the operator can correlate.

The 401 body says nothing on purpose -- no oracle for whoever is probing -- but the
same silence in the server log means a stolen token being replayed, or someone
walking the endpoints, is invisible. The response and the log are different
audiences, and only the response has an attacker in it.
"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.core.log_names import LogField
from loom.rest.auth.abc import RequestCredentials
from loom.rest.auth.middleware import AuthenticationMiddleware
from loom.rest.event import RestEvent


class RefusingAuthenticator:
    async def authenticate(self, credentials: RequestCredentials) -> None:
        return None


class AcceptingAuthenticator:
    def __init__(self, identity: object) -> None:
        self._identity = identity

    async def authenticate(self, credentials: RequestCredentials) -> object:
        return self._identity


async def _call(middleware: AuthenticationMiddleware, **scope: Any) -> list[dict[str, Any]]:
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:  # pragma: no cover - never awaited here
        return {"type": "http.request"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    base: dict[str, Any] = {
        "type": "http",
        "path": "/v1/catalog/search",
        "method": "GET",
        "headers": [],
        "client": ("203.0.113.7", 54321),
    }
    base.update(scope)
    await middleware(base, receive, send)
    return sent


async def _app(scope: Any, receive: Any, send: Any) -> None:
    await send({"type": "http.response.start", "status": 200, "headers": []})


@pytest.fixture
def middleware() -> AuthenticationMiddleware:
    return AuthenticationMiddleware(_app, authenticator=RefusingAuthenticator())


class TestAuthRejectionLogging:
    async def test_a_refusal_is_logged_above_debug(
        self, middleware: AuthenticationMiddleware, caplog: pytest.LogCaptureFixture
    ) -> None:
        """At DEBUG it does not survive a production log level, which is where it matters."""
        with caplog.at_level(logging.INFO, logger="loom.rest.auth.middleware"):
            await _call(middleware)

        assert caplog.records
        assert all(record.levelno >= logging.INFO for record in caplog.records)

    async def test_a_refusal_logs_the_event_and_the_method(
        self, middleware: AuthenticationMiddleware, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A refusal names what was attempted by its method, never the raw path or the client."""
        with caplog.at_level(logging.INFO, logger="loom.rest.auth.middleware"):
            await _call(middleware)

        (record,) = caplog.records
        assert record.msg == RestEvent.AUTHENTICATION_REFUSED
        assert getattr(record, LogField.HTTP_REQUEST_METHOD) == "GET"

    async def test_the_log_never_carries_the_credential(
        self, middleware: AuthenticationMiddleware, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A log that holds the bearer token turns log access into API access."""
        token = "eyJhbGciOiJFZERTQSJ9.c3VwZXItc2VjcmV0.c2lnbmF0dXJl"
        with caplog.at_level(logging.DEBUG, logger="loom.rest.auth.middleware"):
            await _call(middleware, headers=[(b"authorization", f"Bearer {token}".encode())])

        assert token not in caplog.text
        assert "c3VwZXItc2VjcmV0" not in caplog.text

    async def test_an_accepted_request_is_not_logged_as_a_refusal(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Otherwise the signal drowns: every request would look like a refusal."""
        from loom.core.identity import Identity

        accepted = AuthenticationMiddleware(
            _app, authenticator=AcceptingAuthenticator(Identity(subject="a@b.c", roles=("r",)))
        )

        with caplog.at_level(logging.INFO, logger="loom.rest.auth.middleware"):
            await _call(accepted)

        assert not caplog.records

    async def test_an_excluded_path_is_not_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        excluded = AuthenticationMiddleware(
            _app, authenticator=RefusingAuthenticator(), exclude_paths=("/health",)
        )

        with caplog.at_level(logging.INFO, logger="loom.rest.auth.middleware"):
            await _call(excluded, path="/health")

        assert not caplog.records

    async def test_a_client_without_an_address_still_logs(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """ASGI allows `client` to be absent; a missing address must not lose the event."""
        middleware = AuthenticationMiddleware(_app, authenticator=RefusingAuthenticator())

        with caplog.at_level(logging.INFO, logger="loom.rest.auth.middleware"):
            await _call(middleware, client=None)

        assert caplog.records
