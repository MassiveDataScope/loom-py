"""CORS as configuration, with the unsafe combination made unrepresentable.

Starlette does not refuse ``allow_origins=["*"]`` together with
``allow_credentials=True``: it silently switches to reflecting the caller's
Origin and answering ``Access-Control-Allow-Credentials: true``. The wildcard
becomes "any site, with cookies", which is why the framework validates it at
parse time instead of shipping the footgun.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.config.errors import ConfigError
from loom.core.identity import Identity
from loom.rest.auth import AuthenticationMiddleware, RequestCredentials
from loom.rest.cors import CorsConfig
from loom.rest.fastapi.adapter import _mount_cors

_ORIGIN = "https://app.example.com"
_OTHER_ORIGIN = "https://evil.example.com"
_PATH = "/ping"
_ALLOW_ORIGIN = "access-control-allow-origin"


class _AlwaysRefuses:
    """Authenticator that never accepts anyone, to prove preflights bypass it."""

    name = "never"
    provides_roles = False

    async def authenticate(self, credentials: RequestCredentials) -> Identity | None:
        del credentials
        return None


def _app(cors: CorsConfig | None, *, authenticated: bool = False) -> FastAPI:
    app = FastAPI()

    @app.get(_PATH)
    async def ping() -> dict[str, str]:
        return {"status": "ok"}

    if authenticated:
        app.add_middleware(AuthenticationMiddleware, authenticator=_AlwaysRefuses())
    _mount_cors(app, cors)
    return app


def _client(cors: CorsConfig | None, *, authenticated: bool = False) -> TestClient:
    return TestClient(_app(cors, authenticated=authenticated), raise_server_exceptions=False)


def _preflight(client: TestClient, origin: str = _ORIGIN) -> Any:
    return client.options(
        _PATH,
        headers={
            "Origin": origin,
            "Access-Control-Request-Method": "GET",
        },
    )


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_a_wildcard_origin_with_credentials_fails_at_parse() -> None:
    """Starlette would reflect the Origin and allow credentials: refuse to boot."""
    with pytest.raises(ConfigError, match="allow_credentials"):
        CorsConfig(allow_origins=("*",), allow_credentials=True)


def test_the_error_explains_the_reflected_origin() -> None:
    """An actionable message: the operator must know what the wildcard becomes."""
    with pytest.raises(ConfigError, match="reflects the caller's Origin"):
        CorsConfig(allow_origins=("*",), allow_credentials=True)


def test_a_wildcard_origin_without_credentials_is_allowed() -> None:
    """A public read-only API may legitimately answer every origin."""
    assert CorsConfig(allow_origins=("*",)).allow_credentials is False


def test_explicit_origins_with_credentials_are_allowed() -> None:
    """Naming the origins is exactly the safe way to use credentials."""
    config = CorsConfig(allow_origins=(_ORIGIN,), allow_credentials=True)
    assert config.allow_origins == (_ORIGIN,)


# ---------------------------------------------------------------------------
# Behaviour
# ---------------------------------------------------------------------------


def test_without_the_section_no_cors_headers_are_added() -> None:
    """An application that never intended to be called cross-origin stays as it was."""
    response = _client(None).get(_PATH, headers={"Origin": _ORIGIN})
    assert _ALLOW_ORIGIN not in response.headers


def test_an_allowed_origin_receives_the_header() -> None:
    """The ordinary case: a browser call from a listed origin is permitted."""
    response = _client(CorsConfig(allow_origins=(_ORIGIN,))).get(_PATH, headers={"Origin": _ORIGIN})
    assert response.headers[_ALLOW_ORIGIN] == _ORIGIN


def test_an_unlisted_origin_receives_no_header() -> None:
    """The allowlist is the point: an unlisted site gets nothing to work with."""
    client = _client(CorsConfig(allow_origins=(_ORIGIN,)))
    response = client.get(_PATH, headers={"Origin": _OTHER_ORIGIN})
    assert _ALLOW_ORIGIN not in response.headers


def test_a_preflight_is_answered() -> None:
    """A browser will not send the real request until the preflight succeeds."""
    response = _preflight(_client(CorsConfig(allow_origins=(_ORIGIN,))))
    assert response.status_code == 200


def test_a_preflight_is_answered_even_with_authentication_enabled() -> None:
    """A preflight carries no credentials by definition: a 401 would break CORS."""
    client = _client(CorsConfig(allow_origins=(_ORIGIN,)), authenticated=True)
    response = _preflight(client)
    assert (response.status_code, response.headers[_ALLOW_ORIGIN]) == (200, _ORIGIN)


def test_the_real_request_is_still_authenticated() -> None:
    """CORS answers the preflight; it never exempts the request that follows."""
    client = _client(CorsConfig(allow_origins=(_ORIGIN,)), authenticated=True)
    response = client.get(_PATH, headers={"Origin": _ORIGIN})
    assert response.status_code == 401
