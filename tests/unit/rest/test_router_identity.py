"""The router hands the verified caller to the executor, one per request.

``router_runtime`` is the single place in the REST layer that reads the
ambient identity; from there the caller travels as an explicit argument.  These
tests pin that hand-off and, above all, that two concurrent requests never see
each other's caller.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from fastapi import FastAPI

from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.markers import Caller
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestInterface, RestRoute

_SUBJECT_HEADER = "x-test-subject"

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]
_ASGIApp = Callable[[_Scope, _Receive, _Send], Awaitable[None]]


class WhoAmIUseCase(UseCase[Any, dict[str, str]]):
    """Reports the caller the executor injected."""

    async def execute(self, caller: Identity = Caller()) -> dict[str, str]:
        await asyncio.sleep(0)
        return {"subject": caller.subject, "mechanism": caller.mechanism}


class WhoAmIInterface(RestInterface[str]):
    prefix = "/whoami"
    routes = (RestRoute(use_case=WhoAmIUseCase, method="GET", path="/"),)


class _HeaderIdentityMiddleware:
    """Installs an identity taken from a header, resetting it in a finally."""

    def __init__(self, app: _ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        subject = _header(scope.get("headers", []), _SUBJECT_HEADER)
        if scope["type"] != "http" or subject is None:
            await self._app(scope, receive, send)
            return
        token = set_identity(Identity(subject=subject, mechanism="header"))
        try:
            await self._app(scope, receive, send)
        finally:
            reset_identity(token)


def _header(headers: list[tuple[bytes, bytes]], name: str) -> str | None:
    wanted = name.encode()
    return next((value.decode() for key, value in headers if key.lower() == wanted), None)


def _app() -> FastAPI:
    compiler = UseCaseCompiler()
    compiler.compile(WhoAmIUseCase)
    factory = UseCaseFactory(LoomContainer())
    factory.register(WhoAmIUseCase)
    app = FastAPI()
    bind_interfaces(
        app,
        RestInterfaceCompiler(compiler).compile(WhoAmIInterface),
        factory,
        RuntimeExecutor(compiler),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    app.add_middleware(_HeaderIdentityMiddleware)
    return app


def _async_client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
    )


async def _whoami(client: httpx.AsyncClient, subject: str | None) -> dict[str, str]:
    headers = {_SUBJECT_HEADER: subject} if subject is not None else {}
    response = await client.get("/whoami/", headers=headers)
    assert response.status_code == 200
    return dict(response.json())


class TestRouterIdentity:
    async def test_the_verified_caller_reaches_the_use_case(
        self,
    ) -> None:
        """The declared identity parameter is filled from the request context."""
        async with _async_client(_app()) as client:
            assert await _whoami(client, "alice") == {"subject": "alice", "mechanism": "header"}

    async def test_an_unauthenticated_request_yields_the_anonymous_caller(
        self,
    ) -> None:
        """The router always delivers an identity; without one it is explicitly anonymous."""
        async with _async_client(_app()) as client:
            assert await _whoami(client, None) == {"subject": "", "mechanism": ""}

    async def test_concurrent_requests_never_cross_callers(
        self,
    ) -> None:
        """Two in-flight requests with different callers must not leak into each other."""
        async with _async_client(_app()) as client:
            alice, bob = await asyncio.gather(
                _whoami(client, "alice"),
                _whoami(client, "bob"),
            )

        assert (alice["subject"], bob["subject"]) == ("alice", "bob")

    async def test_the_identity_is_reset_between_requests(
        self,
    ) -> None:
        """A reused context must not inherit the previous caller."""
        async with _async_client(_app()) as client:
            await _whoami(client, "alice")
            anonymous = await _whoami(client, None)

        assert anonymous["subject"] == ""
