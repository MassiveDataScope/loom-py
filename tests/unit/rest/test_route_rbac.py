"""Declarative route authorization: ``requires_roles`` (RBAC level 1).

The route states which roles may reach it, and the router enforces that before
the use case exists.  A denied caller must not cause a single line of business
code to run — not even the constructor, which is where repositories and
sessions get resolved.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.model import RestInterface, RestRoute

_ADMIN = "admin"
_READER = "reader"
_PATH = "/reports/"

_Scope = dict[str, Any]
_Receive = Callable[[], Awaitable[dict[str, Any]]]
_Send = Callable[[dict[str, Any]], Awaitable[None]]

BUILDS: list[str] = []


class ReportUseCase(UseCase[Any, dict[str, str]]):
    """Records its own construction so denials can be proven to skip it."""

    def __init__(self) -> None:
        BUILDS.append(type(self).__name__)

    async def execute(self) -> dict[str, str]:
        return {"status": "ok"}


class AcmePublishedIdentityMiddleware:
    """Publishes a fixed identity for the wrapped application."""

    def __init__(self, app: Any, *, identity: Identity | None) -> None:
        self._app = app
        self._identity = identity

    async def __call__(self, scope: _Scope, receive: _Receive, send: _Send) -> None:
        if scope["type"] != "http" or self._identity is None:
            await self._app(scope, receive, send)
            return
        token = set_identity(self._identity)
        try:
            await self._app(scope, receive, send)
        finally:
            reset_identity(token)


def _interface(
    *,
    route_roles: tuple[str, ...] = (),
    interface_roles: tuple[str, ...] = (),
) -> type[RestInterface[Any]]:
    class ReportInterface(RestInterface[str]):
        prefix = "/reports"
        requires_roles = interface_roles
        routes = (
            RestRoute(
                use_case=ReportUseCase,
                method="GET",
                path="/",
                requires_roles=route_roles,
            ),
        )

    return ReportInterface


def _client(interface: type[RestInterface[Any]], identity: Identity | None) -> TestClient:
    compiler = UseCaseCompiler()
    compiler.compile(ReportUseCase)
    factory = UseCaseFactory(LoomContainer())
    factory.register(ReportUseCase)
    app = FastAPI()
    bind_interfaces(
        app,
        RestInterfaceCompiler(compiler).compile(interface),
        factory,
        RuntimeExecutor(compiler),
        observability_runtime=ObservabilityRuntime.noop(),
    )
    app.add_middleware(AcmePublishedIdentityMiddleware, identity=identity)
    return TestClient(app, raise_server_exceptions=False)


def _get(
    identity: Identity | None,
    *,
    route_roles: tuple[str, ...] = (),
    interface_roles: tuple[str, ...] = (),
) -> Any:
    BUILDS.clear()
    interface = _interface(route_roles=route_roles, interface_roles=interface_roles)
    return _client(interface, identity).get(_PATH)


def _caller(*roles: str) -> Identity:
    return Identity(subject="user-1", roles=roles, mechanism="test")


class TestRouteRoles:
    def test_a_caller_holding_the_required_role_is_served(
        self,
    ) -> None:
        """The happy path: the declared role is held, the use case runs."""
        response = _get(_caller(_ADMIN), route_roles=(_ADMIN,))
        assert (response.status_code, response.json()) == (200, {"status": "ok"})

    def test_a_caller_without_the_required_role_is_refused(
        self,
    ) -> None:
        """A role the caller does not hold is a 403, not a 404 or a 500."""
        response = _get(_caller(_READER), route_roles=(_ADMIN,))
        assert response.status_code == 403

    def test_a_refused_caller_never_reaches_the_use_case(
        self,
    ) -> None:
        """Authorization runs before construction: no repository is even resolved."""
        _get(_caller(_READER), route_roles=(_ADMIN,))
        assert BUILDS == []

    def test_a_served_caller_does_reach_the_use_case(
        self,
    ) -> None:
        """The negative test above is only meaningful if the positive one builds it."""
        _get(_caller(_ADMIN), route_roles=(_ADMIN,))
        assert BUILDS == ["ReportUseCase"]

    def test_an_unauthenticated_caller_is_refused(
        self,
    ) -> None:
        """Fail-closed: no identity cannot mean 'no restriction'."""
        response = _get(None, route_roles=(_ADMIN,))
        assert response.status_code == 403

    def test_an_unauthenticated_caller_never_reaches_the_use_case(
        self,
    ) -> None:
        """The absence of an identity stops the request just as a wrong role does."""
        _get(None, route_roles=(_ADMIN,))
        assert BUILDS == []

    def test_holding_any_of_the_declared_roles_is_enough(
        self,
    ) -> None:
        """``requires_roles`` is a set of alternatives, not a conjunction."""
        response = _get(_caller(_READER), route_roles=(_ADMIN, _READER))
        assert response.status_code == 200

    def test_a_route_without_declared_roles_stays_open(
        self,
    ) -> None:
        """RBAC is opt-in: routes that declare nothing keep their current behaviour."""
        response = _get(None)
        assert response.status_code == 200

    def test_the_denial_uses_the_standard_error_body(
        self,
    ) -> None:
        """Denials carry code and trace_id like every other framework error."""
        detail = _get(_caller(_READER), route_roles=(_ADMIN,)).json()["detail"]
        assert (detail["code"], "trace_id" in detail) == ("forbidden", True)

    def test_the_denial_does_not_disclose_the_required_roles(
        self,
    ) -> None:
        """The response must not become an oracle about the route's policy."""
        detail = _get(_caller(_READER), route_roles=(_ADMIN,)).json()["detail"]
        assert _ADMIN not in detail["message"]


class TestInterfaceDefaultRoles:
    def test_the_interface_default_applies_to_its_routes(
        self,
    ) -> None:
        """Declaring once on the interface protects every route it exposes."""
        response = _get(_caller(_READER), interface_roles=(_ADMIN,))
        assert response.status_code == 403

    def test_the_interface_default_serves_a_holder(
        self,
    ) -> None:
        """The inherited declaration is a real check, not a blanket denial."""
        response = _get(_caller(_ADMIN), interface_roles=(_ADMIN,))
        assert response.status_code == 200

    def test_a_route_declaration_overrides_the_interface_default(
        self,
    ) -> None:
        """Route level wins, exactly like pagination and profile defaults."""
        response = _get(_caller(_READER), route_roles=(_READER,), interface_roles=(_ADMIN,))
        assert response.status_code == 200
