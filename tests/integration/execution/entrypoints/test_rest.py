"""REST routes and the REST adapter run as the request's caller, passed explicitly.

The authentication middleware installs the request's caller and starts its
delegation. Each REST entry point reads that caller once, at the boundary,
and passes it to the executor: an unauthenticated request runs as an explicit
``ANONYMOUS`` caller (T02 security F3), not with no caller.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator, Callable
from typing import Any

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.identity import (
    Identity,
    captured_delegation,
    current_caller,
    reset_identity,
    set_identity,
)
from loom.core.observability.observability import Observability
from loom.core.transport.dispatcher import ExecutionRequest
from loom.core.use_case.factory import UseCaseFactory
from loom.rest.auth import AuthenticationMiddleware
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.dispatcher import RestUseCaseDispatcher
from loom.rest.fastapi.router import mount_interfaces
from tests.helpers.delegation import acting_as, assert_new_delegation
from tests.helpers.loggers import CapturedLog

from .doubles import ALICE, BOB, CALLERS, AcmeAuthenticator, AcmeGreeting, AcmeRoutes, as_caller


@pytest.fixture
def client(factory: UseCaseFactory, compiler: UseCaseCompiler) -> Callable[..., TestClient]:
    """A FastAPI application serving ``AcmeRoutes`` behind a transport authenticating as given."""

    def serve(caller: Identity | None) -> TestClient:
        app = FastAPI()
        routes = RestInterfaceCompiler(compiler).compile(AcmeRoutes)
        mount_interfaces(app, routes, factory, UseCaseExecutor(compiler), Observability.noop())
        return TestClient(as_caller(app, caller))  # type: ignore[arg-type]

    return serve


class TestRestRoute:
    @CALLERS
    def test_builds_and_runs_inside_one_execution_as_the_request_caller(
        self, client: Callable[..., TestClient], caller: Identity | None, subject: str
    ) -> None:
        response = client(caller).get("/acme/")

        assert response.status_code == 200
        assert response.json() == {
            "built_as": subject,
            "runs_as": subject,
            "same_execution": True,
            "ambient": subject,
        }


class TestRestAdapter:
    """``RestUseCaseDispatcher`` builds the use case class it is handed inside its execution."""

    @CALLERS
    @pytest.mark.usefixtures("authenticated")
    async def test_injects_a_constructor_port_and_caller_as_the_request_caller(
        self,
        executor: UseCaseExecutor,
        factory: UseCaseFactory,
        caller: Identity | None,
        subject: str,
    ) -> None:
        adapter = RestUseCaseDispatcher(executor, factory=factory)

        seen = await adapter.handle(AcmeGreeting, ExecutionRequest(params={}))

        assert seen == {"greeting": f"hello {subject}", "runs_as": subject, "ambient": subject}


class TestAuthenticationMiddleware:
    """It installs ``(caller, now + window)`` for the request, unfixed: the request starts there."""

    def test_installs_the_caller_with_a_delegation_of_the_configured_window(
        self, window: float
    ) -> None:
        seen: dict[str, Any] = {}

        async def app(scope: Any, receive: Any, send: Any) -> None:
            seen["caller"], seen["deadline"] = captured_delegation()
            reset_identity(set_identity(seen["caller"]))  # unfixed: a transport may install
            await send({"type": "http.response.start", "status": 204, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        before = time.time()
        served = AuthenticationMiddleware(app, authenticator=AcmeAuthenticator(ALICE))
        response = TestClient(served).get("/acme")  # type: ignore[arg-type]

        assert response.status_code == 204
        assert seen["caller"] == ALICE
        assert_new_delegation(seen["deadline"], before, window)

    def test_a_dependency_replacing_the_request_caller_is_a_logged_switch(
        self, switches: CapturedLog
    ) -> None:
        async def as_bob() -> AsyncIterator[None]:
            with acting_as(BOB):
                yield

        api = FastAPI()

        @api.get("/acme", dependencies=[Depends(as_bob)])
        async def who() -> dict[str, str | None]:
            caller = current_caller()
            return {"caller": None if caller is None else caller.subject}

        served = AuthenticationMiddleware(api, authenticator=AcmeAuthenticator(ALICE))
        response = TestClient(served).get("/acme")  # type: ignore[arg-type]

        assert response.json() == {"caller": BOB.subject}
        [(event, fields)] = switches.calls
        assert (event, fields["from_subject"], fields["to_subject"], fields["use_case"]) == (
            "identity_switched",
            ALICE.subject,
            BOB.subject,
            "transport",
        )
