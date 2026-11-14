"""REST routes and the REST adapter run as the request's caller, passed explicitly.

An execution given no identity runs as ``ANONYMOUS`` (T02 security F3), so a
REST entry point that left the identity to the ambient one would silently
drop an authenticated caller: each one reads the request's caller once, at
the boundary, and passes it to the executor.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import ANONYMOUS, Identity, reset_identity, set_identity
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.transport.adapter import AdapterRequest
from loom.core.use_case.factory import UseCaseFactory
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi.router_runtime import bind_interfaces
from loom.rest.rest_adapter import LoomRestAdapter

from .doubles import ALICE, AcmeAnswering, AcmeRoutes, as_caller

CALLERS = pytest.mark.parametrize(
    ("caller", "subject"),
    [(ALICE, ALICE.subject), (None, ANONYMOUS.subject)],
    ids=["authenticated", "anonymous"],
)


@pytest.fixture
def client(factory: UseCaseFactory, compiler: UseCaseCompiler) -> Callable[..., TestClient]:
    """A FastAPI application serving ``AcmeRoutes`` behind a transport authenticating as given."""

    def serve(caller: Identity | None) -> TestClient:
        app = FastAPI()
        routes = RestInterfaceCompiler(compiler).compile(AcmeRoutes)
        bind_interfaces(
            app, routes, factory, RuntimeExecutor(compiler), ObservabilityRuntime.noop()
        )
        return TestClient(as_caller(app, caller))  # type: ignore[arg-type]

    return serve


@pytest.fixture
def authenticated(caller: Identity | None) -> Iterator[None]:
    """The transport authenticated the request as ``caller``, if any."""
    token = None if caller is None else set_identity(caller)
    try:
        yield
    finally:
        if token is not None:
            reset_identity(token)


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
    """``LoomRestAdapter`` executes the instance it is handed (its ``LoomAdapter`` contract)."""

    @CALLERS
    @pytest.mark.usefixtures("authenticated")
    async def test_runs_as_the_request_caller(
        self, executor: RuntimeExecutor, caller: Identity | None, subject: str
    ) -> None:
        adapter = LoomRestAdapter(executor)

        seen = await adapter.handle(AcmeAnswering(), AdapterRequest(params={}))

        assert seen == {"runs_as": subject, "ambient": subject}
