"""How the router answers an agent run refused inside a use case."""

from __future__ import annotations

from http import HTTPStatus
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.di.container import LoomContainer
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import UseCaseExecutor
from loom.core.identity import ANONYMOUS, Identity, reset_identity, set_identity
from loom.core.stores.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.use_case import UseCase
from loom.rest.compiler import RestInterfaceCompiler
from loom.rest.fastapi.router import mount_interfaces
from loom.rest.model import RestInterface, RestRoute

ANA = Identity(subject="ana", mechanism="test")


class RefusedRunUseCase(UseCase[Any, None]):
    """Fails as the agent run it delegates to failed."""

    code: AgentRunErrorCode = AgentRunErrorCode.UNAUTHORIZED

    async def execute(self) -> None:
        raise AgentRunError(self.code, "agent 'triage' requires an authenticated caller")


class RefusedRunInterface(RestInterface[str]):
    prefix = "/runs"
    routes = (RestRoute(use_case=RefusedRunUseCase, method="GET", path="/"),)


def _app(caller: Identity) -> FastAPI:
    compiler = UseCaseCompiler()
    compiler.compile(RefusedRunUseCase)
    factory = UseCaseFactory(LoomContainer(classify_key=classify_key, implicit=main_repository))
    factory.register(RefusedRunUseCase)
    app = FastAPI()
    mount_interfaces(
        app,
        RestInterfaceCompiler(compiler).compile(RefusedRunInterface),
        factory,
        UseCaseExecutor(compiler),
    )

    @app.middleware("http")
    async def acting_as(request: Any, call_next: Any) -> Any:
        token = set_identity(caller)
        try:
            return await call_next(request)
        finally:
            reset_identity(token)

    return app


async def _get(caller: Identity) -> httpx.Response:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(caller)), base_url="http://testserver"
    ) as client:
        return await client.get("/runs/")


class TestAnAgentRunRefusal:
    @pytest.mark.parametrize(
        ("caller", "status"),
        [
            pytest.param(ANONYMOUS, HTTPStatus.UNAUTHORIZED, id="anonymous"),
            pytest.param(ANA, HTTPStatus.FORBIDDEN, id="authenticated"),
        ],
    )
    async def test_answers_the_caller_denial(self, caller: Identity, status: HTTPStatus) -> None:
        response = await _get(caller)

        assert response.status_code == status
        assert "triage" not in response.text

    @pytest.mark.parametrize(
        "code", [AgentRunErrorCode.RUN_TIMEOUT, AgentRunErrorCode.MCP_GRANT_UNKNOWN]
    )
    async def test_leaves_any_other_run_failure_a_500(
        self, monkeypatch: pytest.MonkeyPatch, code: AgentRunErrorCode
    ) -> None:
        monkeypatch.setattr(RefusedRunUseCase, "code", code)

        assert (await _get(ANONYMOUS)).status_code == HTTPStatus.INTERNAL_SERVER_ERROR
