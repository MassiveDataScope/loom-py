"""Acme shipping over HTTP, on FastAPI and on the Lambda ``http`` function.

H1 on both, the callers each refuses, H7 over concurrent FastAPI requests,
H5/H6, the start-up refusal on every entry point, and the swap of the
tracking adapter, which changes only the composition root.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from loom.ai.errors import AgentCompilationError
from loom.composition import Application
from loom.core.identity import ANONYMOUS, Identity
from loom.testing import LambdaTestClient
from tests.integration.hexagonal.adapters import (
    NOTICE_WRITER,
    TRACK_TOOL,
    TRACKING_SERVER,
    http_root,
)
from tests.integration.hexagonal.conftest import (
    ANA,
    BO,
    CALLERS,
    KEY_HEADER,
    PATH_TO_GATEWAY,
    STATUS,
    TRANSPORTS,
    Compose,
    Seen,
    Stubs,
    acted_as,
    key_of,
    report_for,
)
from tests.integration.hexagonal.shipping import TrackShipment

HTTP_CLIENTS: dict[str, Callable[[Any], Any]] = {
    "fastapi": TestClient,
    "lambda-http": LambdaTestClient,
}


@pytest.fixture(params=list(HTTP_CLIENTS))
def http_client(request: pytest.FixtureRequest, compose: Compose) -> Iterator[httpx.Client]:
    """A client of acme shipping served by FastAPI, then by the Lambda ``http`` function."""
    _, entry = compose(request.param)
    with HTTP_CLIENTS[request.param](entry) as client:
        yield client


def _track(client: httpx.Client, headers: dict[str, str]) -> httpx.Response:
    return client.get("/shipments/ACME-1", headers=headers)


async def _served(application: Application, app: Any, headers: dict[str, str]) -> object:
    async with (
        application.running(),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://acme.test"
        ) as client,
    ):
        return (await client.get("/shipments/ACME-1", headers=headers)).json()


class TestTrackShipmentOverHttp:
    @CALLERS
    def test_answers_each_caller_through_adapters_acting_as_them(
        self, http_client: httpx.Client, stubs: Stubs, seen: Seen, caller: Identity
    ) -> None:
        response = _track(http_client, key_of(caller))

        assert (response.status_code, response.json()) == (200, report_for(caller))
        assert seen.calls == acted_as(caller)
        assert [(d.server, d.include) for d in stubs.mcp.declarations] == [
            (TRACKING_SERVER, (TRACK_TOOL,))
        ]
        assert [d.name for d in stubs.agent.declarations] == [NOTICE_WRITER]

    @pytest.mark.parametrize(
        "headers",
        [{}, {KEY_HEADER: "key-carol"}, {KEY_HEADER: "KEY-ANA"}],
        ids=["no-key", "unknown-key", "key-in-another-case"],
    )
    def test_refuses_a_request_without_a_caller_before_reaching_an_adapter(
        self, http_client: httpx.Client, seen: Seen, headers: dict[str, str]
    ) -> None:
        assert _track(http_client, headers).status_code == 401
        assert seen.calls == []

    def test_refuses_an_anonymous_caller_at_the_first_handle(
        self, http_client: httpx.Client, seen: Seen
    ) -> None:
        assert _track(http_client, key_of(ANONYMOUS)).is_error
        assert seen.calls == []

    def test_answers_an_anonymous_caller_401(self, http_client: httpx.Client) -> None:
        assert _track(http_client, key_of(ANONYMOUS)).status_code == 401


class TestConcurrentRequests:
    async def test_never_see_each_others_caller(self, compose: Compose, seen: Seen) -> None:
        application, app = compose("fastapi")
        seen.rendezvous = asyncio.Barrier(2)

        async with (
            application.running(),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://acme.test"
            ) as client,
        ):
            ana, bo = await asyncio.gather(
                client.get("/shipments/ACME-1", headers=key_of(ANA)),
                client.get("/shipments/ACME-2", headers=key_of(BO)),
            )

        assert ana.json() == report_for(ANA)
        assert bo.json() == report_for(BO, "ACME-2")
        assert sorted(seen.calls, key=repr) == sorted(acted_as(ANA, BO), key=repr)


class TestStartupWithoutTheAgentRuntime:
    @pytest.mark.parametrize("transport", TRANSPORTS)
    def test_refuses_the_mcp_server_an_adapter_declares_naming_its_path(
        self, compose_strictly: Callable[..., Application], transport: str
    ) -> None:
        with pytest.raises(AgentCompilationError, match=PATH_TO_GATEWAY):
            compose_strictly(transport)


class TestSwappingTheTrackingAdapter:
    async def test_changes_only_the_composition_root(self, compose: Compose, stubs: Stubs) -> None:
        asked: list[str] = []

        def tracking_api(request: httpx.Request) -> httpx.Response:
            asked.append(request.url.path)
            return httpx.Response(200, json={"status": STATUS})

        transport = httpx.MockTransport(tracking_api)
        async with httpx.AsyncClient(
            transport=transport, base_url="https://tracking.acme.test"
        ) as tracking:
            builds = (compose("fastapi"), compose("fastapi", root=http_root(tracking)))
            assert {application.discovered.use_cases for application, _ in builds} == {
                (TrackShipment,)
            }
            replies = [await _served(application, app, key_of(ANA)) for application, app in builds]

        assert replies == [report_for(ANA), report_for(ANA)]
        assert asked == ["/shipments/ACME-1"]
        assert len(stubs.mcp.declarations) == 1
        assert len(stubs.agent.declarations) == 2

    async def test_leaves_only_the_agent_declared(
        self, compose_strictly: Callable[..., Application]
    ) -> None:
        async with httpx.AsyncClient() as tracking:
            with pytest.raises(AgentCompilationError) as refused:
                compose_strictly("fastapi", root=http_root(tracking))

        assert NOTICE_WRITER in str(refused.value)
        assert TRACKING_SERVER not in str(refused.value)
