"""An agent run is an execution acting as its caller, opened by the host around the run."""

from __future__ import annotations

from typing import Any

import httpx

from loom.core.execution import current_execution
from loom.core.identity import current_caller

from .doubles import AGENT, ALICE, PREFIX, AcmeActingEngine


class TestARun:
    async def test_acts_as_its_caller_while_the_engine_produces_events(
        self, make_runtime: Any
    ) -> None:
        engine = AcmeActingEngine()

        async with make_runtime(engine) as host:
            await host.run(AGENT, "p", identity=ALICE)

        assert engine.acting == [ALICE, ALICE]
        assert engine.in_execution == [True, True]
        assert current_execution() is None
        assert current_caller() is None

    async def test_a_streamed_run_acts_as_its_caller_until_the_stream_is_closed(
        self, make_runtime: Any
    ) -> None:
        engine = AcmeActingEngine()

        async with make_runtime(engine) as host:
            async with host.run_stream(AGENT, "p", identity=ALICE) as events:
                inside = [current_execution() is not None async for _ in events]
            after = current_execution()

        assert inside == [True, True]
        assert engine.acting == [ALICE, ALICE]
        assert after is None


class TestTheHttpStream:
    async def test_runs_as_the_request_caller_with_no_execution_opened_by_the_endpoint(
        self, make_runtime: Any, serving: Any
    ) -> None:
        engine = AcmeActingEngine()

        async with make_runtime(engine) as host, serving(host) as app:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://acme") as client:
                response = await client.post(f"{PREFIX}/{AGENT}/stream", json={"prompt": "p"})

        assert response.status_code == 200
        assert "event: final" in response.text
        assert engine.acting == [ALICE, ALICE]
