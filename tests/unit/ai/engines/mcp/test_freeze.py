"""A thawed process drops its stale pooled connections and keeps its MCP session.

A frozen Lambda never sees the server close an idle connection, and httpcore2's
own keep-alive expiry reads a monotonic clock that did not move while frozen.
So the pool itself checks the wall clock before each request it sends: a gap
over 5 s since its last request drops the idle pool, once, before anything is
sent. The pool serves every caller of the toolset -- grant views and the
model's own tool calls alike -- so they all keep it warm and none is cut by a
drop. Driven against a real FastMCP server in process, wall clock patched.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import httpx2
import pytest

from loom.ai.engines.pydantic_ai import _mcp, _transport
from loom.ai.engines.pydantic_ai._mcp import SharedMcpToolsets
from loom.ai.engines.pydantic_ai._transport import EvictableTransport
from tests.unit.ai.engines.mcp.server import InProcessServer, serving

pytest.importorskip("fastmcp", reason="fastmcp is not installed: uv sync --group mcp-tests")

_HANDSHAKE = ("server/discover", "initialize", "tools/list")
_URL = "http://orders.test/mcp"


class _WallClock:
    """Stands in for the ``time`` module: a wall clock only, no ``monotonic``."""

    def __init__(self) -> None:
        self.now = 1_000_000.0

    def time(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _WallClock:
    wall = _WallClock()
    monkeypatch.setattr(_transport, "time", wall)
    return wall


@pytest.fixture
def built(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Every toolset built, so a renewal shows up as a second entry."""
    toolsets: list[object] = []
    build = _mcp.build_mcp_toolset

    def counting(*args: Any, **kwargs: Any) -> Any:
        toolsets.append(build(*args, **kwargs))
        return toolsets[-1]

    monkeypatch.setattr(_mcp, "build_mcp_toolset", counting)
    return toolsets


def _logging_drops(monkeypatch: pytest.MonkeyPatch, server: InProcessServer) -> None:
    drop = EvictableTransport.drop

    async def logged(self: EvictableTransport) -> None:
        server.log.append("drop")
        await drop(self)

    monkeypatch.setattr(EvictableTransport, "drop", logged)


Scenario = Callable[[InProcessServer, Any, Any], Awaitable[None]]


async def _after_warm_call(monkeypatch: pytest.MonkeyPatch, test: Scenario) -> InProcessServer:
    """Open the shared session as the runtime does, make one call, then run *test*, log cleared.

    *test* gets the server, the grant view's session and the raw toolset the
    model's own tool calls go through.
    """
    async with serving(monkeypatch) as server:
        _logging_drops(monkeypatch, server)
        shared = SharedMcpToolsets()
        async with shared.open(server.capability) as session:
            await session.list_tools()  # as the runtime does at start-up
            await session.call_tool("echo", {"text": "warm"})
            server.log.clear()
            await test(server, session, shared.for_build(server.capability, "agent"))
    return server


async def _model_call(toolset: Any, tool: str, arguments: dict[str, Any]) -> str:
    """One agent run whose model calls *tool* once through the engine's toolset; what it got."""
    from pydantic_ai import Agent
    from pydantic_ai.messages import (
        ModelMessage,
        ModelResponse,
        TextPart,
        ToolCallPart,
        ToolReturnPart,
    )
    from pydantic_ai.models.function import AgentInfo, FunctionModel

    def model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        del info
        if len(messages) == 1:
            return ModelResponse(parts=[ToolCallPart(tool, arguments)])
        returned = [part for part in messages[-1].parts if isinstance(part, ToolReturnPart)]
        return ModelResponse(parts=[TextPart(returned[0].model_response_str())])

    return (await Agent(FunctionModel(model), toolsets=[toolset]).run("go")).output


async def _started(server: InProcessServer, call: Awaitable[Any]) -> asyncio.Task[Any]:
    task = asyncio.ensure_future(call)
    while not server.runs["slow"]:
        await asyncio.sleep(0)
    return task


class TestTheGapSinceTheLastRequest:
    async def test_up_to_five_seconds_drops_nothing(
        self, monkeypatch: pytest.MonkeyPatch, clock: _WallClock
    ) -> None:
        async def call_five_seconds_later(server: InProcessServer, session: Any, _: Any) -> None:
            clock.now += 5.0
            await session.call_tool("echo", {"text": "a"})

        server = await _after_warm_call(monkeypatch, call_five_seconds_later)

        assert server.log == ["tools/call"]

    async def test_over_five_seconds_idle_drops_once_before_sending_and_keeps_the_session(
        self, monkeypatch: pytest.MonkeyPatch, clock: _WallClock, built: list[object]
    ) -> None:
        async def call_after_a_freeze(server: InProcessServer, session: Any, _: Any) -> None:
            clock.now += 5.001
            result = await session.call_tool("echo", {"text": "thawed"})
            assert result.structured == {"result": "thawed"}

        server = await _after_warm_call(monkeypatch, call_after_a_freeze)

        assert server.log == ["drop", "tools/call"]
        assert server.count(*_HANDSHAKE) == 0
        assert len(built) == 1

    async def test_two_calls_after_the_gap_drop_once(
        self, monkeypatch: pytest.MonkeyPatch, clock: _WallClock
    ) -> None:
        async def two_calls_after_a_freeze(server: InProcessServer, session: Any, _: Any) -> None:
            clock.now += 60.0
            await asyncio.gather(*(session.call_tool("echo", {"text": "x"}) for _ in range(2)))

        server = await _after_warm_call(monkeypatch, two_calls_after_a_freeze)

        assert server.log == ["drop", "tools/call", "tools/call"]

    async def test_a_call_in_flight_keeps_the_pool_and_completes(
        self, monkeypatch: pytest.MonkeyPatch, clock: _WallClock
    ) -> None:
        async def call_beside_a_slow_one(server: InProcessServer, session: Any, _: Any) -> None:
            slow = await _started(server, session.call_tool("slow", {}))
            clock.now += 60.0
            await session.call_tool("echo", {"text": "beside"})
            server.release.set()
            assert (await slow).structured == {"result": "done"}

        server = await _after_warm_call(monkeypatch, call_beside_a_slow_one)

        assert "drop" not in server.log
        assert server.runs["slow"] == 1

    async def test_a_call_that_took_longer_than_the_gap_does_not_drop_the_next(
        self, monkeypatch: pytest.MonkeyPatch, clock: _WallClock
    ) -> None:
        async def call_after_a_slow_one(server: InProcessServer, session: Any, _: Any) -> None:
            slow = await _started(server, session.call_tool("slow", {}))
            clock.now += 60.0
            server.release.set()
            await slow
            await session.call_tool("echo", {"text": "next"})

        server = await _after_warm_call(monkeypatch, call_after_a_slow_one)

        assert "drop" not in server.log


class TestTheModelsOwnCalls:
    async def test_keep_the_pool_warm_for_the_grant_view(
        self, monkeypatch: pytest.MonkeyPatch, clock: _WallClock
    ) -> None:
        async def a_busy_minute(server: InProcessServer, session: Any, toolset: Any) -> None:
            for _ in range(6):
                for _ in range(6):
                    clock.now += 1.0
                    await _model_call(toolset, "echo", {"text": "model"})
                await session.call_tool("echo", {"text": "view"})

        server = await _after_warm_call(monkeypatch, a_busy_minute)

        assert "drop" not in server.log
        assert server.count("tools/call") == 42

    async def test_in_flight_are_never_cut_by_a_grant_views_drop(
        self, monkeypatch: pytest.MonkeyPatch, clock: _WallClock
    ) -> None:
        async def view_call_beside_a_slow_model_call(
            server: InProcessServer, session: Any, toolset: Any
        ) -> None:
            slow = await _started(server, _model_call(toolset, "slow", {}))
            clock.now += 10.0
            await session.call_tool("echo", {"text": "view"})
            server.release.set()
            assert "done" in await slow

        server = await _after_warm_call(monkeypatch, view_call_beside_a_slow_model_call)

        assert "drop" not in server.log

    async def test_after_a_gap_drop_the_stale_pool_too(
        self, monkeypatch: pytest.MonkeyPatch, clock: _WallClock
    ) -> None:
        async def model_call_after_a_freeze(_: InProcessServer, __: Any, toolset: Any) -> None:
            clock.now += 60.0
            await _model_call(toolset, "echo", {"text": "thawed"})

        server = await _after_warm_call(monkeypatch, model_call_after_a_freeze)

        assert server.log == ["drop", "tools/call"]


class _Body(httpx2.AsyncByteStream):
    """A body still to be read, as httpx2's own pool hands one over."""

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"{}"


class _Answering(httpx2.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, stream=_Body(), request=request)


class TestThePoolAlone:
    async def test_reads_the_wall_clock_only(self, clock: _WallClock) -> None:
        """The clock a frozen process stops is never consulted: ``_WallClock`` has no monotonic."""
        pools: list[_Answering] = []
        transport = EvictableTransport(inner=lambda: pools.append(_Answering()) or pools[-1])

        clock.now += 6.0
        await transport.handle_async_request(httpx2.Request("POST", _URL))

        assert len(pools) == 2

    async def test_a_get_stream_left_open_does_not_hold_the_pool_forever(
        self, clock: _WallClock
    ) -> None:
        """The handshake era's GET stream lives as long as the session; the client reopens it."""
        pools: list[_Answering] = []
        transport = EvictableTransport(inner=lambda: pools.append(_Answering()) or pools[-1])
        await transport.handle_async_request(httpx2.Request("GET", _URL))

        clock.now += 60.0
        await transport.handle_async_request(httpx2.Request("POST", _URL))

        assert len(pools) == 2

    async def test_a_post_holds_the_pool_until_its_response_is_closed(
        self, clock: _WallClock
    ) -> None:
        pools: list[_Answering] = []
        transport = EvictableTransport(inner=lambda: pools.append(_Answering()) or pools[-1])
        streaming = await transport.handle_async_request(httpx2.Request("POST", _URL))

        clock.now += 60.0
        beside = await transport.handle_async_request(httpx2.Request("POST", _URL))
        held = len(pools)
        await beside.aclose()
        await streaming.aclose()
        clock.now += 60.0
        await transport.handle_async_request(httpx2.Request("POST", _URL))

        assert (held, len(pools)) == (1, 2)

    async def test_a_body_read_already_does_not_hold_the_pool(self, clock: _WallClock) -> None:
        pools: list[httpx2.AsyncBaseTransport] = []

        def answered() -> httpx2.AsyncBaseTransport:
            pools.append(httpx2.MockTransport(lambda request: httpx2.Response(200, json={})))
            return pools[-1]

        transport = EvictableTransport(inner=answered)
        await transport.handle_async_request(httpx2.Request("POST", _URL))

        clock.now += 60.0
        await transport.handle_async_request(httpx2.Request("POST", _URL))

        assert len(pools) == 2

    async def test_a_request_arriving_during_a_drop_does_not_drop_again(
        self, clock: _WallClock
    ) -> None:
        """The gap is stamped as a request starts, before the drop yields to anyone."""
        pools: list[_SlowToClose] = []
        transport = EvictableTransport(inner=lambda: pools.append(_SlowToClose()) or pools[-1])

        clock.now += 60.0
        await asyncio.gather(
            *(transport.handle_async_request(httpx2.Request("POST", _URL)) for _ in range(2))
        )

        assert len(pools) == 2

    async def test_a_request_cancelled_before_its_headers_does_not_hold_the_pool(
        self, clock: _WallClock
    ) -> None:
        pools: list[httpx2.AsyncBaseTransport] = []
        hanging = _Hanging()
        transport = EvictableTransport(
            inner=lambda: pools.append(hanging if not pools else _Answering()) or pools[-1]
        )
        waiting = asyncio.ensure_future(
            transport.handle_async_request(httpx2.Request("POST", _URL))
        )
        await hanging.entered.wait()
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting

        clock.now += 60.0
        await transport.handle_async_request(httpx2.Request("POST", _URL))

        assert len(pools) == 2

    async def test_a_body_of_many_chunks_reaches_the_caller_whole(self) -> None:
        transport = EvictableTransport(inner=_Chunked)

        async with httpx2.AsyncClient(transport=transport) as client:
            response = await client.post(_URL)

        assert response.content == b"one,two,three"


class _SlowToClose(_Answering):
    """A pool whose close yields, as httpcore2's does while it shuts its sockets."""

    async def aclose(self) -> None:
        await asyncio.sleep(0)


class _Hanging(httpx2.AsyncBaseTransport):
    """A pool that never answers: the server has not sent its headers yet."""

    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        self.entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class _Chunks(httpx2.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in (b"one,", b"two,", b"three"):
            yield chunk


class _Chunked(httpx2.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, stream=_Chunks(), request=request)


class TestADrop:
    async def test_swaps_the_new_pool_in_before_the_old_one_finishes_closing(
        self, clock: _WallClock
    ) -> None:
        """A request arriving while the old pool still closes already goes to the new one."""
        closing = asyncio.Event()
        old, new = _ClosingSlowly(closing), _Answering()
        pools = iter([old, new])
        transport = EvictableTransport(inner=lambda: next(pools))

        clock.now += 60.0
        dropping = asyncio.ensure_future(
            transport.handle_async_request(httpx2.Request("POST", _URL))
        )
        await _until_waiting(closing)
        beside = await transport.handle_async_request(httpx2.Request("POST", _URL))
        closing.set()
        await dropping

        assert (old.sent, beside.status_code) == (0, 200)


class _ClosingSlowly(_Answering):
    """A pool that is still closing until a test lets it finish, and counts what it sent."""

    def __init__(self, closing: asyncio.Event) -> None:
        self.closing = closing
        self.sent = 0

    async def handle_async_request(self, request: httpx2.Request) -> httpx2.Response:
        self.sent += 1
        return await super().handle_async_request(request)

    async def aclose(self) -> None:
        await self.closing.wait()


async def _until_waiting(event: asyncio.Event) -> None:
    while not event._waiters:
        await asyncio.sleep(0)
