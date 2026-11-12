"""What the shared ``_ToolsetSession`` does with a failed call, and how many toolsets it keeps.

Driven over toolset doubles, so each promise is one observable fact: how many
toolsets were built, which one served a call, how often the tool was reached,
and how many toolsets were open at once.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import pytest

from loom.ai.abc import McpToolInfo
from loom.ai.compiler import CompiledMcpCapability
from loom.ai.engines.pydantic_ai._failures import McpCallInterrupted
from loom.ai.engines.pydantic_ai._mcp import _entered, _ToolsetSession
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.ai.runtime._grants import McpGrantView
from loom.core.identity import Identity

pytest.importorskip("mcp", reason="the MCP client is not installed: uv sync --group mcp-tests")

from mcp import MCPError  # noqa: E402


def _forgotten() -> MCPError:
    """The server does not know the session: nothing ran, so it is safe to send again."""
    return MCPError(-32600, "Session not found")


def _cut() -> MCPError:
    """The connection closed under the call: it may have run."""
    return MCPError(-32000, "Connection closed")


def _answered() -> MCPError:
    """A JSON-RPC error the server itself sent."""
    return MCPError(-32043, "middleware-sent JSON-RPC error", {"why": "test"})


class _Ledger:
    """Counts the toolsets open at once across every double sharing it."""

    def __init__(self) -> None:
        self.toolsets: list[_Toolset] = []
        self.peak = 0

    def note(self) -> None:
        self.peak = max(self.peak, sum(toolset.is_open for toolset in self.toolsets))


class _Pool:
    """The toolset's HTTP pool double: counts how often it was released."""

    def __init__(self) -> None:
        self.released = 0
        self.fails = False

    async def release(self) -> None:
        self.released += 1
        if self.fails:
            raise OSError("the socket was already gone")


class _Toolset:
    """A toolset double; each call takes the next step of its script.

    A step is ``None`` (answer), an exception (raise it) or an event (wait for
    it, unless the client is closed first). Its client carries the pool the
    builder made, where ``build_mcp_toolset``'s own transport carries it.
    """

    def __init__(self, label: str, ledger: _Ledger, *script: object, connects: bool = True) -> None:
        self.label = label
        self.ledger = ledger
        self.script = list(script)
        self.connects = connects
        self.reached: list[str] = []
        self.holds = 0
        self.closes = 0
        self.over_exits = 0
        self.closed = asyncio.Event()
        self.active = 0
        self.drained = asyncio.Event()
        self.drained.set()
        self.enter_gate: asyncio.Event | None = None
        self.exit_gate: asyncio.Event | None = None
        self.pool = _Pool()
        self.client = SimpleNamespace(
            call_tool_mcp=self._call, close=self._close, transport=SimpleNamespace(pool=self.pool)
        )
        ledger.toolsets.append(self)

    @property
    def is_open(self) -> bool:
        return self.holds > 0 and not self.closed.is_set()

    async def __aenter__(self) -> _Toolset:
        if not self.connects:
            raise ConnectionError(f"{self.label} refused")
        if self.enter_gate is not None:
            await self.enter_gate.wait()
        self.holds += 1
        self.ledger.note()
        return self

    async def __aexit__(self, *args: object) -> None:
        if self.holds == 0:
            self.over_exits += 1
            return
        self.holds -= 1
        if self.holds == 0:
            if self.exit_gate is not None:
                await self.exit_gate.wait()
            self.closes += 1

    async def _close(self) -> None:
        """Cut the connection; like fastmcp, return only once the cut calls have drained."""
        self.closed.set()
        await self.drained.wait()

    async def _call(self, name: str, arguments: dict[str, Any]) -> Any:
        self.reached.append(name)
        step = self.script.pop(0) if self.script else None
        self.active += 1
        self.drained.clear()
        try:
            await asyncio.sleep(0)
            if isinstance(step, asyncio.Event):
                await _first_of(step, self.closed)
                if self.closed.is_set():
                    raise _cut()
            if isinstance(step, Exception):
                raise step
            return SimpleNamespace(is_error=name == "fails", structured_content={"by": self.label})
        finally:
            self.active -= 1
            if not self.active:
                self.drained.set()


async def _first_of(*events: asyncio.Event) -> None:
    waits = [asyncio.create_task(event.wait()) for event in events]
    _, pending = await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()


class _Factory:
    """Hands out the toolsets it was given, in order, and counts how many it built."""

    def __init__(self, *toolsets: _Toolset) -> None:
        self._toolsets = list(toolsets)
        self.built = 0

    def __call__(self) -> Any:
        self.built += 1
        return self._toolsets.pop(0)


def _bounded(test: Callable[..., Awaitable[None]]) -> Callable[..., Awaitable[None]]:
    """Fail a hung test in two seconds rather than at the suite's own timeout."""

    @functools.wraps(test)
    async def bounded(*args: Any, **kwargs: Any) -> None:
        async with asyncio.timeout(2):
            await test(*args, **kwargs)

    return bounded


async def _drained(session: _ToolsetSession) -> None:
    """Let the closes the session handed off finish."""
    await asyncio.gather(*session._closing)


async def _open(first: _Toolset, factory: _Factory) -> _ToolsetSession:
    """A session as ``SharedMcpToolsets.open`` makes one: over an entered first toolset."""
    await first.__aenter__()
    return _ToolsetSession(first, rebuild=factory)  # type: ignore[arg-type]


def _view(session: _ToolsetSession, *, timeout_s: float = 5.0) -> McpGrantView:
    """The ``Mcp()`` marker's view over the session."""
    return McpGrantView(
        span_attributes={"mcp_server": "orders"},
        capability=CompiledMcpCapability(server="orders", url="https://orders.test/mcp"),
        include=(),
        exclude=(),
        session=session,
        catalogue=(
            McpToolInfo(name="write_orders", has_output_schema=False),
            McpToolInfo(name="slow", has_output_schema=False),
        ),
        timeout_s=timeout_s,
        identity=Identity(subject="tester"),
        observability=None,
    )


class TestASafeFailure:
    async def test_is_sent_once_more_on_one_new_toolset(self) -> None:
        ledger = _Ledger()
        first, second = _Toolset("first", ledger, _forgotten()), _Toolset("second", ledger)
        factory = _Factory(second)
        session = await _open(first, factory)

        result = await session.call_tool("write_orders", {})

        assert result.structured == {"by": "second"}
        assert factory.built == 1
        assert (first.reached, second.reached) == (["write_orders"], ["write_orders"])

    @pytest.mark.parametrize("again", [_forgotten, _cut], ids=["safe", "unsafe"])
    async def test_whose_retry_fails_too_is_interrupted_and_never_sent_a_third_time(
        self, again: Any
    ) -> None:
        ledger = _Ledger()
        first, second = _Toolset("first", ledger, _forgotten()), _Toolset("second", ledger, again())
        third = _Toolset("third", ledger)
        factory = _Factory(second, third)
        session = await _open(first, factory)

        with pytest.raises(AgentRunError) as raised:
            await _view(session).call_untyped("write_orders", {})

        assert raised.value.code is AgentRunErrorCode.TOOL_UNAVAILABLE
        assert isinstance(raised.value.__cause__, McpCallInterrupted)
        assert len(first.reached) + len(second.reached) == 2
        assert factory.built == 1
        assert (await session.call_tool("write_orders", {})).structured == {"by": "third"}

    async def test_whose_retry_the_server_answers_with_an_error_passes_that_error(self) -> None:
        ledger = _Ledger()
        first, second = (
            _Toolset("first", ledger, _forgotten()),
            _Toolset("second", ledger, _answered()),
        )
        factory = _Factory(second)
        session = await _open(first, factory)

        with pytest.raises(MCPError) as raised:
            await session.call_tool("write_orders", {})

        assert raised.value.code == -32043
        assert (await session.call_tool("write_orders", {})).structured == {"by": "second"}
        assert factory.built == 1

    async def test_two_concurrent_calls_that_find_it_expired_rebuild_once(self) -> None:
        ledger = _Ledger()
        first = _Toolset("first", ledger, _forgotten(), _forgotten())
        second = _Toolset("second", ledger)
        factory = _Factory(second, _Toolset("one too many", ledger))
        session = await _open(first, factory)

        results = await asyncio.gather(*(session.call_tool("write_orders", {}) for _ in range(2)))

        assert factory.built == 1
        assert [result.structured for result in results] == [{"by": "second"}] * 2


class TestAnUnsafeFailure:
    async def test_reaches_the_marker_once_as_tool_unavailable(self) -> None:
        ledger = _Ledger()
        first, second = _Toolset("first", ledger, _cut()), _Toolset("second", ledger)
        factory = _Factory(second)
        session = await _open(first, factory)

        with pytest.raises(AgentRunError) as raised:
            await _view(session).call_untyped("write_orders", {})

        assert raised.value.code is AgentRunErrorCode.TOOL_UNAVAILABLE
        assert first.reached == ["write_orders"]
        assert factory.built == 0

    async def test_the_next_call_renews_first_and_succeeds(self) -> None:
        ledger = _Ledger()
        first, second = _Toolset("first", ledger, _cut()), _Toolset("second", ledger)
        factory = _Factory(second)
        session = await _open(first, factory)
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})

        result = await session.call_tool("write_orders", {})

        assert result.structured == {"by": "second"}
        assert (first.reached, factory.built) == (["write_orders"], 1)


class TestACompletedCall:
    async def test_a_tools_own_error_is_a_result(self) -> None:
        ledger = _Ledger()
        first = _Toolset("first", ledger)
        factory = _Factory()
        session = await _open(first, factory)

        result = await session.call_tool("fails", {})

        assert result.ok is False
        assert factory.built == 0

    async def test_a_server_sent_error_outside_the_client_set_passes_unchanged(self) -> None:
        ledger = _Ledger()
        first = _Toolset("first", ledger, _answered())
        factory = _Factory()
        session = await _open(first, factory)

        with pytest.raises(MCPError) as raised:
            await session.call_tool("write_orders", {})
        again = await session.call_tool("write_orders", {})

        assert raised.value.code == -32043
        assert (again.structured, factory.built) == ({"by": "first"}, 0)


class TestBoundedSessions:
    @_bounded
    async def test_a_slow_call_on_the_old_toolset_completes_and_closes_it_once_after(
        self,
    ) -> None:
        ledger, gate = _Ledger(), asyncio.Event()
        first = _Toolset("first", ledger, gate, _cut())
        second = _Toolset("second", ledger)
        session = await _open(first, _Factory(second))
        slow = asyncio.create_task(session.call_tool("slow", {}))
        await asyncio.sleep(0)
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})
        await session.call_tool("write_orders", {})
        closes_while_draining = first.closes

        gate.set()

        assert (await slow).structured == {"by": "first"}
        await _drained(session)
        assert (closes_while_draining, first.closes) == (0, 1)
        assert first.pool.released == 1

    @_bounded
    async def test_an_idle_old_toolset_is_closed_at_the_swap(self) -> None:
        ledger = _Ledger()
        first, second = _Toolset("first", ledger, _cut()), _Toolset("second", ledger)
        session = await _open(first, _Factory(second))
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})

        await session.call_tool("write_orders", {})

        assert (first.closes, first.holds, second.closes) == (1, 0, 0)
        assert (first.pool.released, second.pool.released) == (1, 0)

    @_bounded
    async def test_a_second_renewal_force_closes_the_one_still_draining(self) -> None:
        ledger, gate = _Ledger(), asyncio.Event()
        first = _Toolset("first", ledger, gate, _cut())
        second = _Toolset("second", ledger, _cut())
        third = _Toolset("third", ledger)
        session = await _open(first, _Factory(second, third))
        slow = asyncio.create_task(session.call_tool("slow", {}))
        await asyncio.sleep(0)
        for _ in range(2):
            with pytest.raises(McpCallInterrupted):
                await session.call_tool("write_orders", {})

        result = await session.call_tool("write_orders", {})

        with pytest.raises(McpCallInterrupted):
            await slow
        assert result.structured == {"by": "third"}
        assert first.reached.count("slow") == 1
        assert first.closed.is_set()
        assert ledger.peak <= 2
        await _drained(session)
        assert (first.over_exits, first.holds, first.closes, first.pool.released) == (0, 0, 1, 1)

    @_bounded
    async def test_closing_the_session_closes_every_toolset_it_holds(self) -> None:
        ledger, gate = _Ledger(), asyncio.Event()
        first = _Toolset("first", ledger, gate, _cut())
        second = _Toolset("second", ledger)
        session = await _open(first, _Factory(second))
        slow = asyncio.create_task(session.call_tool("slow", {}))
        await asyncio.sleep(0)
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})
        await session.call_tool("write_orders", {})

        await session.aclose()

        with pytest.raises(McpCallInterrupted):
            await slow
        assert [toolset.is_open for toolset in ledger.toolsets] == [False, False]
        assert ledger.peak <= 2
        assert [toolset.pool.released for toolset in ledger.toolsets] == [1, 1]

    @_bounded
    async def test_a_deadline_during_the_close_after_a_slow_call_keeps_its_result(self) -> None:
        ledger, gate = _Ledger(), asyncio.Event()
        first = _Toolset("first", ledger, gate, _cut())
        second = _Toolset("second", ledger)
        session = await _open(first, _Factory(second))
        slow = asyncio.create_task(_view(session, timeout_s=0.2).call_untyped("slow", {}))
        await asyncio.sleep(0)
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})
        await session.call_tool("write_orders", {})
        first.exit_gate = asyncio.Event()

        gate.set()

        assert await slow == {"by": "first"}
        await asyncio.sleep(0.3)
        first.exit_gate.set()
        await _drained(session)
        assert (first.closes, first.over_exits, first.pool.released) == (1, 0, 1)

    @_bounded
    async def test_a_call_after_closing_fails_fast_without_renewing(self) -> None:
        ledger = _Ledger()
        first, second = _Toolset("first", ledger, _cut()), _Toolset("second", ledger)
        factory = _Factory(second)
        session = await _open(first, factory)
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})

        await session.aclose()

        with pytest.raises(McpCallInterrupted) as refused:
            await session.call_tool("write_orders", {})
        assert (factory.built, first.reached) == (0, ["write_orders"])
        assert _says_not_sent(refused.value)

    @_bounded
    async def test_a_renewal_that_cannot_connect_releases_the_pool_it_made(self) -> None:
        ledger = _Ledger()
        first = _Toolset("first", ledger, _cut())
        down = _Toolset("down", ledger, connects=False)
        session = await _open(first, _Factory(down))
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})

        with pytest.raises(McpCallInterrupted) as refused:
            await session.call_tool("write_orders", {})

        assert (down.pool.released, first.pool.released) == (1, 0)
        assert _says_not_sent(refused.value)

    @_bounded
    async def test_closing_during_a_renewal_closes_the_new_toolset_and_sends_nothing(
        self,
    ) -> None:
        ledger, connecting = _Ledger(), asyncio.Event()
        first, second = _Toolset("first", ledger, _cut()), _Toolset("second", ledger)
        second.enter_gate = connecting
        session = await _open(first, _Factory(second))
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})
        renewing = asyncio.ensure_future(session.call_tool("write_orders", {}))
        await _until(lambda: bool(connecting._waiters))

        await session.aclose()
        connecting.set()

        with pytest.raises(McpCallInterrupted) as refused:
            await renewing
        await _drained(session)
        assert _says_not_sent(refused.value)
        assert second.reached == []
        assert (first.closes, first.over_exits, first.pool.released) == (1, 0, 1)
        assert (second.holds, second.closes, second.pool.released) == (0, 1, 1)

    @_bounded
    async def test_a_cancelled_close_still_closes_every_toolset(self) -> None:
        ledger, exiting = _Ledger(), asyncio.Event()
        first = _Toolset("first", ledger)
        first.exit_gate = exiting
        session = await _open(first, _Factory())
        closing = asyncio.ensure_future(session.aclose())
        await _until(lambda: bool(exiting._waiters))

        closing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closing
        exiting.set()

        await _drained(session)
        assert (first.closes, first.pool.released) == (1, 1)

    @_bounded
    async def test_a_drained_old_toolset_is_never_closed_again(self) -> None:
        ledger, gate = _Ledger(), asyncio.Event()
        first = _Toolset("first", ledger, gate, _cut())
        second = _Toolset("second", ledger)
        session = await _open(first, _Factory(second))
        slow = asyncio.create_task(session.call_tool("slow", {}))
        await asyncio.sleep(0)
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})
        await session.call_tool("write_orders", {})
        gate.set()
        await slow
        await _drained(session)

        await session.aclose()

        assert (first.closes, first.over_exits, first.pool.released) == (1, 0, 1)

    @_bounded
    async def test_a_caller_cancelled_during_the_idle_close_leaves_it_to_finish(self) -> None:
        ledger, exiting = _Ledger(), asyncio.Event()
        first, second = _Toolset("first", ledger, _cut()), _Toolset("second", ledger)
        session = await _open(first, _Factory(second))
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("write_orders", {})
        first.exit_gate = exiting
        renewing = asyncio.ensure_future(session.call_tool("write_orders", {}))
        await _until(lambda: bool(exiting._waiters))

        renewing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await renewing
        exiting.set()

        await _drained(session)
        assert (first.closes, first.pool.released) == (1, 1)

    @_bounded
    async def test_a_pool_that_fails_to_close_is_logged_not_raised(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        first = _Toolset("first", _Ledger())
        first.pool.fails = True
        session = await _open(first, _Factory())

        await session.aclose()

        assert (first.closes, first.pool.released) == (1, 1)
        assert "did not close cleanly" in caplog.text

    @_bounded
    async def test_a_healthy_session_once_closed_sends_nothing(self) -> None:
        first = _Toolset("first", _Ledger())
        session = await _open(first, _Factory())

        await session.aclose()

        with pytest.raises(McpCallInterrupted) as refused:
            await session.call_tool("write_orders", {})
        assert first.reached == []
        assert _says_not_sent(refused.value)


async def test_a_first_connect_that_fails_releases_the_pool() -> None:
    down = _Toolset("down", _Ledger(), connects=False)

    with pytest.raises(ConnectionError):
        async with _entered(down):  # type: ignore[arg-type]
            pass

    assert down.pool.released == 1


def _says_not_sent(error: McpCallInterrupted) -> bool:
    """A call refused before it was sent says so, never that it may have run."""
    return "not sent" in str(error) and "may have run" not in str(error)


async def _until(ready: Callable[[], bool]) -> None:
    while not ready():
        await asyncio.sleep(0)
