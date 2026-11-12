"""What ``_ToolsetSession`` promises about a session that died after it opened.

Driven over toolset doubles, so each promise is one observable fact: how many
toolsets were built, which one served a call, and how often the tool ran.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from loom.ai.engines.pydantic_ai._failures import McpCallInterrupted
from loom.ai.engines.pydantic_ai._mcp import _ToolsetSession


class _Toolset:
    """A toolset double: counts its calls and fails them while ``dead``."""

    def __init__(self, label: str, *, dead: bool = False, connects: bool = True) -> None:
        self.label = label
        self.dead = dead
        self.connects = connects
        self.calls: list[str] = []
        self.entered = 0
        self.exited = 0
        self.client = SimpleNamespace(call_tool_mcp=self._call)

    async def __aenter__(self) -> _Toolset:
        if not self.connects:
            raise ConnectionError(f"{self.label} refused")
        self.entered += 1
        return self

    async def __aexit__(self, *args: object) -> None:
        self.exited += 1

    async def _call(self, name: str, arguments: dict[str, Any]) -> Any:
        self.calls.append(name)
        await asyncio.sleep(0)
        if self.dead:
            raise ConnectionError("Connection closed")
        return SimpleNamespace(is_error=name == "fails", structured_content={"by": self.label})


class _Factory:
    """Hands out the toolsets it was given, in order, and counts how many it built."""

    def __init__(self, *toolsets: _Toolset) -> None:
        self._toolsets = list(toolsets)
        self.built = 0

    def __call__(self) -> Any:
        self.built += 1
        return self._toolsets.pop(0)


async def _session(first: _Toolset, factory: _Factory | None) -> _ToolsetSession:
    """A session as ``SharedMcpToolsets.open`` makes one: holding the first toolset's entry."""
    await first.__aenter__()
    return _ToolsetSession(first, rebuild=factory)  # type: ignore[arg-type]


class TestTheCallThatFoundTheSessionDead:
    async def test_it_is_raised_as_interrupted_and_never_run_a_second_time(self) -> None:
        first, second = _Toolset("first", dead=True), _Toolset("second")
        factory = _Factory(second)
        session = await _session(first, factory)

        with pytest.raises(McpCallInterrupted) as raised:
            await session.call_tool("write_orders", {})

        assert isinstance(raised.value.__cause__, ConnectionError)

        assert first.calls == ["write_orders"]
        assert second.calls == []
        assert factory.built == 0

    async def test_the_next_call_is_served_by_a_new_toolset(self) -> None:
        first, second = _Toolset("first", dead=True), _Toolset("second")
        session = await _session(first, _Factory(second))
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("read_orders", {})

        result = await session.call_tool("read_orders", {})

        assert (result.ok, result.structured) == (True, {"by": "second"})
        assert second.entered - second.exited == 1
        assert first.entered - first.exited == 0

    async def test_listing_tools_also_goes_through_the_new_toolset(self) -> None:
        first, second = _Toolset("first", dead=True), _Toolset("second")
        second.list_tools = lambda: _async(  # type: ignore[attr-defined]
            [SimpleNamespace(name="read_orders", output_schema=None)]
        )
        session = await _session(first, _Factory(second))
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("read_orders", {})

        (tool,) = await session.list_tools()

        assert tool.name == "read_orders"


class TestWhatDoesNotRenewASession:
    async def test_a_tools_own_error_is_a_result_not_a_dead_session(self) -> None:
        first = _Toolset("first")
        factory = _Factory(_Toolset("never"))
        session = await _session(first, factory)

        failed = await session.call_tool("fails", {})
        await session.call_tool("read_orders", {})

        assert failed.ok is False
        assert factory.built == 0
        assert first.calls == ["fails", "read_orders"]

    async def test_a_session_without_a_rebuild_keeps_its_toolset_for_life(self) -> None:
        first = _Toolset("first", dead=True)
        session = await _session(first, None)

        for _ in range(2):
            with pytest.raises(McpCallInterrupted):
                await session.call_tool("read_orders", {})

        assert first.calls == ["read_orders", "read_orders"]


class TestRenewal:
    async def test_concurrent_calls_that_find_it_dead_renew_it_once(self) -> None:
        first, second = _Toolset("first", dead=True), _Toolset("second")
        factory = _Factory(second, _Toolset("one too many"))
        session = await _session(first, factory)
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("read_orders", {})

        results = await asyncio.gather(*(session.call_tool("read_orders", {}) for _ in range(8)))

        assert factory.built == 1
        assert [result.structured for result in results] == [{"by": "second"}] * 8

    async def test_a_renewal_that_cannot_connect_is_tried_again_by_the_next_call(self) -> None:
        first = _Toolset("first", dead=True)
        down, back = _Toolset("down", connects=False), _Toolset("back")
        factory = _Factory(down, back)
        session = await _session(first, factory)
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("read_orders", {})

        with pytest.raises(McpCallInterrupted) as refused:
            await session.call_tool("read_orders", {})
        assert str(refused.value.__cause__) == "down refused"
        result = await session.call_tool("read_orders", {})

        assert result.structured == {"by": "back"}
        assert factory.built == 2

    async def test_closing_the_session_closes_every_toolset_it_still_holds(self) -> None:
        first, second = _Toolset("first", dead=True), _Toolset("second")
        session = await _session(first, _Factory(second))
        with pytest.raises(McpCallInterrupted):
            await session.call_tool("read_orders", {})
        await session.call_tool("read_orders", {})

        held_before = (first.entered - first.exited, second.entered - second.exited)

        await session.aclose()

        assert held_before == (0, 1)
        assert (first.entered - first.exited, second.entered - second.exited) == (0, 0)


async def _async(value: Any) -> Any:
    return value
