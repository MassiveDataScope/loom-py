"""A session declares whether it needs serialisation; the host stops deciding it.

``mcp_session_for`` is the only point that decides whether to wrap a session in
``SharedMcpSession``: it does so by default, and leaves the session as-is when it
declares :class:`~loom.ai.abc.ConcurrentMcpSession`. These tests measure what
matters -- that several concurrent calls on a declared session really do run in
parallel, not merely that the lock is gone -- and pin the opposite: an
undeclared session keeps serialising. They also cover cancellation in both
shapes, which does not behave the same way: ``SharedMcpSession`` still
drains the cancelled call before releasing its lock, so its neighbours on the
same session remain usable; a session declared concurrent holds no lock to
protect, so cancelling it returns to the caller immediately instead of
draining.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from types import CoroutineType
from typing import Any

from loom.ai.abc import ConcurrentMcpSession, McpToolCallResult, McpToolInfo
from loom.ai.execution._mcp import SharedMcpSession, mcp_session_for
from tests.helpers.gates import CallGate, await_cancelled


class _GatedSession:
    """A plain ``McpSession`` double: declares nothing, so it keeps queuing."""

    def __init__(self, gate: CallGate) -> None:
        self._gate = gate
        self.finished: list[str] = []

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return ()

    async def call_tool(self, name: str, arguments: Mapping[str, Any]) -> McpToolCallResult:
        await self._gate.enter(name)
        self.finished.append(name)
        return McpToolCallResult(ok=True, structured=None)


class _CountingSession:
    """Counts every ``call_tool`` invocation -- i.e. every coroutine built.

    ``call_tool`` is a plain function returning a coroutine, not an
    ``async def`` itself: calling it builds and returns the coroutine
    object synchronously, without running any of its body, so the counter
    must go up in ``call_tool`` itself rather than in the coroutine it
    hands back. Bumping it inside an ``async def`` body would only go up
    once the coroutine is awaited -- exactly what must not happen for a
    caller still queued behind :class:`SharedMcpSession`'s lock, and
    exactly the bug a doubly-anticipatory coroutine would hide.
    """

    def __init__(self, gate: CallGate) -> None:
        self._gate = gate
        self.built = 0

    async def list_tools(self) -> tuple[McpToolInfo, ...]:
        return ()

    def call_tool(
        self, name: str, arguments: Mapping[str, Any]
    ) -> CoroutineType[Any, Any, McpToolCallResult]:
        self.built += 1
        return self._run()

    async def _run(self) -> McpToolCallResult:
        """Finish when the gate releases the call that is already holding the lock."""
        await self._gate.enter()
        return McpToolCallResult(ok=True, structured=None)


class _GatedConcurrentSession(_GatedSession, ConcurrentMcpSession):
    """The same double, declaring itself already safe for concurrent calls.

    Inherits :meth:`_GatedSession.call_tool` unchanged: a session declaring
    :class:`~loom.ai.abc.ConcurrentMcpSession` must **not** shield a call from
    its own caller's cancellation — there is no shared frame left to
    desynchronise, so nothing here should drain.
    """


class TestConcurrencyDeclaration:
    """``mcp_session_for`` reads the session's declaration, it does not decide for it."""

    async def test_a_session_declaring_concurrency_is_left_unwrapped(self) -> None:
        session = _GatedConcurrentSession(CallGate())
        assert mcp_session_for(session, label="crm") is session

    async def test_a_session_declaring_nothing_is_wrapped_in_shared(self) -> None:
        session = _GatedSession(CallGate())
        wrapped = mcp_session_for(session, label="crm")
        assert isinstance(wrapped, SharedMcpSession)
        assert wrapped is not session


class TestRealParallelism:
    """The success criterion is not that the lock disappears: it is that N
    concurrent calls on one grant really do run in parallel. The "declared"
    side of that criterion is not measured by a double with no lock of
    its own -- ``_GatedConcurrentSession`` runs in parallel by
    construction, and nothing in production can turn it red -- but by the
    real path, which crosses ``mcp_session_for`` and ``_ToolsetSession``
    (``tests/unit/ai/engines/test_pydantic_ai_mcp_concurrency.py``)."""

    async def test_an_undeclared_session_keeps_serialising(self) -> None:
        """A second call stays out until the first, still held, is released."""
        gate = CallGate()
        session = _GatedSession(gate)
        wrapped = mcp_session_for(session, label="crm")
        first = asyncio.create_task(wrapped.call_tool("first", {}))

        await gate.wait_all_entered(1)
        second = asyncio.create_task(wrapped.call_tool("second", {}))
        await gate.let_pending_calls_enter()

        assert gate.entered_count == 1
        gate.release()
        await gate.wait_all_entered(2)
        await first
        await second
        assert session.finished == ["first", "second"]


class TestDrainingOnCancellation:
    """A call cancelled halfway through ``SharedMcpSession`` must not
     desynchronise its neighbours: it drains before releasing the lock. A
     session declared concurrent holds no lock to protect, so its
     cancellation returns to the caller immediately instead of draining
    ."""

    async def test_shared_mcp_session_drains_the_cancelled_call(self) -> None:
        """Cancellation waits until the in-flight call finishes, then the next call runs."""
        gate = CallGate()
        session = _GatedSession(gate)
        shared = SharedMcpSession(session, label="crm")
        task = asyncio.create_task(shared.call_tool("victim", {}))
        await gate.wait_all_entered(1)

        task.cancel()
        await gate.let_pending_calls_enter()

        assert not task.done()
        assert session.finished == []
        gate.release()
        await await_cancelled(task)
        assert session.finished == ["victim"]
        await shared.call_tool("neighbour", {})
        assert session.finished == ["victim", "neighbour"]

    async def test_a_concurrent_session_does_not_drain_on_cancellation(self) -> None:
        """Cancellation returns while the remote call is still waiting."""
        gate = CallGate()
        session = _GatedConcurrentSession(gate)
        task = asyncio.create_task(session.call_tool("victim", {}))
        await gate.wait_all_entered(1)

        task.cancel()

        await await_cancelled(task)
        assert session.finished == []

    async def test_a_concurrent_neighbour_is_not_blocked_when_the_other_is_cancelled(
        self,
    ) -> None:
        """The neighbour finishes while the cancelled call is still held."""
        gate = CallGate()
        gate.hold("victim")
        session = _GatedConcurrentSession(gate)
        victim = asyncio.create_task(session.call_tool("victim", {}))
        await gate.wait_all_entered(1)
        neighbour = asyncio.create_task(session.call_tool("neighbour", {}))

        gate.release()
        await gate.wait_all_entered(2)
        await asyncio.wait_for(neighbour, timeout=5)

        assert session.finished == ["neighbour"]
        assert not victim.done()
        victim.cancel()
        await await_cancelled(victim)


class TestTheCoroutineIsBuiltWithTheLockAlreadyHeld:
    """A caller cancelled while waiting its turn must never leave a coroutine
    of its own unexecuted forever (H6): ``SharedMcpSession`` only builds the
    next turn's call once it already holds the lock."""

    async def test_a_queued_caller_does_not_build_its_coroutine_if_cancelled(self) -> None:
        """A caller still waiting for the lock has not built its coroutine."""
        gate = CallGate()
        session = _CountingSession(gate)
        shared = SharedMcpSession(session, label="crm")
        holder = asyncio.create_task(shared.call_tool("holder", {}))
        await gate.wait_all_entered(1)
        queued = asyncio.create_task(shared.call_tool("queued", {}))
        await gate.let_pending_calls_enter()

        assert session.built == 1
        queued.cancel()
        await await_cancelled(queued)
        gate.release()
        await holder
        assert session.built == 1
