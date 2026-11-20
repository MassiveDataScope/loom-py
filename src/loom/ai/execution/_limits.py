"""Per-run limits enforced over an engine's event stream.

Keeps the whole limit vocabulary out of the lifecycle: a breach of
``run_timeout_ms``, ``tool_timeout_ms`` or ``max_iterations`` becomes the
stream's terminal :class:`~loom.ai.abc.ErrorEvent`, which the runtime turns
back into an :class:`~loom.ai.errors.AgentRunError`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator

from loom.ai.abc import (
    AgentEvent,
    ErrorEvent,
    FinalEvent,
    ToolCallEvent,
    ToolResultEvent,
)
from loom.ai.declarative import PolicySpec
from loom.ai.errors import AgentRunErrorCode

_TERMINAL_EVENT_TYPES: frozenset[type] = frozenset({ErrorEvent, FinalEvent})


class _RunSupervisor:
    """Enforces the per-run limits of one plan over its event stream.

    Keeps the whole limit vocabulary in one place so ``/run`` and ``/stream``
    share a single code path: a breach becomes the stream's terminal
    :class:`~loom.ai.abc.ErrorEvent`, which ``run()`` turns back into an
    :class:`AgentRunError`.

    Args:
        policies: Validated execution limits of the plan being run.
    """

    def __init__(self, policies: PolicySpec) -> None:
        loop = asyncio.get_running_loop()
        self._now = loop.time
        self._run_deadline = loop.time() + policies.run_timeout_ms / 1000
        self._policies = policies
        self._tool_deadline: float | None = None
        self._pending_tool: str | None = None
        self._pending_call: str | None = None
        self._iterations = 0
        self.terminated = False

    @property
    def deadline(self) -> float:
        """Absolute loop time the next event must arrive before."""
        if self._tool_deadline is None:
            return self._run_deadline
        return min(self._run_deadline, self._tool_deadline)

    def timeout_event(self) -> ErrorEvent:
        """Return the terminal event of an expired deadline, naming what expired."""
        tool_expired = (
            self._pending_tool is not None
            and self._tool_deadline is not None
            and self._tool_deadline <= self._run_deadline
        )
        if tool_expired:
            return ErrorEvent(
                code=AgentRunErrorCode.TOOL_TIMEOUT,
                message=(
                    f"tool {self._pending_tool!r} exceeded tool_timeout_ms "
                    f"({self._policies.tool_timeout_ms})"
                ),
            )
        return ErrorEvent(
            code=AgentRunErrorCode.RUN_TIMEOUT,
            message=f"run exceeded run_timeout_ms ({self._policies.run_timeout_ms})",
        )

    def observe(self, event: AgentEvent) -> ErrorEvent | None:
        """Account for one event and return the terminal event of a breach.

        Args:
            event: Event just produced by the engine.

        Returns:
            The terminal :class:`~loom.ai.abc.ErrorEvent` when a limit is
            breached, ``None`` when the event may be forwarded.
        """
        if type(event) is ToolCallEvent:
            return self._observe_tool_call(event)
        if type(event) is ToolResultEvent:
            self._observe_tool_result(event)
            return None
        self.terminated = type(event) in _TERMINAL_EVENT_TYPES
        return None

    def _observe_tool_call(self, event: ToolCallEvent) -> ErrorEvent | None:
        self._iterations += 1
        if self._iterations > self._policies.max_iterations:
            return ErrorEvent(
                code=AgentRunErrorCode.MAX_ITERATIONS_EXCEEDED,
                message=f"run exceeded max_iterations ({self._policies.max_iterations})",
            )
        self._pending_tool = event.tool
        self._pending_call = event.call_id
        self._tool_deadline = self._now() + self._policies.tool_timeout_ms / 1000
        return None

    def _observe_tool_result(self, event: ToolResultEvent) -> None:
        if event.call_id != self._pending_call:
            return
        self._pending_tool = None
        self._pending_call = None
        self._tool_deadline = None


async def supervised_events(
    events: AsyncIterator[AgentEvent], policies: PolicySpec
) -> AsyncGenerator[AgentEvent, None]:
    """Forward an engine stream while enforcing the plan's per-run limits."""
    supervisor = _RunSupervisor(policies)
    iterator = events.__aiter__()
    while True:
        try:
            async with asyncio.timeout_at(supervisor.deadline):
                event = await anext(iterator)
        except StopAsyncIteration:
            return
        except TimeoutError:
            yield supervisor.timeout_event()
            return
        breach = supervisor.observe(event)
        if breach is not None:
            yield breach
            return
        yield event
        if supervisor.terminated:
            return


async def cancel_task(task: asyncio.Task[None]) -> None:
    """Cancel an owned background task and wait for it to actually stop.

    The ``CancelledError`` is swallowed only when it belongs to *task*. If it
    arrived because the caller itself was cancelled while awaiting, it is
    re-raised: absorbing that one would break the cooperative cancellation of
    whoever is shutting this runtime down, and the shutdown would appear to
    succeed while its caller kept running.

    Args:
        task: Background task this runtime owns.
    """
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        if not task.cancelled():
            raise
