"""Call gates concurrency tests use instead of the clock."""

from __future__ import annotations

import asyncio
from typing import Final, TypeVar

_T = TypeVar("_T")

_HANG_GUARD_SECONDS: Final[float] = 5.0
_YIELDS_SO_A_QUEUED_CALL_CAN_ENTER: Final[int] = 3


class CallGate:
    """Counts the calls that have started and holds them until the test releases them.

    A call that :meth:`enter`s has started. It then waits for :meth:`release`,
    or for the event :meth:`hold` returned for its name. :meth:`wait_all_entered`
    bounds its wait with a hang guard, not a timing assertion.
    """

    def __init__(self) -> None:
        self.entered_count = 0
        self._arrival = asyncio.Condition()
        self._release = asyncio.Event()
        self._holds: dict[str, asyncio.Event] = {}

    def hold(self, name: str) -> asyncio.Event:
        """Make the call named ``name`` wait on its own event instead of :meth:`release`.

        Returns:
            The event that call waits on.
        """
        event = asyncio.Event()
        self._holds[name] = event
        return event

    async def enter(self, name: str = "") -> None:
        """Record that the call named ``name`` has started, then wait until it may finish."""
        async with self._arrival:
            self.entered_count += 1
            self._arrival.notify_all()
        await self._holds.get(name, self._release).wait()

    async def wait_all_entered(self, count: int) -> None:
        """Wait until ``count`` calls have entered.

        Raises:
            TimeoutError: Fewer than ``count`` calls entered within the hang guard.
        """
        async with self._arrival:
            await asyncio.wait_for(
                self._arrival.wait_for(lambda: self.entered_count >= count),
                timeout=_HANG_GUARD_SECONDS,
            )

    def release(self) -> None:
        """Let every call waiting on the shared release finish."""
        self._release.set()

    async def let_pending_calls_enter(self) -> None:
        """Yield so a call that is free to start would have entered."""
        for _ in range(_YIELDS_SO_A_QUEUED_CALL_CAN_ENTER):
            await asyncio.sleep(0)


async def await_cancelled(task: asyncio.Task[_T]) -> None:
    """Wait until ``task`` ends because it was cancelled.

    Raises:
        AssertionError: When ``task`` finishes without ``CancelledError``.
        TimeoutError: When ``task`` is still running after the hang guard.
    """
    try:
        await asyncio.wait_for(task, timeout=_HANG_GUARD_SECONDS)
    except asyncio.CancelledError:
        return
    raise AssertionError("expected the cancelled call to re-raise")
