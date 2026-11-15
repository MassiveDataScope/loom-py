"""In-process coalescing of concurrent loads that share a cache key.

Internal to the cache layer. When a hot key misses, every concurrent request
for it misses at once and each one runs the same query; coalescing them turns
that burst into a single load whose result is handed to every caller.

The coordination is per process and per instance. A second process racing on
the same key costs one duplicate load, never a wrong answer, so no distributed
lock is involved.

A flight is detached from its callers, and nothing drains it at shutdown: a
loop closed while a load is still running logs "Task was destroyed but it is
pending". The load is a cache refill, so losing it costs nothing beyond that
line.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from functools import partial
from typing import TypeVar, cast

T = TypeVar("T")

AbandonedFailureReporter = Callable[[str, BaseException], None]


async def _await_loader(loader: Callable[[], Awaitable[T]]) -> T:
    """Adapt an awaitable-returning loader to the coroutine ``create_task`` wants."""
    return await loader()


@dataclass(slots=True)
class _Flight:
    """One running load, its key and how many callers are still waiting on it."""

    key: str
    task: asyncio.Task[object]
    waiters: int = field(default=0)


class SingleFlight:
    """Runs one load per key at a time and shares its outcome with every caller.

    The load runs in its own task, so it is owned by the group rather than by
    the caller that happened to arrive first: a client that disconnects cancels
    only its own await, and the callers still waiting get their value. That
    detachment is only safe for a load that owns everything it needs; the
    caller decides, and :class:`~loom.core.cache.repository.CachedRepository`
    keeps a load bound to a caller-scoped transaction out of here.

    The entry is released when the load finishes, fails or is cancelled, which
    leaves the key loadable again immediately.

    Args:
        on_abandoned_failure: Called with the key and the error when a load
            fails and no caller is left to receive it, which would otherwise be
            silent. Optional; nothing is reported without it.

    Example:
        >>> single_flight = SingleFlight()
        >>> value = await single_flight.run(key, lambda: load_from_database())
    """

    def __init__(self, on_abandoned_failure: AbandonedFailureReporter | None = None) -> None:
        self._in_flight: dict[str, _Flight] = {}
        self._on_abandoned_failure = on_abandoned_failure

    @property
    def in_flight_keys(self) -> frozenset[str]:
        """Keys whose load is running right now."""
        return frozenset(self._in_flight)

    async def run(self, key: str, loader: Callable[[], Awaitable[T]]) -> T:
        """Load *key* once, sharing the outcome with every concurrent caller.

        Args:
            key: Identity of the load; different keys never wait on each other.
            loader: Zero-argument coroutine function producing the value.

        Returns:
            The value produced by the loader, whether this call ran it or
            joined one already in progress.

        Raises:
            Exception: Whatever the loader raises, re-raised in every caller
                waiting on the same key.
        """
        flight = self._in_flight.get(key)
        if flight is None or flight.task.done():
            flight = self._start(key, loader)
        flight.waiters += 1
        try:
            return cast(T, await asyncio.shield(flight.task))
        finally:
            flight.waiters -= 1

    def _start(self, key: str, loader: Callable[[], Awaitable[T]]) -> _Flight:
        task = cast("asyncio.Task[object]", asyncio.create_task(_await_loader(loader)))
        flight = _Flight(key=key, task=task)
        self._in_flight[key] = flight
        task.add_done_callback(partial(self._release, flight))
        return flight

    def _release(self, flight: _Flight, task: asyncio.Task[object]) -> None:
        if self._in_flight.get(flight.key) is flight:
            del self._in_flight[flight.key]
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and flight.waiters == 0 and self._on_abandoned_failure is not None:
            self._on_abandoned_failure(flight.key, error)
