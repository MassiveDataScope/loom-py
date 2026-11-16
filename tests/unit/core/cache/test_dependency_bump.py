"""Unit tests for ``GenerationalDependencyResolver.bump_from_events``.

The counters are bumped concurrently rather than one round trip at a time
(see ``dependency.py`` for the rationale), so these tests pin down the set of
keys touched rather than the calling order, which concurrent execution does
not guarantee.
"""

from __future__ import annotations

import asyncio

from loom.core.cache import GenerationalDependencyResolver
from loom.core.stores.repository.mutation import MutationEvent

from ._doubles import CountingCacheBackend


class TestDependencyBump:
    async def test_bump_from_events_increments_every_expected_key_once(self) -> None:
        backend = CountingCacheBackend()
        resolver = GenerationalDependencyResolver(backend)
        events = (
            MutationEvent(entity="widget", op="update", ids=(1, 2), tags=frozenset({"custom-tag"})),
            MutationEvent(entity="widget", op="update", ids=(2,), tags=frozenset()),
        )

        await resolver.bump_from_events(events)

        expected = {
            "tag:widget:list",
            "tag:widget:id:1",
            "tag:widget:id:2",
            "tag:custom-tag",
        }
        assert set(backend.incr_keys) == expected
        # Each key is bumped exactly once even though id 2 appears in both events.
        assert len(backend.incr_keys) == len(expected)
        for key in expected:
            assert await backend.get_value(key, type=int) == 1

    async def test_bump_from_events_runs_increments_concurrently(self) -> None:
        """The bump must not serialize its round trips behind one another.

        A slow backend that awaits an event before returning proves concurrency:
        a sequential loop would take one wait per key, a concurrent bump takes a
        single wait shared by every key in flight.
        """

        class SlowBackend(CountingCacheBackend):
            def __init__(self, release: asyncio.Event) -> None:
                super().__init__()
                self._release = release
                self.in_flight = 0
                self.max_in_flight = 0

            async def incr(self, key: str, delta: int = 1) -> int:
                self.in_flight += 1
                self.max_in_flight = max(self.max_in_flight, self.in_flight)
                await self._release.wait()
                self.in_flight -= 1
                return await super().incr(key, delta)

        release = asyncio.Event()
        backend = SlowBackend(release)
        resolver = GenerationalDependencyResolver(backend)
        events = (MutationEvent(entity="widget", op="update", ids=(1, 2, 3), tags=frozenset()),)

        task = asyncio.create_task(resolver.bump_from_events(events))
        # Let every incr coroutine reach its await point before releasing them.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        release.set()
        await task

        # 4 keys: entity:list, id:1, id:2, id:3. The bare entity tag is never bumped.
        assert backend.max_in_flight == 4
