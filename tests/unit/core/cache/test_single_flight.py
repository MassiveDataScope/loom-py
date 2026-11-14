"""In-process coalescing of concurrent reads of the same cache key.

When a hot key misses — by expiry or by invalidation — every concurrent
request for it misses at once and all of them run the same query. The first
caller loads and the rest await that same result, so the burst costs one
repository call. A failure, or a caller that disconnects, must leave the key
loadable again straight away.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from loom.core.cache import CacheConfig
from loom.core.cache._single_flight import SingleFlight

from ._doubles import AcmeCacheWidget, CachedEnv, GatedRepository, wrap_with_cache

WAITERS = 200
ROW_COUNT = 3
TICK_BUDGET = 5_000


class GatedLoader:
    """Load that blocks on a gate and counts how many times it was entered.

    Attributes:
        value: Value returned once the gate opens.
        gate: Cleared at construction; the load waits on it.
        failure: Raised once the gate opens, when set.
        calls: Number of times the load was entered.
    """

    def __init__(self, value: str) -> None:
        self.value = value
        self.gate = asyncio.Event()
        self.failure: Exception | None = None
        self.calls = 0

    async def load(self) -> str:
        """Wait for the gate, then fail or return the configured value."""
        self.calls += 1
        await self.gate.wait()
        if self.failure is not None:
            raise self.failure
        return self.value


def _widgets(count: int) -> list[AcmeCacheWidget]:
    return [AcmeCacheWidget(id=index, name=f"w{index}") for index in range(1, count + 1)]


def _env() -> CachedEnv[AcmeCacheWidget]:
    return wrap_with_cache(
        GatedRepository(_widgets(ROW_COUNT)),
        CacheConfig(default_ttl=100, default_list_ttl=50, ttl_jitter=0.0),
    )


def _gated(env: CachedEnv[AcmeCacheWidget]) -> GatedRepository:
    repository = env.repository
    assert isinstance(repository, GatedRepository)
    return repository


async def _until(predicate: Callable[[], bool]) -> None:
    """Yield to the loop until *predicate* holds, or fail the test."""
    for _ in range(TICK_BUDGET):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition never became true; the calls are serialised")


class TestCoalescedEntityReads:
    """A burst of misses on one key costs exactly one repository call."""

    async def test_two_hundred_concurrent_misses_make_one_repository_call(self) -> None:
        env = _env()
        repository = _gated(env)

        waiters = [asyncio.create_task(env.wrapper.get_by_id(1)) for _ in range(WAITERS)]
        await _until(lambda: repository.get_by_id_calls == 1)
        repository.gate.set()
        results = await asyncio.gather(*waiters)

        assert repository.get_by_id_calls == 1
        assert results == [AcmeCacheWidget(id=1, name="w1")] * WAITERS

    async def test_two_keys_are_not_serialised_against_each_other(self) -> None:
        env = _env()
        repository = _gated(env)

        first = asyncio.create_task(env.wrapper.get_by_id(1))
        second = asyncio.create_task(env.wrapper.get_by_id(2))
        await _until(lambda: repository.get_by_id_calls == 2)
        repository.gate.set()

        assert await first == AcmeCacheWidget(id=1, name="w1")
        assert await second == AcmeCacheWidget(id=2, name="w2")

    async def test_the_cached_custom_method_is_coalesced_too(self) -> None:
        env = _env()
        repository = _gated(env)
        find_names = env.wrapper.find_names

        waiters = [asyncio.create_task(find_names("w")) for _ in range(WAITERS)]
        await _until(lambda: repository.custom_calls == 1)
        repository.gate.set()
        results = await asyncio.gather(*waiters)

        assert repository.custom_calls == 1
        assert results == [["w1", "w2", "w3"]] * WAITERS


class TestLoaderFailure:
    """A failed load reaches every waiter and does not poison the key."""

    async def test_every_waiter_receives_the_failure(self) -> None:
        env = _env()
        repository = _gated(env)
        repository.failure = RuntimeError("backend down")

        waiters = [asyncio.create_task(env.wrapper.get_by_id(1)) for _ in range(3)]
        await _until(lambda: repository.get_by_id_calls == 1)
        repository.gate.set()
        results = await asyncio.gather(*waiters, return_exceptions=True)

        assert [str(result) for result in results] == ["backend down"] * 3
        assert repository.get_by_id_calls == 1

    async def test_the_key_is_loadable_again_immediately_afterwards(self) -> None:
        env = _env()
        repository = _gated(env)
        repository.failure = RuntimeError("backend down")
        repository.gate.set()

        with pytest.raises(RuntimeError):
            await env.wrapper.get_by_id(1)
        repository.failure = None

        assert await env.wrapper.get_by_id(1) == AcmeCacheWidget(id=1, name="w1")
        assert repository.get_by_id_calls == 2


class TestCallerScopedTransaction:
    """A load bound to the caller's session must not be detached into a task.

    The coalesced load runs in its own task and deliberately outlives the
    caller that started it. Inside a transaction that task holds the caller's
    session, which its teardown closes; the read must stay inline there.
    """

    async def test_reads_are_not_coalesced_inside_a_transaction(self) -> None:
        env = _env()
        repository = _gated(env)
        repository.caller_scoped_session = True

        waiters = [asyncio.create_task(env.wrapper.get_by_id(1)) for _ in range(3)]
        await _until(lambda: repository.get_by_id_calls == 3)
        repository.gate.set()
        results = await asyncio.gather(*waiters)

        assert repository.get_by_id_calls == 3
        assert results == [AcmeCacheWidget(id=1, name="w1")] * 3

    async def test_a_cancelled_caller_leaves_no_load_running(self) -> None:
        """Nothing may resume against the session the caller is closing."""
        env = _env()
        repository = _gated(env)
        repository.caller_scoped_session = True

        caller = asyncio.create_task(env.wrapper.get_by_id(1))
        await _until(lambda: repository.get_by_id_calls == 1)
        caller.cancel()
        repository.gate.set()
        await asyncio.gather(caller, return_exceptions=True)
        await asyncio.sleep(0)

        assert caller.cancelled()
        assert repository.completed_calls == 0
        assert env.backend.set_ttls == []

    async def test_outside_a_transaction_the_load_still_outlives_its_caller(self) -> None:
        env = _env()
        repository = _gated(env)

        caller = asyncio.create_task(env.wrapper.get_by_id(1))
        await _until(lambda: repository.get_by_id_calls == 1)
        caller.cancel()
        repository.gate.set()
        await asyncio.gather(caller, return_exceptions=True)
        await _until(lambda: repository.completed_calls == 1)

        assert caller.cancelled()
        assert repository.completed_calls == 1


class TestFirstCallerCancellation:
    """A client that disconnects must not take the other waiters down with it."""

    async def test_the_others_still_get_their_value(self) -> None:
        env = _env()
        repository = _gated(env)

        waiters = [asyncio.create_task(env.wrapper.get_by_id(1)) for _ in range(3)]
        await _until(lambda: repository.get_by_id_calls == 1)
        waiters[0].cancel()
        await asyncio.sleep(0)
        repository.gate.set()
        survivors = await asyncio.gather(*waiters[1:])

        assert waiters[0].cancelled()
        assert survivors == [AcmeCacheWidget(id=1, name="w1")] * 2
        assert repository.get_by_id_calls == 1

    async def test_an_abandoned_burst_still_populates_the_cache(self) -> None:
        """The load is paid for either way, so its result must not be dropped."""
        env = _env()
        repository = _gated(env)

        waiters = [asyncio.create_task(env.wrapper.get_by_id(1)) for _ in range(3)]
        await _until(lambda: repository.get_by_id_calls == 1)
        for waiter in waiters:
            waiter.cancel()
        await asyncio.gather(*waiters, return_exceptions=True)
        repository.gate.set()
        await _until(lambda: len(env.backend.set_ttls) == 1)

        assert await env.wrapper.get_by_id(1) == AcmeCacheWidget(id=1, name="w1")
        assert repository.get_by_id_calls == 1


class TestInFlightBookkeeping:
    """Nothing is left behind, whichever way the load ends."""

    async def test_the_map_is_empty_after_a_successful_load(self) -> None:
        single_flight = SingleFlight()
        loader = GatedLoader("value")
        loader.gate.set()

        assert await single_flight.run("key", loader.load) == "value"
        assert single_flight.in_flight_keys == frozenset()

    async def test_the_map_is_empty_after_a_failed_load(self) -> None:
        single_flight = SingleFlight()
        loader = GatedLoader("value")
        loader.failure = RuntimeError("boom")
        loader.gate.set()

        with pytest.raises(RuntimeError):
            await single_flight.run("key", loader.load)

        assert single_flight.in_flight_keys == frozenset()

    async def test_the_map_is_empty_after_the_only_caller_is_cancelled(self) -> None:
        single_flight = SingleFlight()
        loader = GatedLoader("value")

        caller = asyncio.create_task(single_flight.run("key", loader.load))
        await _until(lambda: loader.calls == 1)
        caller.cancel()
        loader.gate.set()
        await asyncio.gather(caller, return_exceptions=True)
        await _until(lambda: not single_flight.in_flight_keys)

        assert caller.cancelled()
        assert single_flight.in_flight_keys == frozenset()

    async def test_the_key_is_held_only_while_the_load_runs(self) -> None:
        single_flight = SingleFlight()
        loader = GatedLoader("value")

        caller = asyncio.create_task(single_flight.run("key", loader.load))
        await _until(lambda: loader.calls == 1)

        assert single_flight.in_flight_keys == frozenset({"key"})

        loader.gate.set()
        await caller

        assert single_flight.in_flight_keys == frozenset()

    async def test_two_keys_load_concurrently(self) -> None:
        single_flight = SingleFlight()
        first = GatedLoader("first")
        second = GatedLoader("second")

        callers = [
            asyncio.create_task(single_flight.run("a", first.load)),
            asyncio.create_task(single_flight.run("b", second.load)),
        ]
        await _until(lambda: first.calls == 1 and second.calls == 1)
        first.gate.set()
        second.gate.set()

        assert await asyncio.gather(*callers) == ["first", "second"]
        assert single_flight.in_flight_keys == frozenset()

    async def test_a_failure_reaches_every_waiter_of_the_same_key(self) -> None:
        single_flight = SingleFlight()
        loader = GatedLoader("value")
        loader.failure = RuntimeError("boom")

        waiters = [asyncio.create_task(single_flight.run("key", loader.load)) for _ in range(3)]
        await _until(lambda: loader.calls == 1)
        loader.gate.set()
        results = await asyncio.gather(*waiters, return_exceptions=True)

        assert [str(result) for result in results] == ["boom"] * 3
        assert loader.calls == 1
        assert single_flight.in_flight_keys == frozenset()

    async def test_a_stale_release_does_not_evict_a_newer_load(self) -> None:
        """The load that finished must not release the key a newer load holds."""
        single_flight = SingleFlight()
        first = GatedLoader("first")
        second = GatedLoader("second")

        # The interleaving is deterministic on CPython's ``_run_once``: the
        # first load completes and queues its release callback, then the newer
        # caller runs and registers its flight, then the stale callback fires.
        # Under a loop that batches differently this test would stop producing
        # the race rather than fail, so treat it as a guard, not a proof.
        caller = asyncio.create_task(single_flight.run("key", first.load))
        await _until(lambda: first.calls == 1)
        first.gate.set()
        newer = asyncio.create_task(single_flight.run("key", second.load))
        await _until(lambda: second.calls == 1)

        assert single_flight.in_flight_keys == frozenset({"key"})

        second.gate.set()

        assert await caller == "first"
        assert await newer == "second"
        assert single_flight.in_flight_keys == frozenset()


class TestAbandonedFailures:
    """A failure nobody is left to receive is reported instead of swallowed."""

    async def test_a_failure_with_no_waiter_left_is_reported(self) -> None:
        reported: list[tuple[str, str]] = []
        single_flight = SingleFlight(lambda key, error: reported.append((key, str(error))))
        loader = GatedLoader("value")
        loader.failure = RuntimeError("backend down")

        caller = asyncio.create_task(single_flight.run("key", loader.load))
        await _until(lambda: loader.calls == 1)
        caller.cancel()
        loader.gate.set()
        await asyncio.gather(caller, return_exceptions=True)
        await _until(lambda: not single_flight.in_flight_keys)

        assert reported == [("key", "backend down")]

    async def test_a_failure_a_waiter_receives_is_not_reported(self) -> None:
        reported: list[tuple[str, str]] = []
        single_flight = SingleFlight(lambda key, error: reported.append((key, str(error))))
        loader = GatedLoader("value")
        loader.failure = RuntimeError("backend down")
        loader.gate.set()

        with pytest.raises(RuntimeError):
            await single_flight.run("key", loader.load)

        assert reported == []
