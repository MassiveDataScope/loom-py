"""N-c: each singleton is built once across threads, and no lock is held across its provider."""

from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, wait

import pytest

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope

THREADS = 16
TIMEOUT_S = 5.0


class Counted:
    """Counts its constructions; yields the GIL while building to widen the race."""

    built = 0
    lock = threading.Lock()

    def __init__(self) -> None:
        with Counted.lock:
            type(self).built += 1
        threading.Event().wait(0.001)


class AcmeBottom(Counted):
    built = 0


class AcmeMiddle(Counted):
    built = 0

    def __init__(self, bottom: AcmeBottom) -> None:
        super().__init__()
        self.bottom = bottom


class AcmeTop(Counted):
    built = 0

    def __init__(self, middle: AcmeMiddle) -> None:
        super().__init__()
        self.middle = middle


@pytest.fixture(autouse=True)
def _reset() -> None:
    for counted in (AcmeBottom, AcmeMiddle, AcmeTop):
        counted.built = 0


def _race(resolve: Callable[[], object]) -> list[object]:
    """*resolve* from every pool thread at once; every call returns within the bound."""
    barrier = threading.Barrier(THREADS)

    def run() -> object:
        barrier.wait(TIMEOUT_S)
        return resolve()

    with ThreadPoolExecutor(THREADS) as pool:
        futures = [pool.submit(run) for _ in range(THREADS)]
        done, pending = wait(futures, timeout=TIMEOUT_S)
    assert not pending, "a thread did not return: deadlock"
    return [future.result() for future in done]


class TestSingletonThreadSafety:
    def test_an_unbuilt_singleton_is_built_once(self) -> None:
        container = LoomContainer()
        container.register(AcmeBottom, AcmeBottom, scope=Scope.APPLICATION)

        results = _race(lambda: container.resolve(AcmeBottom))

        assert AcmeBottom.built == 1
        assert len({id(result) for result in results}) == 1

    def test_a_provider_resolving_a_second_unbuilt_singleton_builds_each_once(self) -> None:
        container = LoomContainer()
        container.register(AcmeBottom, AcmeBottom, scope=Scope.APPLICATION)
        container.register(
            AcmeMiddle, lambda: AcmeMiddle(container.resolve(AcmeBottom)), scope=Scope.APPLICATION
        )

        results = _race(lambda: container.resolve(AcmeMiddle))

        assert (AcmeMiddle.built, AcmeBottom.built) == (1, 1)
        assert len({id(result) for result in results}) == 1

    def test_a_chain_three_deep_resolved_from_its_top_builds_each_once(self) -> None:
        container = LoomContainer()
        for key in (AcmeBottom, AcmeMiddle, AcmeTop):
            container.register(key, key, scope=Scope.APPLICATION)

        results = _race(lambda: container.resolve(AcmeTop))

        assert (AcmeTop.built, AcmeMiddle.built, AcmeBottom.built) == (1, 1, 1)
        assert len({id(result) for result in results}) == 1

    def test_a_provider_joining_a_thread_that_resolves_another_singleton_completes(self) -> None:
        container = LoomContainer()
        container.register(AcmeBottom, AcmeBottom, scope=Scope.APPLICATION)

        def middle() -> AcmeMiddle:
            found: list[AcmeBottom] = []
            # A daemon thread: the test stays bounded if the worker is stuck on a lock.
            worker = threading.Thread(
                target=lambda: found.append(container.resolve(AcmeBottom)), daemon=True
            )
            worker.start()
            worker.join(TIMEOUT_S)
            assert found, "the worker did not return: a lock is held across the provider"
            return AcmeMiddle(found[0])

        container.register(AcmeMiddle, middle, scope=Scope.APPLICATION)

        built = container.resolve(AcmeMiddle)

        assert built.bottom is container.resolve(AcmeBottom)
        assert (AcmeMiddle.built, AcmeBottom.built) == (1, 1)
