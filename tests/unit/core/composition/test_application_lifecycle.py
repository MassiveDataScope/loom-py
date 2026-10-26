"""An application opens its resources once, in a fixed order, and closes them in reverse."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from types import SimpleNamespace
from typing import Any, cast

import pytest

from loom.core.bootstrap.kernel import KernelRuntime
from loom.core.cache.wiring import CacheGateways
from loom.core.composition import Application
from loom.core.di.container import LoomContainer
from loom.core.persistence.abc import PersistenceWiring


class Journal:
    """Records every open, close and check, and fails the steps it is told to."""

    def __init__(self, *, fail: tuple[str, ...] = ()) -> None:
        self.events: list[str] = []
        self.fail = set(fail)

    def step(self, event: str) -> None:
        self.events.append(event)
        if event in self.fail:
            raise RuntimeError(event)

    def resource(self, name: str) -> AbstractAsyncContextManager[object]:
        @asynccontextmanager
        async def resource() -> AsyncIterator[object]:
            self.step(f"open:{name}")
            try:
                yield self
            finally:
                self.step(f"close:{name}")

        return resource()

    async def check(self) -> None:
        self.step("check:persistence")

    def lifespan(self) -> AbstractAsyncContextManager[None]:
        @asynccontextmanager
        async def lifespan() -> AsyncIterator[None]:
            self.step("open:persistence")
            try:
                yield
            finally:
                self.step("close:persistence")

        return lifespan()


class Gateway:
    def __init__(self, journal: Journal, name: str) -> None:
        self.journal = journal
        self.name = name

    async def close(self) -> None:
        self.journal.step(f"close:{self.name}")


def _application(journal: Journal, *, cache: bool = True) -> Application:
    container = LoomContainer()
    if cache:
        container.register_instance(
            CacheGateways,
            CacheGateways(
                data=cast(Any, Gateway(journal, "cache")),
                counters=cast(Any, Gateway(journal, "counters")),
            ),
        )
    persistence = PersistenceWiring(
        uow_factory=None,
        repo_registration_module=lambda _container: None,
        lifespan_init=journal.lifespan,
        startup_checks=journal.check,
    )
    # Fresh resources on every open, as a registry or runtime context manager is.
    return Application(
        kernel=cast(KernelRuntime, SimpleNamespace(container=container)),
        persistence=persistence,
        resources=lambda: (journal.resource("sql"), journal.resource("ai")),
    )


OPEN = ["open:sql", "open:ai", "check:persistence", "open:persistence"]
CLOSE = ["close:persistence", "close:ai", "close:sql", "close:counters", "close:cache"]


async def test_opens_in_order_and_closes_in_reverse() -> None:
    journal = Journal()
    application = _application(journal)

    async with application.running():
        assert journal.events == OPEN
        assert application.is_open

    assert journal.events == OPEN + CLOSE
    assert not application.is_open


async def test_fifty_cycles_open_and_close_each_resource_once_per_cycle() -> None:
    journal = Journal()
    application = _application(journal)

    for _ in range(50):
        async with application.running():
            pass

    assert journal.events == (OPEN + CLOSE) * 50


async def test_opening_an_open_application_fails_without_touching_it() -> None:
    journal = Journal()
    application = _application(journal)

    async with application.running():
        with pytest.raises(RuntimeError, match="already open"):
            async with application.running():
                pass  # pragma: no cover - never entered
        assert journal.events == OPEN
        assert application.is_open

    assert journal.events == OPEN + CLOSE


@pytest.mark.parametrize(
    ("failing", "closed"),
    [
        ("open:sql", ["close:counters", "close:cache"]),
        ("open:ai", ["close:sql", "close:counters", "close:cache"]),
        ("check:persistence", ["close:ai", "close:sql", "close:counters", "close:cache"]),
        ("open:persistence", ["close:ai", "close:sql", "close:counters", "close:cache"]),
    ],
)
async def test_a_failure_to_open_closes_what_opened_and_raises_the_original(
    failing: str, closed: list[str]
) -> None:
    journal = Journal(fail=(failing,))
    application = _application(journal)

    with pytest.raises(RuntimeError, match=failing):
        async with application.running():
            pass  # pragma: no cover - never entered

    assert journal.events == OPEN[: OPEN.index(failing) + 1] + closed
    assert not application.is_open


async def test_a_failure_to_close_after_a_failure_to_open_is_a_note_on_the_original() -> None:
    journal = Journal(fail=("open:persistence", "close:sql"))
    application = _application(journal)

    with pytest.raises(RuntimeError, match="open:persistence") as raised:
        async with application.running():
            pass  # pragma: no cover - never entered

    assert any("close:sql" in note for note in raised.value.__notes__)
    assert journal.events[-3:] == ["close:sql", "close:counters", "close:cache"]


async def test_one_failure_to_close_raises_that_exception_after_closing_the_rest() -> None:
    journal = Journal(fail=("close:ai",))
    application = _application(journal)

    with pytest.raises(RuntimeError, match="close:ai"):
        async with application.running():
            pass

    assert journal.events == OPEN + CLOSE
    assert not application.is_open


async def test_several_failures_to_close_raise_all_of_them() -> None:
    journal = Journal(fail=("close:persistence", "close:cache"))
    application = _application(journal)

    with pytest.raises(ExceptionGroup) as raised:
        async with application.running():
            pass

    assert [str(error) for error in raised.value.exceptions] == [
        "close:persistence",
        "close:cache",
    ]
    assert journal.events == OPEN + CLOSE


async def test_an_application_reopens_after_closing() -> None:
    journal = Journal(fail=("open:ai",))
    application = _application(journal)
    with pytest.raises(RuntimeError):
        async with application.running():
            pass  # pragma: no cover - never entered
    journal.fail.clear()
    journal.events.clear()

    async with application.running():
        pass

    assert journal.events == OPEN + CLOSE


async def test_running_without_check_does_not_run_the_startup_checks() -> None:
    journal = Journal()
    application = _application(journal)

    async with application.running(check=False):
        pass

    assert "check:persistence" not in journal.events
    assert journal.events[: len(OPEN) - 1] == ["open:sql", "open:ai", "open:persistence"]


async def test_check_runs_the_startup_checks_and_opens_nothing() -> None:
    journal = Journal()
    application = _application(journal)

    await application.check()

    assert journal.events == ["check:persistence"]
    assert not application.is_open


async def test_without_cache_gateways_nothing_closes_them() -> None:
    journal = Journal()
    application = _application(journal, cache=False)

    async with application.running():
        pass

    assert journal.events == OPEN + CLOSE[:-2]
