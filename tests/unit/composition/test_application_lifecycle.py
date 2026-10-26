"""An application opens its resources once, in the order of their kind, closing in reverse."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from types import SimpleNamespace
from typing import cast

import pytest

from loom.composition import Application, Resource, ResourceKind, closing
from loom.core.bootstrap.kernel import KernelRuntime


class Journal:
    """Records every open, close and check, and fails or cancels the steps it is told to."""

    def __init__(self, *, fail: tuple[str, ...] = (), cancel: tuple[str, ...] = ()) -> None:
        self.events: list[str] = []
        self.fail = set(fail)
        self.cancel = set(cancel)

    def step(self, event: str) -> None:
        self.events.append(event)
        if event in self.fail:
            raise RuntimeError(event)
        if event in self.cancel:
            raise asyncio.CancelledError

    def resource(self, name: str) -> AbstractAsyncContextManager[None]:
        @asynccontextmanager
        async def resource() -> AsyncIterator[None]:
            self.step(f"open:{name}")
            try:
                yield
            finally:
                self.step(f"close:{name}")

        return resource()

    def closer(self, name: str) -> Resource:
        async def close() -> None:
            self.step(f"close:{name}")

        return Resource(kind=ResourceKind.CACHE, name=name, manager=closing(close))

    async def check(self) -> None:
        self.step("check:persistence")


def _kernel() -> KernelRuntime:
    return cast(KernelRuntime, SimpleNamespace())


def _store(journal: Journal, *, startup_checks: bool = True) -> Resource:
    return Resource(
        kind=ResourceKind.STORE,
        name="default",
        manager=lambda: journal.resource("persistence"),
        check=journal.check if startup_checks else None,
    )


def _application(
    journal: Journal,
    *,
    cache: bool = True,
    resources: Sequence[Resource] | None = None,
    startup_checks: bool = True,
) -> Application:
    """Cache gateways, SQL and agents (or *resources*), and one store."""
    if resources is None:
        # Fresh managers on every open, as a registry or runtime context manager is.
        resources = (
            Resource(kind=ResourceKind.SQL, name="sql", manager=lambda: journal.resource("sql")),
            Resource(kind=ResourceKind.AGENTS, name="ai", manager=lambda: journal.resource("ai")),
        )
    gateways = (journal.closer("cache"), journal.closer("counters")) if cache else ()
    return Application(
        kernel=_kernel(),
        resources=(*gateways, *resources, _store(journal, startup_checks=startup_checks)),
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

    assert journal.events == ["open:sql", "open:ai", "open:persistence", *CLOSE]


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


async def test_a_cancellation_while_closing_propagates_after_closing_everything() -> None:
    journal = Journal(cancel=("close:counters",))
    application = _application(journal)

    with pytest.raises(asyncio.CancelledError):
        async with application.running():
            pass

    assert journal.events == OPEN + CLOSE
    assert not application.is_open


class Witness:
    """A resource that records the error it was closed with."""

    def __init__(self) -> None:
        self.closed_with: BaseException | None | str = "never closed"

    async def __aenter__(self) -> Witness:
        return self

    async def __aexit__(self, _type: object, error: BaseException | None, _tb: object) -> None:
        self.closed_with = error


async def test_each_resource_is_told_the_error_that_ended_the_block() -> None:
    witness = Witness()
    journal = Journal()
    application = _application(
        journal,
        cache=False,
        startup_checks=False,
        resources=(
            Resource(kind=ResourceKind.SQL, name="witness", manager=lambda: witness),
            Resource(kind=ResourceKind.SQL, name="sql", manager=lambda: journal.resource("sql")),
        ),
    )
    boom = ValueError("boom")

    with pytest.raises(ValueError, match="boom") as raised:
        async with application.running():
            raise boom

    assert raised.value is boom
    assert witness.closed_with is boom
    assert journal.events[-2:] == ["close:persistence", "close:sql"]


async def test_an_application_without_resources_opens_nothing() -> None:
    application = Application(kernel=_kernel())

    async with application.running():
        assert application.is_open

    assert not application.is_open


async def test_the_kind_decides_the_order_whatever_order_the_resources_come_in() -> None:
    journal = Journal()
    given = _application(journal).resources

    application = Application(kernel=_kernel(), resources=tuple(reversed(given)))
    async with application.running():
        pass

    assert [resource.kind for resource in application.resources] == [
        ResourceKind.CACHE,
        ResourceKind.CACHE,
        ResourceKind.SQL,
        ResourceKind.AGENTS,
        ResourceKind.STORE,
    ]
    assert journal.events[: len(OPEN)] == OPEN


async def test_a_failure_to_close_names_the_resource() -> None:
    journal = Journal(fail=("close:ai",))
    application = _application(journal)

    with pytest.raises(RuntimeError, match="close:ai") as raised:
        async with application.running():
            pass

    assert raised.value.__notes__ == ["while closing agents:ai"]


async def test_a_failure_to_close_after_an_error_in_the_block_chains_that_error() -> None:
    journal = Journal(fail=("close:ai",))
    application = _application(journal)
    boom = ValueError("boom")

    with pytest.raises(RuntimeError, match="close:ai") as raised:
        async with application.running():
            raise boom

    assert raised.value.__cause__ is boom
    assert journal.events == OPEN + CLOSE
    assert not application.is_open
