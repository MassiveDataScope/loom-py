"""A loop host keeps an application open on a loop of its own across synchronous calls."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import pytest

from loom.composition import LoopHost
from loom.composition.resources import Resource, ResourceKind
from tests.helpers.composition import application_of
from tests.helpers.loops import RecordedLoops


class _Store:
    """A store recording its lifecycle, failing to open or close on demand."""

    def __init__(self, *, fail_open: bool = False, fail_close: bool = False) -> None:
        self.events: list[str] = []
        self._fail_open = fail_open
        self._fail_close = fail_close

    def resource(self) -> Resource:
        return Resource(kind=ResourceKind.STORE, name="default", manager=self._opened)

    @asynccontextmanager
    async def _opened(self) -> AsyncIterator[None]:
        if self._fail_open:
            raise ConnectionError("store unreachable")
        self.events.append("open")
        yield
        self.events.append("close")
        if self._fail_close:
            raise RuntimeError("store failed to close")


def _host(store: _Store) -> LoopHost:
    return LoopHost(application_of(store.resource()))


def test_building_the_host_makes_its_loop_current(event_loops: RecordedLoops) -> None:
    host = _host(_Store())

    assert event_loops.made_current == event_loops.created
    host.close()


def test_start_opens_the_application_once() -> None:
    store = _Store()
    host = _host(store)

    host.start()

    assert store.events == ["open"]
    host.close()


def test_run_returns_the_coroutines_result_on_the_hosts_loop(
    event_loops: RecordedLoops,
) -> None:
    host = _host(_Store())
    host.start()

    async def running_loop() -> asyncio.AbstractEventLoop:
        return asyncio.get_running_loop()

    assert host.run(running_loop()) is event_loops.created[0]
    host.close()


def test_run_makes_the_hosts_loop_current_again(event_loops: RecordedLoops) -> None:
    host = _host(_Store())
    host.start()
    stray = asyncio.new_event_loop()
    asyncio.set_event_loop(stray)

    host.run(asyncio.sleep(0))

    assert event_loops.made_current[-1] is event_loops.created[0]
    host.close()
    stray.close()


def test_close_closes_the_application() -> None:
    store = _Store()
    host = _host(store)
    host.start()

    host.close()

    assert store.events == ["open", "close"]


@pytest.mark.parametrize("closes", [1, 2], ids=["once", "twice"])
def test_close_closes_the_loop(event_loops: RecordedLoops, closes: int) -> None:
    host = _host(_Store())
    host.start()

    for _ in range(closes):
        host.close()

    assert event_loops.created[0].is_closed()


def test_close_unsets_the_loop(event_loops: RecordedLoops) -> None:
    host = _host(_Store())
    host.start()

    host.close()

    assert event_loops.made_current[-1] is None


def test_a_failure_to_close_still_closes_the_loop(event_loops: RecordedLoops) -> None:
    host = _host(_Store(fail_close=True))
    host.start()

    with pytest.raises(RuntimeError, match="store failed to close"):
        host.close()
    assert event_loops.created[0].is_closed()


def test_a_failure_to_open_propagates() -> None:
    host = _host(_Store(fail_open=True))

    with pytest.raises(ConnectionError, match="store unreachable"):
        host.start()


def test_a_failure_to_open_closes_the_loop(event_loops: RecordedLoops) -> None:
    host = _host(_Store(fail_open=True))

    with pytest.raises(ConnectionError):
        host.start()
    assert event_loops.created[0].is_closed()


def test_a_failure_to_open_unsets_the_loop(event_loops: RecordedLoops) -> None:
    host = _host(_Store(fail_open=True))

    with pytest.raises(ConnectionError):
        host.start()
    assert event_loops.made_current[-1] is None


def test_closing_after_a_failure_to_open_does_nothing(event_loops: RecordedLoops) -> None:
    host = _host(_Store(fail_open=True))
    with pytest.raises(ConnectionError):
        host.start()

    host.close()

    assert event_loops.made_current.count(None) == 1
