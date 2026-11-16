"""A step finishes whatever happens to the task awaiting it."""

from __future__ import annotations

import asyncio
import contextvars
import gc
import weakref
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Any, cast
from unittest.mock import MagicMock

import pytest

from loom.core._async import run_in_executor, run_to_completion
from loom.core.stores.catalog import DEFAULT_STORE
from loom.stores.mongo.uow import MongoUnitOfWork
from loom.stores.sqlalchemy.uow import SQLAlchemyUnitOfWork
from tests.unit.core.conftest import make_session
from tests.unit.core.uow.conftest import make_session_manager

_LONG = 60.0
"""A timeout no step here reaches."""

_REQUEST: contextvars.ContextVar[str] = contextvars.ContextVar("request", default="")


class TestRunInExecutor:
    async def test_returns_what_the_call_returned(self) -> None:
        with ThreadPoolExecutor(max_workers=1) as executor:
            assert await run_in_executor(executor, str.upper, "ab") == "AB"

    async def test_the_caller_context_is_visible_inside_the_thread(self) -> None:
        seen: list[str] = []

        def call() -> None:
            seen.append(_REQUEST.get())

        _REQUEST.set("req-42")
        with ThreadPoolExecutor(max_workers=1) as executor:
            await run_in_executor(executor, call)

        assert seen == ["req-42"]


class TestRunStepResult:
    async def test_returns_what_the_step_returned(self) -> None:
        async def step() -> str:
            return "done"

        assert await run_to_completion(step(), timeout=_LONG) == "done"

    async def test_raises_what_the_step_raised(self) -> None:
        async def step() -> None:
            raise ValueError("boom")

        call = step()

        with pytest.raises(ValueError, match="boom"):
            await run_to_completion(call, timeout=_LONG)


class TestRunStepTimeout:
    async def test_a_step_outlasting_its_timeout_raises_timeout_error(self) -> None:
        """The step ends on its own a little later, so an unbounded run fails instead of hanging."""
        call = asyncio.sleep(0.5)

        with pytest.raises(TimeoutError):
            await run_to_completion(call, timeout=0.01)


class TestRunStepCancellation:
    async def test_waits_through_repeated_cancellations_and_raises_the_first(self) -> None:
        finished: list[bool] = []
        gate = asyncio.Event()

        async def step() -> None:
            await gate.wait()
            finished.append(True)

        caller = asyncio.create_task(run_to_completion(step(), timeout=_LONG))
        await asyncio.sleep(0)
        caller.cancel("first")
        await asyncio.sleep(0)
        caller.cancel("second")
        await asyncio.sleep(0)
        gate.set()

        with pytest.raises(asyncio.CancelledError) as raised:
            await caller

        assert finished == [True]
        assert raised.value.args == ("first",)

    async def test_a_step_that_ends_cancelled_still_raises_the_callers_cancellation(self) -> None:
        gate = asyncio.Event()

        async def step() -> None:
            await gate.wait()
            raise asyncio.CancelledError("step's own")

        caller = asyncio.create_task(run_to_completion(step(), timeout=_LONG))
        await asyncio.sleep(0)
        caller.cancel("caller's")
        await asyncio.sleep(0)
        gate.set()

        with pytest.raises(asyncio.CancelledError) as raised:
            await caller

        assert raised.value.args == ("caller's",)


def _sqlalchemy() -> tuple[object, str]:
    return SQLAlchemyUnitOfWork(make_session_manager(make_session())), "_exit_session"


def _mongo() -> tuple[object, str]:
    return MongoUnitOfWork(MagicMock(), DEFAULT_STORE), "_end_session"


class TestCancelledClose:
    @pytest.mark.parametrize("unit_of_work", [_sqlalchemy, _mongo], ids=["sqlalchemy", "mongo"])
    async def test_a_cancelled_close_is_not_collected_before_it_ends(
        self, unit_of_work: Callable[[], tuple[object, str]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Nothing else holds a driver call waiting on its socket.

        Dropped, its session would leak.
        """
        uow, ending = unit_of_work()
        waiting: list[weakref.ref[asyncio.Future[None]]] = []
        ended: list[bool] = []

        async def end() -> None:
            released = asyncio.get_running_loop().create_future()
            waiting.append(weakref.ref(released))
            try:
                await released
            finally:
                ended.append(released.done())

        monkeypatch.setattr(uow, ending, end)
        close = asyncio.create_task(cast(Any, uow).close())
        await asyncio.sleep(0)
        close.cancel()
        with pytest.raises(asyncio.CancelledError):
            await close
        gc.collect()

        released = waiting[0]()
        assert released is not None, "the close was collected before it ended"
        released.set_result(None)
        await asyncio.sleep(0)
        assert ended == [True]
