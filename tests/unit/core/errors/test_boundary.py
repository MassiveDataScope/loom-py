"""The loom boundary: what leaves a callable loom hands out, and the process hooks."""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from types import TracebackType
from typing import Any

import pytest

from loom.core.errors import NotFound, UnexpectedError
from loom.core.errors.boundary import ProcessHooks, boundary
from loom.core.errors.log_events import ErrorLogEvent, ErrorLogField

_SECRET = "card 4111-1111-1111-1111"


class _HostSignal(Exception):
    """A host's control flow, declared by its adapter as a pass-through."""


@pytest.fixture
def reports(caplog: pytest.LogCaptureFixture) -> Callable[[], list[logging.LogRecord]]:
    """The records the boundary wrote about a failure leaving loom."""
    caplog.set_level(logging.DEBUG, logger="loom.core.errors.failure")

    def left() -> list[logging.LogRecord]:
        return [r for r in caplog.records if r.getMessage() == ErrorLogEvent.FAILURE_LEFT_LOOM]

    return left


def _failing() -> None:
    try:
        raise KeyError(_SECRET)
    except KeyError:
        raise ValueError(_SECRET) from None


def _steps() -> Iterator[int]:
    yield 1
    raise ValueError(_SECRET)


async def _async_failing() -> None:
    raise ValueError(_SECRET)


async def _async_steps() -> AsyncIterator[int]:
    yield 1
    raise ValueError(_SECRET)


async def _drive(fn: Callable[..., Any]) -> list[int]:
    """Call *fn* the way a host would, whatever its kind, and collect what it yields."""
    result = fn()
    if isinstance(result, Iterator):
        return list(result)
    if isinstance(result, AsyncIterator):
        return [item async for item in result]
    await result
    return []


class TestBoundary:
    @pytest.mark.parametrize(
        "fn", [_failing, _steps, _async_failing, _async_steps], ids=["sync", "gen", "async", "agen"]
    )
    async def test_a_foreign_failure_leaves_as_an_unexpected_error_with_no_chain(
        self, fn: Callable[..., Any], reports: Callable[[], list[logging.LogRecord]]
    ) -> None:
        with pytest.raises(UnexpectedError) as raised:
            await _drive(boundary(fn))

        leaving = raised.value
        assert leaving.original_type == "ValueError"
        assert (leaving.__cause__, leaving.__context__) == (None, None)
        [record] = reports()
        assert record.__dict__[ErrorLogField.EXCEPTION_TYPE] == "ValueError"

    def test_a_generator_yields_as_usual_until_it_fails(self) -> None:
        steps = boundary(_steps)()

        assert next(steps) == 1
        with pytest.raises(UnexpectedError):
            next(steps)

    def test_a_loom_error_leaves_as_itself_unlogged_when_expected(
        self, reports: Callable[[], list[logging.LogRecord]]
    ) -> None:
        error = NotFound("Invoice", id=1)

        def find() -> None:
            raise error

        with pytest.raises(NotFound) as raised:
            boundary(find)()

        assert raised.value is error
        assert reports() == []

    @pytest.mark.parametrize(
        "signal", [_HostSignal(_SECRET), KeyboardInterrupt()], ids=["pass-through", "base"]
    )
    def test_a_host_signal_or_a_base_exception_passes_untouched(
        self, signal: BaseException, reports: Callable[[], list[logging.LogRecord]]
    ) -> None:
        def steer() -> None:
            raise signal

        with pytest.raises(type(signal)) as raised:
            boundary(steer, pass_through=(_HostSignal,))()

        assert raised.value is signal
        assert reports() == []

    def test_wrapping_twice_wraps_once_and_keeps_the_signature(self) -> None:
        def handle(event: dict[str, Any], context: object) -> int:
            """Handle one event."""
            return 1

        once = boundary(handle)

        assert boundary(once) is once
        assert (once.__name__, once.__doc__, once({}, None)) == ("handle", "Handle one event.", 1)


class _Recorded:
    """A previous hook: it keeps the exception it was handed."""

    def __init__(self) -> None:
        self.seen: list[BaseException | None] = []

    def excepthook(
        self, kind: type[BaseException], exc: BaseException, tb: TracebackType | None
    ) -> None:
        self.seen.append(exc)

    def thread_hook(self, args: threading.ExceptHookArgs) -> None:
        self.seen.append(args.exc_value)

    def unraisable_hook(self, args: Any) -> None:
        self.seen.append(args.exc_value)


@pytest.fixture
def previous(monkeypatch: pytest.MonkeyPatch) -> _Recorded:
    """Recording hooks in place of the interpreter's, put back after the test."""
    recorded = _Recorded()
    monkeypatch.setattr(sys, "excepthook", recorded.excepthook)
    monkeypatch.setattr(threading, "excepthook", recorded.thread_hook)
    monkeypatch.setattr(sys, "unraisablehook", recorded.unraisable_hook)
    return recorded


class TestProcessHooks:
    def test_restoring_puts_the_previous_hooks_back(self, previous: _Recorded) -> None:
        before = (sys.excepthook, threading.excepthook, sys.unraisablehook)

        with ProcessHooks():
            assert sys.excepthook != before[0]

        assert (sys.excepthook, threading.excepthook, sys.unraisablehook) == before

    def test_the_interpreter_hook_logs_once_and_hands_on_the_translated_failure(
        self, previous: _Recorded, reports: Callable[[], list[logging.LogRecord]]
    ) -> None:
        error = ValueError(_SECRET)

        with ProcessHooks():
            sys.excepthook(ValueError, error, None)

        [handed] = previous.seen
        assert isinstance(handed, UnexpectedError)
        assert len(reports()) == 1

    def test_a_thread_failure_is_translated_before_the_previous_hook(
        self, previous: _Recorded, reports: Callable[[], list[logging.LogRecord]]
    ) -> None:
        def fail() -> None:
            raise ValueError(_SECRET)

        with ProcessHooks():
            worker = threading.Thread(target=fail)
            worker.start()
            worker.join()

        [handed] = previous.seen
        assert isinstance(handed, UnexpectedError)
        assert len(reports()) == 1

    def test_ctrl_c_reaches_the_previous_hook_as_it_is(self, previous: _Recorded) -> None:
        interrupt = KeyboardInterrupt()

        with ProcessHooks():
            sys.excepthook(KeyboardInterrupt, interrupt, None)

        assert previous.seen == [interrupt]

    async def test_an_unretrieved_task_failure_on_a_loom_loop_is_logged_once(
        self, reports: Callable[[], list[logging.LogRecord]]
    ) -> None:
        ProcessHooks.handle_asyncio_exception(
            asyncio.get_running_loop(),
            {"message": f"Task exception was never retrieved {_SECRET}", "exception": ValueError()},
        )

        [record] = reports()
        assert _SECRET not in str(record.__dict__)
