"""The behavioural canary of spec 019, one test per rule.

Rule 1: whatever loom hands a foreign framework goes through the boundary, so
a failure leaves loom with no foreign text. Every registration API on the
boundary list hands its framework one of the callable kinds below; each one
raises a foreign exception holding the canary, or a ``LoomError`` raised from
it, and what leaves holds no canary in its message, its fields, its chain, its
notes or its pickle, while the log holds the type and frames only.

Rule 2: a ``LoomError`` is built from declared fields, rendered once, so a
``Secret`` and a long string never reach its message.
"""

from __future__ import annotations

import asyncio
import logging
import pickle
import sys
import threading
from collections.abc import AsyncIterator, Callable, Iterator
from types import MappingProxyType, TracebackType
from typing import Any, ClassVar

import pytest

from loom.core.errors import SystemError
from loom.core.errors.boundary import BoundaryClass, ProcessHooks, boundary
from loom.core.errors.failure import Failure
from loom.core.fields import MASK, MAX_VALUE_LENGTH, TRUNCATION_MARKER, Secret
from loom.core.logger import LogField
from tests.helpers.canary import CANARY, canary_failure


class _Declared(SystemError):
    template = "charge {setting} failed: {reason}"
    field_keys = MappingProxyType({"setting": LogField.SETTING, "reason": LogField.REASON})


def _foreign() -> Exception:
    return canary_failure()


def _loom_from_foreign() -> Exception:
    try:
        raise _Declared(setting=Secret(CANARY), reason="declined") from canary_failure()
    except _Declared as error:
        error.add_note(CANARY)
        return error


_FAILURES = {"foreign": _foreign, "loom-from-foreign": _loom_from_foreign}


def _sync(make: Callable[[], Exception]) -> Callable[[], Any]:
    def run() -> None:
        raise make()

    return lambda: boundary(run)()


def _coroutine(make: Callable[[], Exception]) -> Callable[[], Any]:
    async def run() -> None:
        raise make()

    return lambda: asyncio.run(boundary(run)())


def _generator(make: Callable[[], Exception]) -> Callable[[], Any]:
    def steps() -> Iterator[int]:
        yield 1
        raise make()

    return lambda: list(boundary(steps)())


def _async_generator(make: Callable[[], Exception]) -> Callable[[], Any]:
    async def steps() -> AsyncIterator[int]:
        yield 1
        raise make()

    async def drain() -> None:
        async for _ in boundary(steps)():
            pass

    return lambda: asyncio.run(drain())


def _method(make: Callable[[], Exception]) -> Callable[[], Any]:
    class _Partition(BoundaryClass):
        BOUNDARY_METHODS: ClassVar[frozenset[str]] = frozenset({"next_batch"})

        def next_batch(self) -> list[int]:
            raise make()

    return lambda: _Partition().next_batch()


_CALLABLE_KINDS = {
    "function": _sync,
    "coroutine": _coroutine,
    "generator": _generator,
    "async-generator": _async_generator,
    "framework-method": _method,
}
"""The kinds of callable the registration APIs hand over: Celery, Typer, SQLAlchemy and
bytewax operators take functions; pydantic-ai tools, Starlette handlers and ASGI take
coroutines; SSE streams are generators; bytewax partitions and pydantic-ai toolsets
are methods of a loom class."""


def _carries_canary(leaving: BaseException) -> bool:
    texts = [str(leaving), repr(leaving), repr(leaving.args), repr(Failure(leaving).fields())]
    texts += [repr(getattr(leaving, "__notes__", ())), repr(leaving.__cause__)]
    texts.append(repr(leaving.__context__))
    return any(CANARY in text for text in texts) or CANARY.encode() in pickle.dumps(leaving)


@pytest.fixture
def log_text(caplog: pytest.LogCaptureFixture) -> Callable[[], str]:
    caplog.set_level(logging.DEBUG)
    return lambda: caplog.text + repr([r.__dict__ for r in caplog.records])


class TestRuleOneCanary:
    @pytest.mark.parametrize("failure", _FAILURES.values(), ids=_FAILURES.keys())
    @pytest.mark.parametrize("kind", _CALLABLE_KINDS.values(), ids=_CALLABLE_KINDS.keys())
    def test_nothing_that_leaves_loom_or_its_log_carries_the_canary(
        self,
        kind: Callable[[Callable[[], Exception]], Callable[[], Any]],
        failure: Callable[[], Exception],
        log_text: Callable[[], str],
    ) -> None:
        with pytest.raises(Exception) as raised:
            kind(failure)()

        assert not _carries_canary(raised.value)
        assert "exception.stacktrace" in log_text()
        assert CANARY not in log_text()

    def test_a_process_hook_hands_on_no_canary(
        self, monkeypatch: pytest.MonkeyPatch, log_text: Callable[[], str]
    ) -> None:
        handed: list[BaseException] = []

        def previous(
            kind: type[BaseException], exc: BaseException, tb: TracebackType | None
        ) -> None:
            handed.append(exc)

        monkeypatch.setattr(sys, "excepthook", previous)
        monkeypatch.setattr(threading, "excepthook", threading.excepthook)
        monkeypatch.setattr(sys, "unraisablehook", sys.unraisablehook)
        error = canary_failure()

        with ProcessHooks():
            sys.excepthook(type(error), error, error.__traceback__)

        [leaving] = handed
        assert not _carries_canary(leaving)
        assert CANARY not in log_text()


class TestRuleTwoCanary:
    def test_a_secret_is_masked_and_a_long_string_is_cut_in_the_message(self) -> None:
        error = _Declared(setting=Secret(CANARY), reason="x" * MAX_VALUE_LENGTH + CANARY)

        assert MASK in str(error)
        assert TRUNCATION_MARKER in str(error)
        assert not _carries_canary(error)
