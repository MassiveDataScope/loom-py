"""The behavioural canary of spec 019, one test per rule.

Rule 1: whatever loom hands to code outside loom goes through the boundary, so
a failure leaves loom with no foreign text. Each callable kind below raises a
foreign exception holding the canary, or a ``LoomError`` raised from it, and
what leaves holds no canary in its message, its fields, its chain, its notes or
its pickle, while the log holds the type and frames only. Each kind of foreign
callee the boundary rule finds is driven once through its real API too.

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
from loom.core.logger import LogField
from loom.core.vocabulary import MASK, MAX_VALUE_LENGTH, TRUNCATION_MARKER, Secret
from tests.helpers.canary import CANARY, canary_failure


class _Declared(SystemError):
    template = "charge {setting} failed: {reason}"
    attributes = MappingProxyType({"setting": LogField.SETTING, "reason": LogField.REASON})


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
"""The kinds of callable loom hands out: Celery, Typer, SQLAlchemy and bytewax
operators take functions; pydantic-ai tools, Starlette handlers and ASGI take
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


def _failing() -> None:
    raise canary_failure()


async def _async_failing() -> None:
    raise canary_failure()


def _through_celery() -> list[BaseException]:
    from celery import Celery

    from loom.celery.runner import CELERY_PASS_THROUGH

    app = Celery("canary", set_as_current=False)
    app.conf.task_always_eager = True
    task = app.task(name="canary.task")(boundary(_failing, pass_through=CELERY_PASS_THROUGH))
    return [task.apply().result]


def _through_typer() -> list[BaseException]:
    import typer
    from typer.testing import CliRunner

    from loom.cli.app import TYPER_PASS_THROUGH

    app = typer.Typer()
    app.command()(boundary(_failing, pass_through=TYPER_PASS_THROUGH))
    result = CliRunner().invoke(app, [])
    assert CANARY not in result.output
    return [result.exception] if result.exception is not None else []


def _through_starlette() -> list[BaseException]:
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.testclient import TestClient

    from loom.rest.fastapi._errors import STARLETTE_PASS_THROUGH
    from loom.rest.middleware import BoundaryMiddleware

    async def endpoint(_: object) -> None:
        await _async_failing()

    app = Starlette(routes=[Route("/", boundary(endpoint, pass_through=STARLETTE_PASS_THROUGH))])
    BoundaryMiddleware.mount_on(app, STARLETTE_PASS_THROUGH)
    response = TestClient(app, raise_server_exceptions=False).get("/")
    assert CANARY not in response.text
    with pytest.raises(Exception) as raised:
        TestClient(app).get("/")
    return [raised.value]


def _through_pydantic_ai() -> list[BaseException]:
    from pydantic_ai import Agent
    from pydantic_ai.models.test import TestModel

    from loom.ai.engines.pydantic_ai._failures import PYDANTIC_AI_PASS_THROUGH

    agent: Agent[None, str] = Agent(TestModel())
    agent.tool_plain(boundary(_async_failing, pass_through=PYDANTIC_AI_PASS_THROUGH))
    with pytest.raises(Exception) as raised:
        agent.run_sync("go")
    return [raised.value]


_FOREIGN_CALLEES = {
    "celery-task": _through_celery,
    "typer-command": _through_typer,
    "starlette-route-and-middleware": _through_starlette,
    "pydantic-ai-tool": _through_pydantic_ai,
}
"""The kinds of foreign callee the boundary rule finds, each driven through its real API.

A Prefect task is not here: running one needs Prefect's engine, which the unit
lane does not start; its flow, task and hooks take the same wrapped callables."""


class TestRuleOneCanaryPerCallee:
    @pytest.mark.parametrize("drive", _FOREIGN_CALLEES.values(), ids=_FOREIGN_CALLEES.keys())
    def test_what_the_framework_raises_prints_or_logs_carries_no_canary(
        self, drive: Callable[[], list[BaseException]], log_text: Callable[[], str]
    ) -> None:
        leaving = drive()

        assert leaving
        assert not any(map(_carries_canary, leaving))
        assert CANARY not in log_text()
