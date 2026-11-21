"""The behavioural canary of handoffs: what leaves the process never carries a failure's text.

Every runtime adapter lists its handoffs as a :class:`~loom.core.logger.failure.Handoff`
subclass. Each member has a driver here that makes the adapter hand off
:func:`~tests.helpers.canary.canary_failure` through its real code path and
returns everything the foreign side received, as text. A member with no driver
fails :meth:`TestHandoffList.test_every_handoff_has_a_driver`.
"""

from __future__ import annotations

import ast
import asyncio
import traceback
from collections.abc import AsyncIterator, Callable, MutableMapping
from contextlib import asynccontextmanager, suppress
from typing import Any

import pytest
from celery.exceptions import Retry
from starlette.applications import Starlette

from loom.celery.runner import CeleryHandoff, _CeleryProcessBridge
from loom.celery.runtime import _WorkerLifecycle
from loom.core.errors import Conflict
from loom.core.job.job import Job
from loom.core.job.policy import Backoff
from loom.core.logger.failure import FailureReference, Handoff
from loom.rest.errors import HttpErrorMapper
from loom.rest.fastapi.runtime import RestHandoff, application_lifespan
from loom.serverless.aws.lambda_.boundary import LambdaBoundary, LambdaHandoff, lambda_init
from tests.helpers.canary import CANARY, raise_canary_failure
from tests.helpers.jobs import CeleryWorker
from tests.helpers.source_tree import parsed, source_files


def _raised_text(call: Callable[[], object]) -> str:
    """Everything a runtime that catches what *call* raises can write: type, text, traceback."""
    try:
        call()
    except BaseException as caught:  # noqa: BLE001 - the canary reads whatever leaves
        return "".join(traceback.format_exception(caught)) + repr(caught) + repr(caught.args)
    pytest.fail("the handoff raised nothing")


def _lambda_init() -> str:
    return _raised_text(lambda: lambda_init(raise_canary_failure))


def _lambda_invoke() -> str:
    return _raised_text(lambda: LambdaBoundary(raise_canary_failure)({}, None))  # type: ignore[arg-type]


class _Application:
    """Stands in for an application whose resources fail to open or to close."""

    def __init__(self, *, fails_on_close: bool) -> None:
        self._fails_on_close = fails_on_close

    @asynccontextmanager
    async def running(self) -> AsyncIterator[None]:
        if not self._fails_on_close:
            raise_canary_failure()
        yield
        raise_canary_failure()


async def _lifespan_messages(*, fails_on_close: bool) -> list[dict[str, Any]]:
    """What the ASGI server receives over the lifespan protocol, as uvicorn logs it."""
    app = Starlette(lifespan=application_lifespan(_Application(fails_on_close=fails_on_close)))  # type: ignore[arg-type]
    inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
    for kind in ("lifespan.startup", "lifespan.shutdown"):
        inbox.put_nowait({"type": kind})
    sent: list[dict[str, Any]] = []

    async def send(message: MutableMapping[str, Any]) -> None:
        sent.append(dict(message))

    with suppress(BaseException):
        await app({"type": "lifespan", "asgi": {"version": "3.0"}, "state": {}}, inbox.get, send)
    return sent


def _lifespan(*, fails_on_close: bool) -> str:
    sent = asyncio.run(_lifespan_messages(fails_on_close=fails_on_close))
    failed = [m for m in sent if m["type"].endswith(".failed")]
    assert failed, sent
    return repr(sent)


def _rest_error_body() -> str:
    return repr(HttpErrorMapper().to_http(Conflict(CANARY)).detail)


class _CanaryJob(Job[None]):
    __retry__ = Backoff(first_s=3, factor=3, max_s=300, jitter=False)

    def execute(self) -> None:
        raise_canary_failure()


_LAST_ATTEMPT = 3
_CALLBACK = "loom.callback_error.AcmeCallback"


def _celery(run: Callable[[CeleryWorker], str]) -> str:
    worker = CeleryWorker()
    try:
        return run(worker)
    finally:
        worker.close()


def _celery_result(worker: CeleryWorker) -> str:
    return _raised_text(lambda: worker.run(_CanaryJob, attempt=_LAST_ATTEMPT))


def _celery_retry(worker: CeleryWorker) -> str:
    try:
        worker.run(_CanaryJob, attempt=1)
    except Retry as retried:
        stored = "".join(traceback.format_exception(retried)) + repr(retried.exc)
        return stored + repr(retried.exc.args if retried.exc else ())
    pytest.fail("the job was not retried")


def _celery_failure_callback(worker: CeleryWorker) -> str:
    with suppress(BaseException):
        worker.run(_CanaryJob, attempt=_LAST_ATTEMPT, failure_callback=_CALLBACK)
    (message,) = worker.published(_CALLBACK)
    return repr(message.kwargs)


class _FailingToOpen:
    @asynccontextmanager
    async def running(self, *, check: bool = True) -> AsyncIterator[None]:
        raise_canary_failure()
        yield


def _celery_worker_init() -> str:
    bridge = _CeleryProcessBridge()
    lifecycle = _WorkerLifecycle(_FailingToOpen(), bridge)  # type: ignore[arg-type]
    lifecycle.on_process_init()
    refused = _raised_text(bridge.ensure_open)
    terminated = _raised_text(lambda: lifecycle.on_worker_init(None))
    return refused + terminated


DRIVERS: dict[Handoff, Callable[[], str]] = {
    LambdaHandoff.INIT: _lambda_init,
    LambdaHandoff.INVOKE: _lambda_invoke,
    RestHandoff.LIFESPAN_STARTUP: lambda: _lifespan(fails_on_close=False),
    RestHandoff.LIFESPAN_SHUTDOWN: lambda: _lifespan(fails_on_close=True),
    RestHandoff.ERROR_BODY: _rest_error_body,
    CeleryHandoff.RESULT: lambda: _celery(_celery_result),
    CeleryHandoff.RETRY: lambda: _celery(_celery_retry),
    CeleryHandoff.FAILURE_CALLBACK: lambda: _celery(_celery_failure_callback),
    CeleryHandoff.WORKER_INIT: _celery_worker_init,
}


def _declared_handoff_classes() -> set[str]:
    """The names of every ``Handoff`` subclass ``src/loom`` declares."""
    return {
        node.name
        for path in source_files()
        for node in ast.walk(parsed(path))
        if isinstance(node, ast.ClassDef)
        and any(isinstance(base, ast.Name) and base.id == "Handoff" for base in node.bases)
    }


class TestHandoffList:
    def test_every_declared_handoff_list_is_driven(self) -> None:
        driven = {type(member).__name__ for member in DRIVERS}

        assert _declared_handoff_classes() == driven

    def test_every_handoff_has_a_driver(self) -> None:
        lists: set[type[Handoff]] = {type(member) for member in DRIVERS}
        members = {member for kind in lists for member in kind}

        assert members == set(DRIVERS)


class TestHandoffCanary:
    @pytest.mark.parametrize("handoff", list(DRIVERS), ids=str)
    def test_no_handoff_carries_the_canary(self, handoff: Handoff) -> None:
        received = DRIVERS[handoff]()

        assert received, "the foreign side received the failure"
        assert CANARY not in received

    def test_a_handed_off_reference_has_no_context(self) -> None:
        with pytest.raises(FailureReference) as raised:
            LambdaBoundary(raise_canary_failure)({}, None)  # type: ignore[arg-type]

        assert (raised.value.__context__, raised.value.__cause__) == (None, None)
