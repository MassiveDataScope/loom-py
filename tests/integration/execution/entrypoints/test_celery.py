"""Celery: the job service builds inside the execution; callback tasks act as the dispatcher.

The callback tasks run eagerly on a real Celery application: a sync callback
in the task's thread, an async one in the coroutine the worker's async bridge
runs (here its eager fallback, ``asyncio.run``). The task runs under alice's
ambient identity, as an eager callback does inside the job that links it: the
link envelope's identity governs, never the worker's (D7 revised, T09a).
"""

from __future__ import annotations

import asyncio
import contextvars
import time
from collections.abc import Callable, Coroutine, Iterator
from dataclasses import replace
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from celery import Celery  # type: ignore[import-untyped]

from loom.celery.runner import (
    _CeleryAsyncRuntime,  # pyright: ignore[reportPrivateUsage]
    _make_callback_task,  # pyright: ignore[reportPrivateUsage]
    _make_job_task,  # pyright: ignore[reportPrivateUsage]
)
from loom.celery.service import CeleryJobService
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import DelegationExpired, Unauthenticated
from loom.core.identity import Delegation, Identity
from loom.core.job.callback import CallbackOutcome
from loom.core.job.config import JobSettings
from loom.core.job.envelope import wire_job_name
from loom.core.observability.event import EventKind, Scope
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.markers import Caller
from tests.helpers.delegation import acting_as, assert_new_delegation
from tests.helpers.spans import build_recorder

from .doubles import (
    BOB,
    CALLBACKS,
    NO_CALLER,
    AcmeAsyncCallback,
    AcmeCallback,
    AcmeCallerCallback,
    AcmeProbeJob,
    AcmeRun,
    CallbackLog,
    seen_now,
)

AN_HOUR = 3600.0


def link_from(identity: Identity, *, until: float) -> dict[str, Any]:
    """The caller part of a link envelope a dispatch acting as *identity* builds."""
    return {"identity": Delegation(identity, until).encoded()}


@pytest.fixture
def celery_app() -> Celery:
    app = Celery("acme", set_as_current=False)
    app.conf.task_always_eager = True
    return app


@pytest.fixture
def backend_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """The result backend holds the failed job's exception."""
    monkeypatch.setattr(
        "loom.celery.runner._resolve_error_info",
        lambda job_id, app: {"exc_type": "LookupError", "exc_msg": "acme"},
    )


@pytest.fixture
def succeed(celery_app: Celery, factory: UseCaseFactory) -> Callable[[type[Any]], Any]:
    """Register the success callback task of a callback class and apply it eagerly."""

    def apply(callback: type[Any], **link: Any) -> Any:
        task = _make_callback_task(
            celery_app, callback, CallbackOutcome.SUCCESS, factory, _CeleryAsyncRuntime()
        )
        kwargs = {"job_id": "acme-job", "context": {}, **link}
        return task.apply(args=("acme-result",), kwargs=kwargs)

    return apply


class TestCeleryJobService:
    async def test_run_builds_inside_an_execution_as_the_caller_acting(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        service = CeleryJobService(cast(Any, None), factory=factory, executor=executor)

        with acting_as(BOB, until=time.time() + AN_HOUR):
            job = await service.run(AcmeProbeJob)

        assert job.session.caller is BOB
        assert job.session.execution is job.ran_in

    async def test_run_with_nothing_acting_has_no_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        service = CeleryJobService(cast(Any, None), factory=factory, executor=executor)

        with pytest.raises(Unauthenticated):
            await service.run(AcmeProbeJob)


class TestCeleryJobTask:
    """The worker task resumes the envelope's caller once, eagerly and through the bridge."""

    @pytest.mark.parametrize("bridged", [False, True], ids=["eager", "bridge"])
    def test_acts_as_the_envelope_caller_within_its_delegation(
        self,
        celery_app: Celery,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        bridged: bool,
    ) -> None:
        runtime = _CeleryAsyncRuntime()
        if bridged:
            runtime.initialize()
        try:
            _make_job_task(
                celery_app, AcmeProbeJob, factory, executor, runtime, settings=JobSettings()
            )
            task = celery_app.tasks[wire_job_name(AcmeProbeJob.__qualname__)]
            result = task.apply(kwargs=link_from(BOB, until=time.time() + AN_HOUR))
            job = result.get()
        finally:
            runtime.shutdown()

        assert job.session.caller == BOB
        assert job.session.execution is job.ran_in

    async def test_a_dispatch_and_its_links_carry_the_caller_and_its_deadline(
        self, factory: UseCaseFactory, executor: RuntimeExecutor, window: float
    ) -> None:
        app = MagicMock()
        service = CeleryJobService(app, factory=factory, executor=executor)

        async def dispatch() -> None:
            service.dispatch(AcmeProbeJob, on_success=AcmeCallback, on_failure=AcmeCallback)

        before = time.time()
        await executor.execute(AcmeRun(dispatch), identity=BOB)

        sent = app.send_task.call_args.kwargs["kwargs"]["identity"]
        links = [call.kwargs["kwargs"]["identity"] for call in app.signature.call_args_list]
        assert_new_delegation(sent["until"], before, window)
        assert sent == Delegation(BOB, sent["until"]).encoded()
        assert links == [sent, sent]


@pytest.mark.usefixtures("as_alice")
class TestCeleryCallbackTasks:
    @CALLBACKS
    def test_a_success_callback_acts_as_the_identity_its_link_carries(
        self,
        succeed: Callable[..., Any],
        callback_log: CallbackLog,
        callback: type[AcmeCallback],
    ) -> None:
        succeed(callback, **link_from(BOB, until=time.time() + AN_HOUR)).get()

        assert [seen.caller for _, seen in callback_log] == [BOB, BOB]
        assert callback_log[1][0] == "on_success"

    @CALLBACKS
    @pytest.mark.usefixtures("backend_error")
    def test_a_failure_callback_acts_as_the_identity_its_link_carries(
        self,
        celery_app: Celery,
        factory: UseCaseFactory,
        callback_log: CallbackLog,
        callback: type[AcmeCallback],
    ) -> None:
        task = _make_callback_task(
            celery_app, callback, CallbackOutcome.FAILURE, factory, _CeleryAsyncRuntime()
        )
        link = link_from(BOB, until=time.time() + AN_HOUR)

        task.apply(kwargs={"job_id": "acme-job", "context": {}, **link}).get()

        assert [seen.caller for _, seen in callback_log] == [BOB, BOB]
        assert callback_log[1][0] == "on_failure"

    @CALLBACKS
    def test_a_link_without_an_identity_acts_as_no_one_not_as_the_worker(
        self,
        succeed: Callable[..., Any],
        callback_log: CallbackLog,
        callback: type[AcmeCallback],
    ) -> None:
        """A link minted before the contract carried the caller fails closed."""
        succeed(callback).get()

        assert callback_log == [("built", NO_CALLER), ("on_success", NO_CALLER)]

    def test_a_constructor_caller_receives_the_dispatcher(
        self, succeed: Callable[..., Any], callback_log: CallbackLog
    ) -> None:
        succeed(AcmeCallerCallback, **link_from(BOB, until=time.time() + AN_HOUR)).get()

        [(_, seen)] = callback_log
        assert seen.caller is not None
        assert seen.caller.subject == BOB.subject

    def test_a_constructor_caller_is_refused_when_the_link_carries_none(
        self, succeed: Callable[..., Any]
    ) -> None:
        result = succeed(AcmeCallerCallback)

        with pytest.raises(Unauthenticated, match="identity="):
            result.get()

    def test_a_link_past_its_delegation_deadline_is_refused(
        self, succeed: Callable[..., Any], callback_log: CallbackLog
    ) -> None:
        result = succeed(AcmeCallback, **link_from(BOB, until=time.time() - 1))

        with pytest.raises(Unauthenticated, match="system identity"):
            result.get()
        assert callback_log == []


class TestFailureCallbackBackendRead:
    """The failed job's exception is read after the link's caller is checked, inside the span."""

    @CALLBACKS
    def test_a_link_past_its_deadline_reads_nothing_from_the_backend(
        self,
        celery_app: Celery,
        factory: UseCaseFactory,
        callback_log: CallbackLog,
        callback: type[AcmeCallback],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        read = MagicMock()
        monkeypatch.setattr("loom.celery.runner._resolve_error_info", read)
        task = _make_callback_task(
            celery_app, callback, CallbackOutcome.FAILURE, factory, _CeleryAsyncRuntime()
        )
        link = link_from(BOB, until=time.time() - 1)
        result = task.apply(kwargs={"job_id": "acme-job", "context": {}, **link})

        with pytest.raises(DelegationExpired):
            result.get()

        read.assert_not_called()
        assert callback_log == []

    @CALLBACKS
    def test_a_failing_read_is_recorded_on_the_callback_span(
        self,
        celery_app: Celery,
        factory: UseCaseFactory,
        callback: type[AcmeCallback],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        recorder = build_recorder()
        monkeypatch.setattr(
            "loom.celery.runner._resolve_error_info", MagicMock(side_effect=ConnectionError("acme"))
        )
        task = _make_callback_task(
            celery_app,
            callback,
            CallbackOutcome.FAILURE,
            factory,
            _CeleryAsyncRuntime(),
            observability_runtime=recorder.runtime,
        )
        link = link_from(BOB, until=time.time() + AN_HOUR)
        result = task.apply(kwargs={"job_id": "acme-job", "context": {}, **link})

        with pytest.raises(ConnectionError):
            result.get()

        [error] = [
            event.kind
            for event in recorder.collector.scoped(Scope.JOB)
            if event.name == callback.__qualname__ and event.kind is not EventKind.START
        ]
        assert error is EventKind.ERROR


class _ContextFreeBridge:
    """An async bridge whose coroutines start in an empty context: no context variable crosses."""

    is_alive = True

    def __init__(self, **options: Any) -> None:
        del options

    def run(self, coro: Coroutine[Any, Any, Any], *, timeout: float | None = None) -> Any:
        return contextvars.Context().run(asyncio.run, coro)

    def shutdown(self) -> None:
        """Nothing to release."""


class AcmeAsyncCallerCallback:
    """An async job callback whose constructor declares ``Caller()``."""

    def __init__(self, log: CallbackLog, caller: Identity = Caller()) -> None:
        log.append(("caller", replace(seen_now(), caller=caller)))
        self.log = log

    async def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        self.log.append(("on_success", seen_now()))

    async def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        self.log.append(("on_failure", seen_now()))


@pytest.fixture(params=["portal", "context-free"])
def bridged(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """A worker runtime whose bridge is anyio's portal, or one that carries no context variable."""
    if request.param == "context-free":
        monkeypatch.setattr("loom.celery.runner.AsyncBridge", _ContextFreeBridge)
    runtime = _CeleryAsyncRuntime()
    runtime.initialize()
    try:
        yield runtime
    finally:
        runtime.shutdown()


@pytest.mark.usefixtures("as_alice", "backend_error")
class TestAsyncCallbackThroughTheBridge:
    """An async callback runs on the bridge's loop, still as the identity its link carries."""

    @pytest.mark.parametrize(
        ("outcome", "job_result"),
        [(CallbackOutcome.SUCCESS, ("acme-result",)), (CallbackOutcome.FAILURE, ())],
        ids=["success", "failure"],
    )
    def test_its_constructor_caller_and_handle_see_the_link_caller(
        self,
        celery_app: Celery,
        factory: UseCaseFactory,
        callback_log: CallbackLog,
        bridged: Any,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
    ) -> None:
        task = _make_callback_task(celery_app, AcmeAsyncCallerCallback, outcome, factory, bridged)
        link = {"job_id": "acme-job", "context": {}, **link_from(BOB, until=time.time() + AN_HOUR)}

        task.apply(args=job_result, kwargs=link).get()

        [(_, built), (hook, called)] = callback_log
        assert built.caller == BOB
        assert hook == f"on_{outcome}"
        assert (called.caller, called.handle) == (BOB, BOB.subject)

    def test_a_link_without_an_identity_fails_closed_on_the_bridge(
        self, celery_app: Celery, factory: UseCaseFactory, callback_log: CallbackLog, bridged: Any
    ) -> None:
        success = CallbackOutcome.SUCCESS
        task = _make_callback_task(celery_app, AcmeAsyncCallback, success, factory, bridged)

        task.apply(args=("acme-result",), kwargs={"job_id": "acme-job", "context": {}}).get()

        assert callback_log == [("built", NO_CALLER), ("on_success", NO_CALLER)]
