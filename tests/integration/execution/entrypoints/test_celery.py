"""Celery: the job service builds inside the execution; callback tasks act as the dispatcher.

The callback tasks run eagerly on a real Celery application: a sync callback
in the task's thread, an async one in the coroutine the worker's async bridge
runs (here its eager fallback, ``asyncio.run``). The task runs under alice's
ambient identity, as an eager callback does inside the job that links it: the
link envelope's identity governs, never the worker's (D7 revised, T09a).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from celery import Celery  # type: ignore[import-untyped]

from loom.celery.constants import TASK_CALLBACK_ERROR_PREFIX, TASK_CALLBACK_PREFIX
from loom.celery.runner import (
    _CeleryAsyncRuntime,  # pyright: ignore[reportPrivateUsage]
    _make_callback_error_task,  # pyright: ignore[reportPrivateUsage]
    _make_callback_task,  # pyright: ignore[reportPrivateUsage]
    _make_job_task,  # pyright: ignore[reportPrivateUsage]
)
from loom.celery.service import CeleryJobService
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import Unauthenticated
from loom.core.identity import Delegation, Identity
from loom.core.job.config import JobSettings
from loom.core.job.envelope import wire_job_name
from loom.core.use_case.factory import UseCaseFactory
from tests.helpers.delegation import acting_as, assert_new_delegation

from .doubles import (
    BOB,
    CALLBACKS,
    NO_CALLER,
    AcmeCallback,
    AcmeCallerCallback,
    AcmeProbeJob,
    AcmeRun,
    CallbackLog,
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
        "loom.celery.runner._resolve_error_info", lambda job_id, app: ("LookupError", "acme")
    )


@pytest.fixture
def succeed(celery_app: Celery, factory: UseCaseFactory) -> Callable[[type[Any]], Any]:
    """Register the success callback task of a callback class and apply it eagerly."""

    def apply(callback: type[Any], **link: Any) -> Any:
        _make_callback_task(celery_app, callback, factory, _CeleryAsyncRuntime())
        task = celery_app.tasks[f"{TASK_CALLBACK_PREFIX}.{callback.__qualname__}"]
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
        _make_callback_error_task(celery_app, callback, factory, _CeleryAsyncRuntime())
        task = celery_app.tasks[f"{TASK_CALLBACK_ERROR_PREFIX}.{callback.__qualname__}"]
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
        assert seen.caller is not None and seen.caller.subject == BOB.subject

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
