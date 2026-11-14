"""Celery: the job service builds inside the execution; callback tasks run with no caller (D7).

The callback tasks run eagerly on a real Celery application: a sync callback
in the task's thread, an async one in the coroutine the worker's async bridge
runs (here its eager fallback, ``asyncio.run``). The task runs under alice's
ambient identity, as an eager callback does inside the job that links it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any, cast

import pytest
from celery import Celery  # type: ignore[import-untyped]

from loom.celery.constants import TASK_CALLBACK_ERROR_PREFIX, TASK_CALLBACK_PREFIX
from loom.celery.runner import (
    _CeleryAsyncRuntime,  # pyright: ignore[reportPrivateUsage]
    _make_callback_error_task,  # pyright: ignore[reportPrivateUsage]
    _make_callback_task,  # pyright: ignore[reportPrivateUsage]
)
from loom.celery.service import CeleryJobService
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import Unauthenticated
from loom.core.identity import ANONYMOUS, reset_identity, set_identity
from loom.core.use_case.factory import UseCaseFactory

from .doubles import (
    ALICE,
    AcmeAsyncCallback,
    AcmeCallback,
    AcmeCallerCallback,
    AcmeProbeJob,
    CallbackLog,
    Seen,
)

NO_CALLER = Seen(
    in_execution=True, execution_identity=None, ambient=ANONYMOUS, handle="Unauthenticated"
)
CALLBACKS = pytest.mark.parametrize(
    "callback", [AcmeCallback, AcmeAsyncCallback], ids=["sync", "async"]
)


@pytest.fixture
def celery_app() -> Celery:
    app = Celery("acme", set_as_current=False)
    app.conf.task_always_eager = True
    return app


@pytest.fixture
def as_alice() -> Iterator[None]:
    token = set_identity(ALICE)
    try:
        yield
    finally:
        reset_identity(token)


@pytest.fixture
def backend_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """The result backend holds the failed job's exception."""
    monkeypatch.setattr(
        "loom.celery.runner._resolve_error_info", lambda job_id, app: ("LookupError", "acme")
    )


@pytest.fixture
def succeed(celery_app: Celery, factory: UseCaseFactory) -> Callable[[type[Any]], Any]:
    """Register the success callback task of a callback class and apply it eagerly."""

    def apply(callback: type[Any]) -> Any:
        _make_callback_task(celery_app, callback, factory, _CeleryAsyncRuntime())
        task = celery_app.tasks[f"{TASK_CALLBACK_PREFIX}.{callback.__qualname__}"]
        return task.apply(args=("acme-result",), kwargs={"job_id": "acme-job", "context": {}})

    return apply


class TestCeleryJobService:
    async def test_run_builds_inside_an_execution_as_the_ambient_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        service = CeleryJobService(cast(Any, None), factory=factory, executor=executor)

        job = await service.run(AcmeProbeJob)

        assert job.session.caller is ANONYMOUS
        assert job.session.execution is job.ran_in


@pytest.mark.usefixtures("as_alice")
class TestCeleryCallbackTasks:
    @CALLBACKS
    def test_a_success_callback_is_built_and_called_with_no_caller(
        self,
        succeed: Callable[[type[Any]], Any],
        callback_log: CallbackLog,
        callback: type[AcmeCallback],
    ) -> None:
        succeed(callback).get()

        assert callback_log == [("built", NO_CALLER), ("on_success", NO_CALLER)]

    @CALLBACKS
    @pytest.mark.usefixtures("backend_error")
    def test_a_failure_callback_is_built_and_called_with_no_caller(
        self,
        celery_app: Celery,
        factory: UseCaseFactory,
        callback_log: CallbackLog,
        callback: type[AcmeCallback],
    ) -> None:
        _make_callback_error_task(celery_app, callback, factory, _CeleryAsyncRuntime())
        task = celery_app.tasks[f"{TASK_CALLBACK_ERROR_PREFIX}.{callback.__qualname__}"]

        task.apply(kwargs={"job_id": "acme-job", "context": {}}).get()

        assert callback_log == [("built", NO_CALLER), ("on_failure", NO_CALLER)]

    def test_a_constructor_caller_is_refused_as_a_callback_has_none(
        self, succeed: Callable[[type[Any]], Any]
    ) -> None:
        result = succeed(AcmeCallerCallback)

        with pytest.raises(Unauthenticated, match="job callback"):
            result.get()
