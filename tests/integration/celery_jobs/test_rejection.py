"""A job that will not run again is stored as failed with its own error, acked and not retried."""

from __future__ import annotations

import time
from collections.abc import Generator
from typing import Any

import pytest
from celery import Celery, current_app, signals  # type: ignore[import-untyped]
from celery.contrib.testing.worker import start_worker  # type: ignore[import-untyped]
from celery.result import AsyncResult  # type: ignore[import-untyped]
from celery.worker.request import Request  # type: ignore[import-untyped]

from loom.celery.runner import _CeleryAsyncRuntime, _make_job_task
from loom.core.job.config import JobSettings
from loom.core.job.job import Job
from loom.core.job.outcome import JobRejection
from tests.contract.jobs.conftest import JobKernel

QUEUE = "rejections"
SETTLE_TIMEOUT_S = 10.0


class RejectedJob(Job[None]):
    runs: list[str] = []

    async def execute(self) -> None:
        self.runs.append("ran")
        raise JobRejection("tenant deleted")


@pytest.fixture
def job_app() -> Generator[tuple[Celery, Any]]:
    previous = current_app._get_current_object()
    app = Celery(broker="memory://", backend="cache+memory://")
    app.conf.update(task_default_queue=QUEUE, worker_hijack_root_logger=False)
    kernel = JobKernel()
    kernel.factory.register(RejectedJob)
    runtime = _CeleryAsyncRuntime()
    runtime.initialize()
    task = _make_job_task(
        app, RejectedJob, kernel.factory, kernel.executor, runtime, settings=JobSettings()
    )
    RejectedJob.runs.clear()
    yield app, task
    runtime.shutdown()
    previous.set_current()
    previous.set_default()


class TestEagerRejection:
    def test_the_result_raises_the_jobs_own_error(self, job_app: tuple[Celery, Any]) -> None:
        app, task = job_app
        errbacks: list[str] = []

        @app.task(name="on-error", shared=False)
        def on_error(*_: Any) -> None:
            errbacks.append("fired")

        app.conf.task_always_eager = True

        result = task.apply(link_error=on_error.si())

        assert result.failed()
        with pytest.raises(JobRejection, match="tenant deleted"):
            result.get()
        assert errbacks == ["fired"]


class TestWorkerRejection:
    def test_is_stored_once_with_its_error_and_acked_not_requeued(
        self, job_app: tuple[Celery, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app, task = job_app
        errbacks: list[str] = []
        failures: list[str] = []
        settlements: list[tuple[str, ...]] = []

        @app.task(name="on-error", shared=False)
        def on_error(*_: Any) -> None:
            errbacks.append("fired")

        def on_failure(exception: BaseException | None = None, **_: Any) -> None:
            failures.append(type(exception).__name__)

        acknowledge, reject = Request.acknowledge, Request.reject

        def acknowledge_spy(request: Request) -> None:
            if not request.acknowledged:
                settlements.append(("ack",))
            acknowledge(request)

        def reject_spy(request: Request, requeue: bool = False) -> None:
            settlements.append(("reject", str(requeue)))
            reject(request, requeue)

        monkeypatch.setattr(Request, "acknowledge", acknowledge_spy)
        monkeypatch.setattr(Request, "reject", reject_spy)
        signals.task_failure.connect(on_failure, weak=False)
        try:
            with start_worker(
                app, pool="solo", perform_ping_check=False, loglevel="WARNING", queues=[QUEUE]
            ):
                task.apply_async(task_id="job-1", link_error=on_error.si())
                stored = AsyncResult("job-1", app=app)
                deadline = time.monotonic() + SETTLE_TIMEOUT_S
                while not stored.ready() and time.monotonic() < deadline:
                    time.sleep(0.05)
                time.sleep(0.5)
        finally:
            signals.task_failure.disconnect(on_failure)

        assert (stored.state, type(stored.result)) == ("FAILURE", JobRejection)
        assert errbacks == ["fired"]
        assert failures == ["JobRejection"]
        assert settlements == [("ack",)]
        assert RejectedJob.runs == ["ran"]
