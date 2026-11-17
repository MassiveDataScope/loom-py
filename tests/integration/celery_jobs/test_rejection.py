"""A job that will not run again is stored as failed with its own error, acked and not retried."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest
from celery import signals  # type: ignore[import-untyped]
from celery.worker.request import Request  # type: ignore[import-untyped]

from loom.core.job.envelope import wire_job_name
from loom.core.job.job import Job
from loom.core.job.outcome import JobRejection
from tests.integration.celery_jobs.conftest import Workbench


class RejectedJob(Job[None]):
    runs: list[str] = []

    async def execute(self) -> None:
        self.runs.append("ran")
        raise JobRejection("tenant deleted")


class TestEagerRejection:
    def test_the_result_raises_the_jobs_own_error_after_sending_the_failure_callback(
        self, workbench: Callable[[str], Workbench], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        bench = workbench("json")
        app, task = bench.app, bench.job_task(RejectedJob)
        sent = MagicMock()
        monkeypatch.setattr(app, "send_task", sent)
        app.conf.task_always_eager = True

        result = task.apply(kwargs={"failure_callback": "on-error"})

        assert result.failed()
        with pytest.raises(JobRejection, match="tenant deleted"):
            result.get()
        sent.assert_called_once()
        assert sent.call_args.args == ("on-error",)
        assert sent.call_args.kwargs["kwargs"]["exc_type"] == "JobRejection"


class TestWorkerRejection:
    def test_is_stored_once_with_its_error_and_acked_not_requeued(
        self, workbench: Callable[[str], Workbench], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        bench = workbench("json")
        app, task = bench.app, bench.job_task(RejectedJob)
        RejectedJob.runs.clear()
        errbacks: list[str] = []
        failures: list[str] = []
        settlements: list[tuple[str, ...]] = []

        @app.task(name="on-error", shared=False)
        def on_error(**kwargs: Any) -> None:
            errbacks.append(kwargs["exc_type"])

        def on_failure(exception: BaseException | None = None, **_: Any) -> None:
            failures.append(type(exception).__name__)

        acknowledge, reject = Request.acknowledge, Request.reject

        def acknowledge_spy(request: Request) -> None:
            if not request.acknowledged and request.name == wire_job_name(RejectedJob.__qualname__):
                settlements.append(("ack",))
            acknowledge(request)

        def reject_spy(request: Request, requeue: bool = False) -> None:
            settlements.append(("reject", str(requeue)))
            reject(request, requeue)

        monkeypatch.setattr(Request, "acknowledge", acknowledge_spy)
        monkeypatch.setattr(Request, "reject", reject_spy)
        signals.task_failure.connect(on_failure, weak=False)
        try:
            with bench.worker() as stored:
                task.apply_async(task_id=bench.task_id, kwargs={"failure_callback": "on-error"})
        finally:
            signals.task_failure.disconnect(on_failure)

        assert (stored.state, type(stored.result)) == ("FAILURE", JobRejection)
        assert errbacks == ["JobRejection"]
        assert failures == ["JobRejection"]
        assert settlements == [("ack",)]
        assert RejectedJob.runs == ["ran"]
