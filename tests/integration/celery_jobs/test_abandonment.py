"""A job that will not run again is stored as failed with its own error, acked and not retried."""

from __future__ import annotations

from collections.abc import Callable

import pytest
from celery import signals  # type: ignore[import-untyped]
from celery.worker.request import Request  # type: ignore[import-untyped]

from loom.core.job.envelope import wire_job_name
from loom.core.job.job import Job
from loom.core.job.outcome import JobAbandonment
from tests.helpers.celery import CeleryBroker
from tests.integration.celery_jobs.conftest import JSON, Workbench
from tests.integration.celery_jobs.doubles import ABANDONMENT, FAILURE_CALLBACK, AbandonedJob


class TestEagerAbandonment:
    def test_the_result_raises_the_jobs_own_error_after_sending_the_failure_callback(
        self, workbench: Callable[[str], Workbench], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        bench = workbench("json")
        task: Job[None] = bench.job_task(AbandonedJob)
        broker = CeleryBroker([bench.queue], bench.app)
        monkeypatch.setitem(bench.app.conf, "task_always_eager", True)

        result = task.apply(kwargs={"failure_callback": "on-error"})  # type: ignore[attr-defined]

        assert result.failed()
        with pytest.raises(JobAbandonment, match=ABANDONMENT):
            result.get()
        (message,) = broker.sent()
        assert (message.task, message.kwargs["exc_type"]) == ("on-error", "JobAbandonment")


@JSON
class TestWorkerAbandonment:
    def test_is_stored_once_with_its_error_and_acked_not_requeued(
        self, served: Workbench, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        AbandonedJob.runs.clear()
        failures: list[str] = []
        settlements: list[tuple[str, ...]] = []
        job_name = wire_job_name(AbandonedJob.__qualname__)

        def on_failure(exception: BaseException | None = None, **_: object) -> None:
            failures.append(type(exception).__name__)

        acknowledge, reject = Request.acknowledge, Request.reject

        def acknowledge_spy(request: Request) -> None:
            if not request.acknowledged and request.name == job_name:
                settlements.append(("ack",))
            acknowledge(request)

        def reject_spy(request: Request, requeue: bool = False) -> None:
            settlements.append(("reject", str(requeue)))
            reject(request, requeue)

        monkeypatch.setattr(Request, "acknowledge", acknowledge_spy)
        monkeypatch.setattr(Request, "reject", reject_spy)
        signals.task_failure.connect(on_failure, weak=False)
        try:
            task_id = served.send(served.tasks[AbandonedJob], failure_callback=FAILURE_CALLBACK)
            stored, calls = served.settle(task_id, notified=1)
        finally:
            signals.task_failure.disconnect(on_failure)

        assert (stored.state, type(stored.result)) == ("FAILURE", JobAbandonment)
        assert [call["hook"] for call in calls] == ["on_failure"]
        assert failures.count("JobAbandonment") == 1
        assert settlements == [("ack",)]
        assert AbandonedJob.runs == ["ran"]
