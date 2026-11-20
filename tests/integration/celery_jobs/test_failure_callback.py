"""Every terminal failure of a job task sends its failure callback once, with the real error."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.core.errors import (
    JobTimeoutError,
    PostCommitError,
    RuleViolation,
    RuleViolations,
)
from tests.integration.celery_jobs.conftest import JSON, Workbench
from tests.integration.celery_jobs.doubles import (
    ABANDONMENT,
    FAILURE_CALLBACK,
    SOFT_LIMIT,
    AbandonedJob,
    AcmeKeywordError,
    BridgeDownJob,
    FlakyJob,
    IgnoringTaskJob,
    NotOpenJob,
    RaisingJob,
    RejectingTaskJob,
    SoftLimitJob,
    SucceedingJob,
)


class TestFailureCallbackAcrossSerializers:
    @pytest.mark.parametrize("served", ["json", "pickle"], indirect=True)
    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(
                PostCommitError(committed=True, failures=(ConnectionError("broker down"),)),
                id="post-commit",
            ),
            pytest.param(JobTimeoutError("job-1", 2.0), id="job-timeout"),
            pytest.param(RuleViolations([RuleViolation("name", "required")]), id="rule-violations"),
            pytest.param(AcmeKeywordError("acme", reason="deleted"), id="keyword-error"),
        ],
    )
    def test_an_abandoned_job_notifies_once_with_the_error_class_and_message(
        self, served: Workbench, error: Exception
    ) -> None:
        RaisingJob.error = error

        task_id = served.send(served.tasks[RaisingJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        assert stored.state == "FAILURE"
        assert [(call["exc_type"], call["exc_msg"]) for call in calls] == [
            (type(error).__qualname__, str(error))
        ]


@JSON
class TestFailureCallbackFromTheWorker:
    def test_a_job_abandonment_notifies_once_with_its_reason(self, served: Workbench) -> None:
        task_id = served.send(served.tasks[AbandonedJob], failure_callback=FAILURE_CALLBACK)
        _, calls = served.settle(task_id, notified=1)

        assert [(call["exc_type"], call["exc_msg"]) for call in calls] == [
            ("JobAbandonment", ABANDONMENT)
        ]

    def test_a_worker_that_could_not_open_its_application_notifies_once(
        self, served: Workbench
    ) -> None:
        task_id = served.send(served.tasks[NotOpenJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        assert stored.state == "FAILURE"
        assert [call["exc_type"] for call in calls] == ["WorkerNotOpenError"]
        assert "ConnectionError: database down" in calls[0]["exc_msg"]

    def test_a_soft_time_limit_notifies_once(self, served: Workbench) -> None:
        task_id = served.send(served.tasks[SoftLimitJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        assert stored.state == "FAILURE"
        assert [(call["exc_type"], call["exc_msg"]) for call in calls] == [
            ("SoftTimeLimitExceeded", str(SOFT_LIMIT))
        ]

    def test_a_bridge_error_notifies_once_with_the_bridge_message(self, served: Workbench) -> None:
        task_id = served.send(served.tasks[BridgeDownJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        assert stored.state == "FAILURE"
        assert [call["exc_type"] for call in calls] == ["RuntimeError"]
        assert "async bridge is not initialized" in calls[0]["exc_msg"]

    @pytest.mark.parametrize("job", [RejectingTaskJob, IgnoringTaskJob])
    def test_a_task_rejected_or_ignored_by_celery_notifies_no_one(
        self, served: Workbench, job: type[RejectingTaskJob | IgnoringTaskJob]
    ) -> None:
        task_id = served.send(served.tasks[job], failure_callback=FAILURE_CALLBACK)
        sentinel = served.send(served.tasks[SucceedingJob])
        served.settle(sentinel, notified=0)

        assert served.calls(task_id) == []

    def test_a_failure_that_is_retried_notifies_no_one(self, served: Workbench) -> None:
        FlakyJob.attempts = 0

        task_id = served.send(served.tasks[FlakyJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=0)

        assert (stored.state, stored.result, FlakyJob.attempts) == ("SUCCESS", "recovered", 2)
        assert calls == []

    def test_a_job_that_succeeds_notifies_no_failure(self, served: Workbench) -> None:
        task_id = served.send(served.tasks[SucceedingJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=0)

        assert (stored.state, stored.result) == ("SUCCESS", "done")
        assert calls == []

    def test_a_job_without_a_failure_callback_notifies_no_one(self, served: Workbench) -> None:
        task_id = served.send(served.tasks[AbandonedJob])
        stored, calls = served.settle(task_id, notified=0)

        assert stored.state == "FAILURE"
        assert calls == []


@JSON
class TestBrokenFailureCallbackSend:
    def test_the_job_stores_its_own_error_and_the_send_failure_is_logged(
        self,
        served: Workbench,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        send_task = served.app.send_task

        def refuse_the_callback(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == FAILURE_CALLBACK:
                raise ConnectionError("broker down")
            return send_task(name, *args, **kwargs)

        monkeypatch.setattr(served.app, "send_task", refuse_the_callback)
        caplog.set_level(logging.ERROR)

        task_id = served.send(served.tasks[AbandonedJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=0)

        assert calls == []
        assert (stored.state, type(stored.result).__name__) == ("FAILURE", "JobAbandonment")
        assert [
            record.msg["event"]
            for record in caplog.records
            if isinstance(record.msg, dict) and "not sent" in record.msg.get("event", "")
        ] == ["Failure callback not sent"]
