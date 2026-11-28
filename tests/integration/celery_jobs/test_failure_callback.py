"""Every terminal failure of a job task sends its failure callback once, with its type and code."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.celery.event import CeleryEvent
from loom.core.errors import (
    JobTimeoutError,
    PostCommitError,
    RuleViolationError,
    RuleViolationsError,
)
from loom.core.errors.codes import ErrorCode
from tests.helpers.loggers import CapturedLog
from tests.integration.celery_jobs.conftest import JSON, Workbench
from tests.integration.celery_jobs.doubles import (
    FAILURE_CALLBACK,
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
        ("error", "code"),
        [
            pytest.param(
                PostCommitError(committed=True, failures=(ConnectionError("broker down"),)),
                ErrorCode.POST_COMMIT_FAILURE,
                id="post-commit",
            ),
            pytest.param(JobTimeoutError("job-1", 2.0), ErrorCode.JOB_TIMEOUT, id="job-timeout"),
            pytest.param(
                RuleViolationsError([RuleViolationError("name", "required")]),
                ErrorCode.RULE_VIOLATIONS,
                id="rule-violations",
            ),
            pytest.param(AcmeKeywordError("acme", reason="deleted"), None, id="keyword-error"),
        ],
    )
    def test_an_abandoned_job_notifies_once_with_the_error_class_and_code(
        self, served: Workbench, error: Exception, code: ErrorCode | None
    ) -> None:
        RaisingJob.error = error

        task_id = served.send(served.tasks[RaisingJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        assert stored.state == "FAILURE"
        assert [(call["exc_type"], call["exc_code"]) for call in calls] == [
            (f"{type(error).__module__}.{type(error).__qualname__}", code)
        ]


@JSON
class TestFailureCallbackFromTheWorker:
    def test_a_job_abandonment_notifies_once_with_its_type_and_no_code(
        self, served: Workbench
    ) -> None:
        task_id = served.send(served.tasks[AbandonedJob], failure_callback=FAILURE_CALLBACK)
        _, calls = served.settle(task_id, notified=1)

        assert [(call["exc_type"], call["exc_code"]) for call in calls] == [
            ("loom.core.job.outcome.JobAbandonment", None)
        ]

    def test_a_worker_that_could_not_open_its_application_notifies_once(
        self, served: Workbench
    ) -> None:
        task_id = served.send(served.tasks[NotOpenJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        assert stored.state == "FAILURE"
        assert [(call["exc_type"], call["exc_code"]) for call in calls] == [
            ("loom.celery.errors.WorkerNotOpenError", ErrorCode.SYSTEM_ERROR)
        ]

    def test_a_soft_time_limit_notifies_once(self, served: Workbench) -> None:
        task_id = served.send(served.tasks[SoftLimitJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        assert stored.state == "FAILURE"
        assert [(call["exc_type"], call["exc_code"]) for call in calls] == [
            ("billiard.exceptions.SoftTimeLimitExceeded", None)
        ]

    def test_a_bridge_error_notifies_once_with_its_type_and_no_code(
        self, served: Workbench
    ) -> None:
        task_id = served.send(served.tasks[BridgeDownJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        assert stored.state == "FAILURE"
        assert [(call["exc_type"], call["exc_code"]) for call in calls] == [
            ("loom.core.errors.base.InvalidUse", None)
        ]

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
        assert len(CapturedLog(caplog).named(CeleryEvent.FAILURE_CALLBACK_NOT_SENT)) == 1
