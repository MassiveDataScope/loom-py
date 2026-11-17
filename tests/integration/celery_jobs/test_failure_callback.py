"""Every terminal failure of a job task sends its failure callback once, with the real error."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pytest

from loom.celery.constants import callback_task_name
from loom.core.errors import (
    JobTimeoutError,
    PostCommitError,
    RuleViolation,
    RuleViolations,
)
from loom.core.job.callback import CallbackOutcome
from tests.integration.celery_jobs.conftest import JSON, Workbench
from tests.integration.celery_jobs.doubles import (
    REJECTION,
    FlakyJob,
    NotOpenJob,
    RaisingJob,
    RecordingCallback,
    RejectedJob,
    SoftLimitJob,
    SucceedingJob,
)

FAILURE_CALLBACK = callback_task_name(RecordingCallback, CallbackOutcome.FAILURE)


class AcmeKeywordError(Exception):
    """An error its own class cannot rebuild from its arguments."""

    def __init__(self, tenant: str, *, reason: str) -> None:
        super().__init__(f"{tenant}: {reason}")


def keyword_error() -> Exception:
    return AcmeKeywordError("acme", reason="deleted")


def post_commit() -> Exception:
    return PostCommitError(committed=True, failures=(ConnectionError("broker down"),))


def job_timeout() -> Exception:
    return JobTimeoutError("job-1", 2.0)


def rule_violations() -> Exception:
    return RuleViolations([RuleViolation("name", "required")])


class TestFailureCallbackAcrossSerializers:
    @pytest.mark.parametrize("served", ["json", "pickle"], indirect=True)
    @pytest.mark.parametrize(
        "make_error",
        [post_commit, job_timeout, rule_violations, keyword_error],
        ids=["post-commit", "job-timeout", "rule-violations", "keyword-error"],
    )
    def test_a_rejected_job_notifies_once_with_the_error_class_and_message(
        self, served: Workbench, make_error: Callable[[], Exception]
    ) -> None:
        RaisingJob.error = make_error()

        task_id = served.send(served.tasks[RaisingJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=1)

        error = RaisingJob.error
        assert stored.state == "FAILURE"
        assert [(call["exc_type"], call["exc_msg"]) for call in calls] == [
            (type(error).__qualname__, str(error))
        ]


@JSON
class TestFailureCallbackFromTheWorker:
    def test_a_job_rejection_notifies_once_with_its_reason(self, served: Workbench) -> None:
        task_id = served.send(served.tasks[RejectedJob], failure_callback=FAILURE_CALLBACK)
        _, calls = served.settle(task_id, notified=1)

        assert [(call["exc_type"], call["exc_msg"]) for call in calls] == [
            ("JobRejection", REJECTION)
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
        assert [call["exc_type"] for call in calls] == ["SoftTimeLimitExceeded"]

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
        task_id = served.send(served.tasks[RejectedJob])
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

        task_id = served.send(served.tasks[RejectedJob], failure_callback=FAILURE_CALLBACK)
        stored, calls = served.settle(task_id, notified=0)

        assert calls == []
        assert (stored.state, type(stored.result).__name__) == ("FAILURE", "JobRejection")
        assert [
            record.msg["event"]
            for record in caplog.records
            if isinstance(record.msg, dict) and "not sent" in record.msg.get("event", "")
        ] == ["Failure callback not sent"]
