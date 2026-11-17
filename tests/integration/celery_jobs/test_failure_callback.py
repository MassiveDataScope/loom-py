"""``on_failure`` is called once, with the error's class and clean message, on any serializer."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar

import pytest

from loom.celery.constants import callback_task_name
from loom.core.errors import (
    JobTimeoutError,
    PostCommitError,
    RuleViolation,
    RuleViolations,
)
from loom.core.job.callback import CallbackOutcome, JobCallback
from loom.core.job.job import Job
from tests.integration.celery_jobs.conftest import Workbench


class RaisingJob(Job[None]):
    error: ClassVar[Exception]

    async def execute(self) -> None:
        raise self.error


class RecordingCallback(JobCallback):
    seen: ClassVar[list[dict[str, Any]]] = []

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        self.seen.append({"exc_type": exc_type, "exc_msg": exc_msg})


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


class TestFailureCallbackFromTheWorkerThatRejects:
    @pytest.mark.parametrize(
        ("serializer", "make_error"),
        [
            pytest.param("json", post_commit, id="post-commit-json"),
            pytest.param("pickle", post_commit, id="post-commit-pickle"),
            pytest.param("json", job_timeout, id="job-timeout-json"),
            pytest.param("json", rule_violations, id="rule-violations-json"),
            pytest.param("json", keyword_error, id="keyword-error-json"),
        ],
    )
    def test_is_called_once_with_the_error_class_and_message(
        self,
        workbench: Callable[[str], Workbench],
        serializer: str,
        make_error: Callable[[], Exception],
    ) -> None:
        bench = workbench(serializer)
        RaisingJob.error = make_error()
        RecordingCallback.seen.clear()
        task = bench.job_task(RaisingJob, max_attempts=1)
        bench.failure_callback_task(RecordingCallback)
        name = callback_task_name(RecordingCallback, CallbackOutcome.FAILURE)

        with bench.worker():
            task.apply_async(task_id=bench.task_id, kwargs={"failure_callback": name})

        error = RaisingJob.error
        assert RecordingCallback.seen == [
            {"exc_type": type(error).__qualname__, "exc_msg": str(error)}
        ]
