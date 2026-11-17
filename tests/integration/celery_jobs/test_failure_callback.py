"""``on_failure`` learns the class and the clean message of the error whatever the serializer."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, ClassVar

import pytest

from loom.celery.service import _CallbackLink
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


def post_commit() -> Exception:
    return PostCommitError(committed=True, failures=(ConnectionError("broker down"),))


def job_timeout() -> Exception:
    return JobTimeoutError("job-1", 2.0)


def rule_violations() -> Exception:
    return RuleViolations([RuleViolation("name", "required")])


class TestFailureCallbackAfterAWorkerStoredTheError:
    @pytest.mark.parametrize(
        ("serializer", "make_error"),
        [
            pytest.param("json", post_commit, id="post-commit-json"),
            pytest.param("pickle", post_commit, id="post-commit-pickle"),
            pytest.param("json", job_timeout, id="job-timeout-json"),
            pytest.param("json", rule_violations, id="rule-violations-json"),
        ],
    )
    def test_gets_the_error_class_and_message(
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
        link = _CallbackLink(job_id=bench.task_id, context={}, trace_id=None, identity=None)
        signature = link.signature(bench.app, RecordingCallback, CallbackOutcome.FAILURE)

        with bench.worker():
            task.apply_async(task_id=bench.task_id, link_error=signature)

        error = RaisingJob.error
        assert RecordingCallback.seen == [
            {"exc_type": type(error).__qualname__, "exc_msg": str(error)}
        ]
