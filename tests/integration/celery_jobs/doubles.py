"""The jobs, callbacks and runtimes the shared Celery worker of this folder serves."""

from __future__ import annotations

import threading
from typing import Any, ClassVar

from billiard.exceptions import SoftTimeLimitExceeded  # type: ignore[import-untyped]

from loom.celery.constants import callback_task_name
from loom.celery.runner import _CeleryAsyncRuntime
from loom.core.job.callback import CallbackOutcome, JobCallback
from loom.core.job.job import Job
from loom.core.job.outcome import JobRejection
from loom.core.job.policy import Backoff

QUEUE = "jobs"
REJECTION = "tenant deleted"


class RaisingJob(Job[None]):
    """Raises ``error`` on its only attempt."""

    error: ClassVar[Exception]

    async def execute(self) -> None:
        raise self.error


class RejectedJob(Job[None]):
    runs: ClassVar[list[str]] = []

    async def execute(self) -> None:
        self.runs.append("ran")
        raise JobRejection(REJECTION)


class SucceedingJob(Job[str]):
    async def execute(self) -> str:
        return "done"


class FlakyJob(Job[str]):
    """Fails its first attempt and is retried at once; the retry succeeds."""

    __retry__ = Backoff(first_s=0, factor=1, max_s=0, jitter=False)
    attempts: ClassVar[int] = 0

    async def execute(self) -> str:
        type(self).attempts += 1
        if type(self).attempts == 1:
            raise ValueError("first attempt")
        return "recovered"


class NotOpenJob(Job[None]):
    """Runs on a worker process whose application did not open."""

    async def execute(self) -> None:
        return None


class SoftLimitJob(Job[None]):
    """Hits its soft time limit outside the runner, in the worker's bridge."""

    async def execute(self) -> None:
        return None


class BridgeDownJob(Job[None]):
    """Runs on a worker whose async bridge was never initialised."""

    async def execute(self) -> None:
        return None


class RejectingTaskJob(Job[None]):
    """Its task is rejected by Celery from outside the runner."""

    async def execute(self) -> None:
        return None


class IgnoringTaskJob(Job[None]):
    """Its task is ignored by Celery from outside the runner."""

    async def execute(self) -> None:
        return None


class AcmeKeywordError(Exception):
    """An error its own class cannot rebuild from its arguments."""

    def __init__(self, tenant: str, *, reason: str) -> None:
        super().__init__(f"{tenant}: {reason}")


SOFT_LIMIT = SoftTimeLimitExceeded(30)


class FailingRuntime(_CeleryAsyncRuntime):
    """A runtime whose bridge raises *error*, as Celery's own machinery would from outside."""

    def __init__(self, error: BaseException) -> None:
        super().__init__()
        self.error = error

    def run(self, coro: Any, *, timeout: float | None = None, eager_fallback: bool) -> Any:
        coro.close()
        raise self.error


class RecordingCallback(JobCallback):
    """Records each hook it is called with, under the job it was called for."""

    seen: ClassVar[list[dict[str, Any]]] = []
    lock: ClassVar[threading.Lock] = threading.Lock()

    def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        with self.lock:
            self.seen.append({"job_id": job_id, "hook": "on_success", "result": result})

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        with self.lock:
            self.seen.append(
                {"job_id": job_id, "hook": "on_failure", "exc_type": exc_type, "exc_msg": exc_msg}
            )


FAILURE_CALLBACK = callback_task_name(RecordingCallback, CallbackOutcome.FAILURE)
