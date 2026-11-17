"""An in-process Celery worker for jobs, leaving the process's Celery state as it found it."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable, Generator
from contextlib import ExitStack, contextmanager
from typing import Any

import pytest
from celery import Celery, _state, signals  # type: ignore[import-untyped]
from celery.contrib.testing.worker import start_worker  # type: ignore[import-untyped]
from celery.result import AsyncResult  # type: ignore[import-untyped]

from loom.celery.runner import _CeleryAsyncRuntime, _make_callback_task, _make_job_task
from loom.core.job.callback import CallbackOutcome
from loom.core.job.config import JobConfig, JobOptions, JobSettings, RetryConfig
from loom.core.job.job import Job
from tests.helpers.jobs import JobKernel
from tests.integration.celery_jobs.doubles import (
    QUEUE,
    FlakyJob,
    NotOpenJob,
    RaisingJob,
    RecordingCallback,
    RejectedJob,
    SoftLimitJob,
    SoftLimitRuntime,
    SucceedingJob,
)

WORKER_SIGNALS = (
    signals.worker_init,
    signals.worker_ready,
    signals.worker_process_init,
    signals.worker_shutting_down,
    signals.worker_process_shutdown,
    signals.worker_shutdown,
)
SETTLE_TIMEOUT_S = 10.0
POLL_S = 0.02
BROKER_POLL_S = 0.05
WAKE_DELAY_S = 0.05
REPEAT_GRACE_S = 0.2


class Workbench:
    """A Celery app with a memory broker, serving the jobs and callbacks registered on it.

    Tasks are registered before :meth:`worker` starts, which one worker serves for
    every case; each case sends its own task id and reads what was recorded under it.
    """

    def __init__(self, serializer: str, runtime: _CeleryAsyncRuntime) -> None:
        self.queue = f"{QUEUE}-{serializer}"
        self.app = Celery(broker="memory://", backend="cache+memory://")
        self.app.conf.update(
            task_default_queue=self.queue,
            broker_transport_options={"polling_interval": BROKER_POLL_S},
            worker_hijack_root_logger=False,
            result_serializer=serializer,
            accept_content=["json", "pickle"],
        )
        self.kernel = JobKernel()
        self.tasks: dict[type[Job[Any]], Any] = {}
        self._runtime = runtime

    def job_task(
        self,
        job_type: type[Job[Any]],
        *,
        max_attempts: int | None = None,
        runtime: _CeleryAsyncRuntime | None = None,
    ) -> Any:
        """Register *job_type*'s task; *max_attempts* makes its first failure final when 1."""
        self.kernel.factory.register(job_type)
        settings = JobSettings()
        if max_attempts is not None:
            config = JobConfig(retry=RetryConfig(max_attempts=max_attempts))
            settings = JobSettings({job_type: JobOptions.resolve(job_type, config)})
        task = _make_job_task(
            self.app,
            job_type,
            self.kernel.factory,
            self.kernel.executor,
            runtime or self._runtime,
            settings=settings,
        )
        self.tasks[job_type] = task
        return task

    def callback_tasks(self, callback_type: type[Any]) -> None:
        """Register the tasks running *callback_type*'s ``on_success`` and ``on_failure``."""
        self.kernel.factory.register(callback_type)
        for outcome in CallbackOutcome:
            _make_callback_task(
                self.app, callback_type, outcome, self.kernel.factory, self._runtime
            )

    @contextmanager
    def worker(self) -> Generator[None]:
        """Run a solo-pool worker for everything registered so far.

        The worker looks at its shutdown flag only when its loop wakes, every two seconds
        while idle; a message sent as the block ends wakes it at once.
        """
        with start_worker(
            self.app,
            pool="solo",
            perform_ping_check=False,
            loglevel="CRITICAL",
            queues=[self.queue],
        ):
            try:
                yield
            finally:
                threading.Timer(WAKE_DELAY_S, lambda: self.app.send_task("celery.ping")).start()

    def send(self, task: Any, **kwargs: Any) -> str:
        """Send *task* under a task id of its own, with *kwargs*; return the id."""
        task_id = str(uuid.uuid4())
        task.apply_async(task_id=task_id, kwargs=kwargs)
        return task_id

    def settle(self, task_id: str, *, notified: int) -> tuple[AsyncResult, list[dict[str, Any]]]:
        """Wait for the job *task_id* to end and for *notified* callbacks, then for repeats.

        Returns:
            The stored result and every callback call recorded for the job.
        """
        stored = AsyncResult(task_id, app=self.app)
        self._wait_until(stored.ready)
        self._wait_until(lambda: len(self._seen(task_id)) >= notified)
        time.sleep(REPEAT_GRACE_S)
        return stored, self._seen(task_id)

    @staticmethod
    def _seen(task_id: str) -> list[dict[str, Any]]:
        return [call for call in RecordingCallback.seen if call["job_id"] == task_id]

    @staticmethod
    def _wait_until(condition: Callable[[], bool]) -> None:
        deadline = time.monotonic() + SETTLE_TIMEOUT_S
        while not condition() and time.monotonic() < deadline:
            time.sleep(POLL_S)


@pytest.fixture(scope="package")
def workbench() -> Generator[Callable[[str], Workbench]]:
    """Build workbenches on a worker no other app's lifecycle handlers reach.

    The handlers other tests connected to the worker signals are set aside and
    put back, with the current and default apps and the loggers, on teardown.
    """
    root = logging.getLogger()
    task_logger = logging.getLogger("celery.task")
    root_level = root.level
    root_handlers = list(root.handlers)
    task_handlers = list(task_logger.handlers)
    previous_default = _state.default_app
    previous_current = getattr(_state._tls, "current_app", None)
    connected = [list(signal.receivers) for signal in WORKER_SIGNALS]
    for signal in WORKER_SIGNALS:
        signal.receivers[:] = []
        signal.sender_receivers_cache.clear()
    with ExitStack() as cleanup:

        def build(serializer: str = "json") -> Workbench:
            runtime = _CeleryAsyncRuntime()
            runtime.initialize()
            cleanup.callback(runtime.shutdown)
            return Workbench(serializer, runtime)

        try:
            yield build
        finally:
            cleanup.close()
            _state.default_app = previous_default
            _state._tls.current_app = previous_current
            root.setLevel(root_level)
            root.handlers[:] = root_handlers
            task_logger.handlers[:] = task_handlers
            for signal, receivers in zip(WORKER_SIGNALS, connected, strict=True):
                signal.receivers[:] = receivers
                signal.sender_receivers_cache.clear()


@contextmanager
def _serving_the_doubles(bench: Workbench) -> Generator[Workbench]:
    unopened = _CeleryAsyncRuntime()
    unopened.refuse(ConnectionError("database down"))
    bench.job_task(RaisingJob, max_attempts=1)
    bench.job_task(RejectedJob)
    bench.job_task(SucceedingJob)
    bench.job_task(FlakyJob)
    bench.job_task(NotOpenJob, runtime=unopened)
    bench.job_task(SoftLimitJob, runtime=SoftLimitRuntime())
    bench.callback_tasks(RecordingCallback)
    with bench.worker():
        yield bench


@pytest.fixture(scope="package")
def served(
    request: pytest.FixtureRequest, workbench: Callable[[str], Workbench]
) -> Generator[Workbench]:
    """The worker of the folder for a result serializer, serving every job of ``doubles``.

    A test names the serializer with ``parametrize("served", [...], indirect=True)``; the
    workers are never up together, as they would share one in-memory broker.
    """
    with _serving_the_doubles(workbench(request.param)) as bench:
        yield bench


JSON = pytest.mark.parametrize("served", ["json"], indirect=True)
"""For a test that does not depend on the result serializer."""
