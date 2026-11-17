"""An in-process Celery worker for jobs, leaving the process's Celery state as it found it."""

from __future__ import annotations

import logging
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

WORKER_SIGNALS = (
    signals.worker_init,
    signals.worker_ready,
    signals.worker_process_init,
    signals.worker_shutting_down,
    signals.worker_process_shutdown,
    signals.worker_shutdown,
)
QUEUE = "jobs"
SETTLE_TIMEOUT_S = 10.0
SETTLE_GRACE_S = 0.5


class Workbench:
    """A Celery app with a memory broker, serving the jobs and callbacks registered on it."""

    def __init__(self, serializer: str, runtime: _CeleryAsyncRuntime) -> None:
        self.app = Celery(broker="memory://", backend="cache+memory://")
        self.app.conf.update(
            task_default_queue=QUEUE,
            worker_hijack_root_logger=False,
            result_serializer=serializer,
            accept_content=["json", "pickle"],
        )
        self.kernel = JobKernel()
        self.task_id = str(uuid.uuid4())
        self._runtime = runtime

    def job_task(self, job_type: type[Job[Any]], *, max_attempts: int | None = None) -> Any:
        """Register *job_type*'s task; *max_attempts* makes its first failure final when 1."""
        self.kernel.factory.register(job_type)
        settings = JobSettings()
        if max_attempts is not None:
            config = JobConfig(retry=RetryConfig(max_attempts=max_attempts))
            settings = JobSettings({job_type: JobOptions.resolve(job_type, config)})
        return _make_job_task(
            self.app,
            job_type,
            self.kernel.factory,
            self.kernel.executor,
            self._runtime,
            settings=settings,
        )

    def failure_callback_task(self, callback_type: type[Any]) -> Any:
        """Register the task running *callback_type*'s ``on_failure``."""
        self.kernel.factory.register(callback_type)
        return _make_callback_task(
            self.app,
            callback_type,
            CallbackOutcome.FAILURE,
            self.kernel.factory,
            self._runtime,
        )

    @contextmanager
    def worker(self) -> Generator[AsyncResult]:
        """Run a solo-pool worker; yield the stored result of ``task_id``, settled on exit."""
        stored = AsyncResult(self.task_id, app=self.app)
        with start_worker(
            self.app, pool="solo", perform_ping_check=False, loglevel="WARNING", queues=[QUEUE]
        ):
            yield stored
            deadline = time.monotonic() + SETTLE_TIMEOUT_S
            while not stored.ready() and time.monotonic() < deadline:
                time.sleep(0.05)
            time.sleep(SETTLE_GRACE_S)


@pytest.fixture
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
