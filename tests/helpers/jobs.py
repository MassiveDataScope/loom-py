"""Jobs and sources the job service and job routing tests share."""

from __future__ import annotations

import threading
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, cast

from loom.celery.runner import _CeleryProcessBridge, _make_callback_task, _make_job_task
from loom.celery.service import CeleryJobTransport
from loom.composition import AppConfig, Application, Project, Recipe
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.discovery.base import DiscoveryResult
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind, ExecutionEvent
from loom.core.engine.execution_observer import ExecutionObserver
from loom.core.engine.executor import UseCaseExecutor
from loom.core.job.callback import ANY_CALLBACKS, CallbackOutcome, DispatchableCallbacks
from loom.core.job.config import JobSettings
from loom.core.job.delivery import JobDelivery
from loom.core.job.job import Job
from loom.core.job.run import EnvelopeInvocation
from loom.core.job.service import JobService
from loom.core.job.timer import JobTimer
from loom.core.observability.observability import Observability
from loom.core.stores.profile import WORKER_PROFILE
from loom.core.stores.repository.registration import classify_key
from loom.core.use_case.factory import UseCaseFactory, main_repository
from loom.core.use_case.kinds import declare_kinds
from loom.jobs.aws.queue import QueueConfig
from loom.jobs.aws.sqs import SQSConfig, SQSJobTransport
from loom.jobs.inline import AsyncioJobTimer, InlineJobTransport
from tests.helpers.celery import BrokerMessage, CeleryBroker
from tests.helpers.composition import composition_project

DEFAULT_QUEUE = "celery"
BROKER = {"broker_url": "memory://", "result_backend": "cache+memory://"}
SQS_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/emails"
SQS_FIFO_URL = "https://sqs.eu-west-1.amazonaws.com/123456789012/orders.fifo"


class EmailJob(Job[str]):
    __queue__ = "emails"

    def execute(self) -> str:
        return "email"


class RecordingExecutionObserver:
    """An execution observer that keeps every runtime event it receives."""

    def __init__(self) -> None:
        self.events: list[ExecutionEvent] = []

    @property
    def kinds(self) -> list[EventKind]:
        """The kind of each event received, in order."""
        return [event.kind for event in self.events]

    def on_event(self, event: ExecutionEvent) -> None:
        self.events.append(event)


class JobsAdapter:
    """Compose the given jobs, as a worker would, and mount nothing."""

    def __init__(self, *jobs: type[Job[Any]]) -> None:
        self._jobs = jobs

    def recipe(self, project: Project) -> Recipe:
        return Recipe(
            profile=WORKER_PROFILE,
            discovered=DiscoveryResult(models=(), use_cases=cast(Any, self._jobs), jobs=self._jobs),
            connection_only=True,
        )

    def mount(self, application: Application) -> Application:
        return application


def job_project(tmp_path: Path, config: dict[str, Any], job_service: str | None = None) -> Project:
    return composition_project(
        ConfigContext.from_dict(config),
        app=AppConfig(name="jobs", job_service=job_service),
        code_path=tmp_path,
        config_root=tmp_path,
    )


class SQSClient:
    """Stands in for the boto3 ``sqs`` client.

    ``requests`` holds each message sent, one ``send_message`` or one batch
    entry; ``batches`` each ``send_message_batch`` call as its queue and ids.
    ``refuse`` counts how many more times SQS refuses a task's batch entry.
    """

    def __init__(self, error: Exception | None = None, *, overlap: float = 0) -> None:
        self.requests: list[dict[str, Any]] = []
        self.batches: list[tuple[str, list[str]]] = []
        self.refuse: dict[str, int] = {}
        self.sent = threading.Event()
        self.peak = 0
        self.entered = threading.Event()
        self.hold: threading.Event | None = None
        self._error = error
        self._overlap = overlap
        self._in_flight = 0
        self._lock = threading.Lock()
        self._other = threading.Condition(self._lock)

    def send_message(self, **request: Any) -> dict[str, str]:
        if self._error is not None:
            raise self._error
        self.requests.append(request)
        self.sent.set()
        return {"MessageId": "m-1"}

    def send_message_batch(self, QueueUrl: str, Entries: list[dict[str, Any]]) -> dict[str, Any]:  # noqa: N803
        self.entered.set()
        if self.hold is not None:
            self.hold.wait(5)
        self._wait_for_another_call()
        self.batches.append((QueueUrl, [entry["Id"] for entry in Entries]))
        if self._error is not None:
            raise self._error
        failed = []
        for entry in Entries:
            if self.refuse.get(entry["Id"], 0) > 0:
                self.refuse[entry["Id"]] -= 1
                failed.append({"Id": entry["Id"], "Code": "InternalError", "Message": "hunter2"})
            else:
                self.requests.append({"QueueUrl": QueueUrl} | entry)
        self.sent.set()
        return {"Successful": [], "Failed": failed}

    def _wait_for_another_call(self) -> None:
        """Hold the call up to ``overlap`` seconds while another one starts: records the peak."""
        with self._other:
            self._in_flight += 1
            self.peak = max(self.peak, self._in_flight)
            self._other.notify_all()
            self._other.wait_for(lambda: self._in_flight > 1, timeout=self._overlap)
            self._in_flight -= 1


def sqs_service(client: SQSClient, **knobs: Any) -> JobService:
    """A job service over *client* sending ``emails`` and FIFO ``orders``."""
    queues = {"emails": QueueConfig(url=SQS_URL), "orders": QueueConfig(url=SQS_FIFO_URL)}
    config = SQSConfig(queues=queues, **knobs)
    return JobService(SQSJobTransport(client, config))


class InlineServiceFixture(JobService):
    """A public job service using inline transport for direct tests.

    Without *timer*, it runs as a process that cannot wait (a Lambda): its jobs run
    on an :class:`AsyncioJobTimer` and every delay is refused.
    """

    def __init__(
        self,
        factory: Any,
        executor: Any,
        *,
        settings: JobSettings | None = None,
        callbacks: DispatchableCallbacks = ANY_CALLBACKS,
        timer: JobTimer | None = None,
        execution_observer: ExecutionObserver | None = None,
    ) -> None:
        super().__init__(
            InlineJobTransport(
                factory,
                executor,
                timer=AsyncioJobTimer() if timer is None else timer,
                can_wait_in_process=timer is not None,
                execution_observer=execution_observer,
            ),
            factory=factory,
            executor=executor,
            settings=settings,
            callbacks=callbacks,
        )


class CeleryServiceFixture(JobService):
    """A public job service using Celery transport for direct tests."""

    def __init__(
        self,
        app: Any,
        execution_observer: ExecutionObserver | None = None,
        factory: Any = None,
        executor: Any = None,
        observability: Any = None,
        *,
        settings: JobSettings | None = None,
        callbacks: DispatchableCallbacks = ANY_CALLBACKS,
    ) -> None:
        super().__init__(
            CeleryJobTransport(
                app, execution_observer=execution_observer, observability=observability
            ),
            factory=factory,
            executor=executor,
            settings=settings,
            callbacks=callbacks,
        )


class SQSServiceFixture(JobService):
    """A public job service using SQS transport for direct tests."""

    def __init__(
        self,
        client: Any,
        config: SQSConfig,
        *,
        factory: Any,
        executor: Any,
        execution_observer: ExecutionObserver | None = None,
        settings: JobSettings | None = None,
    ) -> None:
        super().__init__(
            SQSJobTransport(client, config, execution_observer=execution_observer),
            factory=factory,
            executor=executor,
            settings=settings,
        )


class JobKernel:
    """A real factory and executor, serving the jobs a test delivers."""

    def __init__(self) -> None:
        container = LoomContainer(classify_key=classify_key, implicit=main_repository)
        declare_kinds(container.kinds)
        self.factory = UseCaseFactory(container)
        self.executor = UseCaseExecutor(UseCaseCompiler())

    def invoke_envelope(self) -> Callable[[JobDelivery], Awaitable[Any]]:
        return EnvelopeInvocation(self.factory, self.executor, Observability.noop())


class CeleryWorker:
    """The Celery tasks of jobs and callbacks over the in-memory broker, run as a worker runs them.

    A retry or a failure callback a task publishes lands in :attr:`broker`.

    Args:
        settings: The job settings the tasks are built with.
        initialized: Whether the async runtime is up, as in a forked worker;
            without it only an eager request runs.

    Attributes:
        broker: The broker every task of this worker publishes to.
        kernel: The factory and executor the tasks run jobs with.
        runtime: The per-process async runtime.
        execution_observer: The events the job tasks emitted.
    """

    TASK_ID = "task-1"

    def __init__(self, *, settings: JobSettings | None = None, initialized: bool = True) -> None:
        self.broker = CeleryBroker([DEFAULT_QUEUE])
        self.kernel = JobKernel()
        self.runtime = _CeleryProcessBridge()
        self.execution_observer = RecordingExecutionObserver()
        self._settings = settings or JobSettings()
        if initialized:
            self.runtime.initialize()

    def close(self) -> None:
        """Shut the async runtime down."""
        self.runtime.shutdown()

    def published(self, task_prefix: str) -> list[BrokerMessage]:
        """The messages published so far for the tasks whose name starts with *task_prefix*.

        A retry is a job message published again; a failure callback is the message of its task.
        """
        return [m for m in self.broker.sent() if m.task.startswith(task_prefix)]

    def task(self, job_type: type[Job[Any]], observability: Observability | None = None) -> Any:
        """Register *job_type* and return its Celery task."""
        self.kernel.factory.register(job_type)
        return _make_job_task(
            self.broker.app,
            job_type,
            self.kernel.factory,
            self.kernel.executor,
            self.runtime,
            self.execution_observer,
            observability,
            settings=self._settings,
        )

    def callback_task(self, callback_type: type[Any], outcome: CallbackOutcome) -> Any:
        """Register *callback_type* and return the task calling its hook for *outcome*."""
        self.kernel.factory.register(callback_type)
        return _make_callback_task(
            self.broker.app, callback_type, outcome, self.kernel.factory, self.runtime
        )

    def run(
        self,
        job_type: type[Job[Any]],
        *,
        attempt: int = 1,
        eager: bool | None = False,
        observability: Observability | None = None,
        **kwargs: Any,
    ) -> Any:
        """Run one delivery of *job_type* as the worker does and return its result.

        Args:
            job_type: The job the task runs.
            attempt: The attempt the request carries; postponements add retries to it.
            eager: Whether the request is eager; ``None`` for a request that does not say.
            observability: What opens the job's span.
            **kwargs: The task's keyword arguments: ``payload``, ``params``,
                ``trace_id``, ``identity``, ``postponements``, ``failure_callback``.

        Returns:
            What the job returned.

        Raises:
            Retry: The task retried: its message is in :attr:`broker`.
            Exception: The error that ended the task.
        """
        task = self.task(job_type, observability)
        task.push_request(
            id=self.TASK_ID,
            retries=attempt - 1 + kwargs.get("postponements", 0),
            kwargs=kwargs,
            is_eager=eager,
            called_directly=False,
            delivery_info={"routing_key": DEFAULT_QUEUE},
        )
        try:
            return task.run(**kwargs)
        finally:
            task.pop_request()
