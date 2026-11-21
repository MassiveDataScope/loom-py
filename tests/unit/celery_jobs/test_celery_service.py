"""What only the Celery transport does with a dispatch: broker options, links and its events.

What every transport does (the message, its queue, the countdown, the commit) is
the dispatch contract of ``tests/contract/jobs``.
"""

from __future__ import annotations

import threading
from typing import Any

import pytest
from celery import Celery

from loom.celery.constants import TASK_JOB_PREFIX
from loom.celery.service import CallbackLink, CeleryJobTransport
from loom.core.engine.events import EventKind
from loom.core.job.callback import DispatchableCallbacks, JobCallback
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.job import Job
from loom.core.job.service import JobService
from loom.core.logger.failure import FailureReference
from loom.core.tracing import reset_trace_id, set_trace_id
from tests.helpers.celery import CeleryBroker
from tests.helpers.jobs import RecordingExecutionObserver
from tests.helpers.spans import build_recorder

QUEUES = ("email", "heavy")
DEFAULT_QUEUE = "celery"


class EmailJob(Job[None]):
    __queue__ = "email"

    def execute(self) -> None:
        return None


class HeavyJob(Job[int]):
    __queue__ = "heavy"
    __priority__ = 5
    __timeout__ = 300

    def execute(self) -> int:
        return 42


class SuccessCallback(JobCallback):
    pass


class FailureCallback(JobCallback):
    pass


@pytest.fixture
def broker() -> CeleryBroker:
    return CeleryBroker(QUEUES)


@pytest.fixture
def observer() -> RecordingExecutionObserver:
    return RecordingExecutionObserver()


@pytest.fixture
def service(broker: CeleryBroker, observer: RecordingExecutionObserver) -> JobService:
    return JobService(
        CeleryJobTransport(broker.app, execution_observer=observer),
        callbacks=DispatchableCallbacks([SuccessCallback, FailureCallback]),
    )


class TestBrokerOptions:
    @pytest.mark.parametrize(
        ("given", "expected"), [(None, 5), (1, 1)], ids=["job priority", "call priority"]
    )
    async def test_the_message_carries_the_effective_priority(
        self, service: JobService, broker: CeleryBroker, given: int | None, expected: int
    ) -> None:
        service.dispatch_after_commit(HeavyJob, priority=given)
        await flush_pending_dispatches()

        assert [message.priority for message in broker.sent()] == [expected]

    @pytest.mark.parametrize(
        ("job", "expected"), [(HeavyJob, 300), (EmailJob, None)], ids=["timeout", "no timeout"]
    )
    async def test_the_message_carries_the_soft_time_limit_of_the_job(
        self,
        service: JobService,
        broker: CeleryBroker,
        job: type[Job[object]],
        expected: int | None,
    ) -> None:
        service.dispatch_after_commit(job)
        await flush_pending_dispatches()

        assert [message.soft_time_limit for message in broker.sent()] == [expected]

    async def test_the_message_names_the_task_of_the_job(
        self, service: JobService, broker: CeleryBroker
    ) -> None:
        service.dispatch_after_commit(EmailJob)
        await flush_pending_dispatches()

        assert [message.task for message in broker.sent()] == [
            f"{TASK_JOB_PREFIX}.{EmailJob.__qualname__}"
        ]


class _ThreadRecordingCelery(Celery):  # type: ignore[misc]
    """A Celery application that notes the thread each ``send_task`` runs on."""

    def __init__(self) -> None:
        super().__init__(broker="memory://", backend="cache+memory://")
        self.send_threads: list[int] = []

    def send_task(self, *args: Any, **kwargs: Any) -> Any:
        self.send_threads.append(threading.get_ident())
        return super().send_task(*args, **kwargs)


class TestSendNow:
    async def test_the_broker_call_leaves_the_event_loop_thread(self) -> None:
        app = _ThreadRecordingCelery()
        service = JobService(CeleryJobTransport(app))

        await service.dispatch_now(EmailJob)

        assert [thread == threading.get_ident() for thread in app.send_threads] == [False]


class TestCallbacks:
    async def test_without_callbacks_the_message_has_no_link_and_no_failure_callback(
        self, service: JobService, broker: CeleryBroker
    ) -> None:
        service.dispatch_after_commit(EmailJob)
        await flush_pending_dispatches()

        (message,) = broker.sent()
        assert (message.callbacks, message.kwargs["failure_callback"]) == ([], None)

    @pytest.mark.parametrize("trace_id", [None, "acme-trace"], ids=["no-trace", "trace"])
    async def test_the_success_link_names_its_task_and_carries_the_dispatch(
        self, service: JobService, broker: CeleryBroker, trace_id: str | None
    ) -> None:
        token = set_trace_id(trace_id)
        try:
            handle = service.dispatch_after_commit(
                EmailJob, payload={"order_id": 99}, on_success=SuccessCallback
            )
        finally:
            reset_trace_id(token)
        await flush_pending_dispatches()

        (message,) = broker.sent()
        (link,) = message.callbacks
        assert (link["task"], link["immutable"], link["kwargs"]) == (
            "loom.callback.SuccessCallback",
            False,
            {
                "job_id": handle.job_id,
                "context": {"order_id": 99},
                "trace_id": trace_id,
                "identity": None,
            },
        )

    async def test_the_failure_callback_travels_as_a_task_name_in_the_kwargs(
        self, service: JobService, broker: CeleryBroker
    ) -> None:
        service.dispatch_after_commit(EmailJob, on_failure=FailureCallback)
        await flush_pending_dispatches()

        (message,) = broker.sent()
        assert message.kwargs["failure_callback"] == "loom.callback_error.FailureCallback"


class TestFailureCallbackSend:
    def test_the_worker_sends_the_callback_task_with_the_link_and_the_failure_reference(
        self,
    ) -> None:
        broker = CeleryBroker([DEFAULT_QUEUE])
        link = CallbackLink("job-1", {"order_id": 99}, "acme-trace", None)

        failure = FailureReference("ValueError", None, "acme-trace", expected=False, retryable=True)

        link.send_failure(broker.app, "loom.callback_error.Acme", failure)

        (message,) = broker.sent()
        assert (message.task, message.kwargs) == (
            "loom.callback_error.Acme",
            {
                "job_id": "job-1",
                "context": {"order_id": 99},
                "trace_id": "acme-trace",
                "identity": None,
                "exc_type": "ValueError",
                "exc_msg": "ValueError trace_id=acme-trace",
            },
        )


class TestEvents:
    def test_a_dispatch_emits_job_dispatched_with_the_job_and_trace(
        self, service: JobService, observer: RecordingExecutionObserver
    ) -> None:
        token = set_trace_id("dispatch-trace")
        try:
            service.dispatch_after_commit(EmailJob)
        finally:
            reset_trace_id(token)

        (event,) = observer.events
        assert (event.kind, event.use_case_name, event.trace_id) == (
            EventKind.JOB_DISPATCHED,
            EmailJob.__qualname__,
            "dispatch-trace",
        )

    async def test_the_broker_send_is_one_transport_span(self, broker: CeleryBroker) -> None:
        recorder = build_recorder()
        service = JobService(CeleryJobTransport(broker.app, observability=recorder.runtime))

        service.dispatch_after_commit(EmailJob)
        await flush_pending_dispatches()

        assert recorder.names() == [f"transport:{TASK_JOB_PREFIX}.{EmailJob.__qualname__}"]
