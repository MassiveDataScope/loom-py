"""What the Celery task of a job and of a callback do on the worker, over the in-memory broker."""

from __future__ import annotations

import asyncio
import warnings
from collections.abc import Callable, Coroutine, Iterator
from typing import Any, ClassVar

import pytest
from celery import Celery
from celery.exceptions import Retry

from loom.celery.constants import TASK_JOB_PREFIX, callback_task_name
from loom.celery.errors import WorkerNotOpenError
from loom.celery.runner import _CeleryAsyncRuntime, _make_callback_task
from loom.core.engine.events import EventKind
from loom.core.errors import (
    PostCommitError,
    ScopeOwnershipError,
    StoresNotCommitted,
    UnitOfWorkInsideScope,
)
from loom.core.identity import Identity, current_identity
from loom.core.job.callback import CallbackOutcome
from loom.core.job.config import JobConfig, JobOptions, JobSettings, RetryConfig
from loom.core.job.delivery import JobDelivery
from loom.core.job.job import Job
from loom.core.job.outcome import JobAbandonment, JobPostponement
from loom.core.job.policy import Backoff
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import InvocationOutcome, Uncertain
from loom.core.stores.uow.scope import invocation_error
from loom.core.tracing import get_trace_id
from loom.core.use_case import Delivery
from loom.core.use_case.markers import Caller
from tests.helpers.jobs import CeleryWorker
from tests.helpers.spans import build_recorder, hex_trace

WIRE_TRACE = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
BACKOFF = Backoff(first_s=3, factor=3, max_s=300, jitter=False)
JOB_TASKS = f"{TASK_JOB_PREFIX}."


class DoubleJob(Job[int]):
    __retry__ = BACKOFF

    def execute(self, value: int) -> int:
        return value * 2


class UpperJob(Job[str]):
    __queue__ = "heavy"
    __timeout__ = 300

    async def execute(self, msg: str) -> str:
        return msg.upper()


class DeliveryJob(Job[None]):
    __retry__ = BACKOFF
    __timeout__ = 300
    seen: ClassVar[list[JobDelivery | None]] = []

    def execute(self, delivery: JobDelivery | None = Delivery()) -> None:
        self.seen.append(delivery)


class TraceJob(Job[None]):
    seen: ClassVar[list[str | None]] = []

    def execute(self) -> None:
        self.seen.append(get_trace_id())


class CallerJob(Job[str]):
    def execute(self, caller: Identity = Caller()) -> str:
        return caller.subject


class ContextJob(Job[str | None]):
    def execute(self) -> str | None:
        caller = current_identity()
        return caller.subject if caller.is_authenticated else None


ERRORS: dict[str, Callable[[], Exception]] = {
    "post-commit-committed": lambda: PostCommitError(
        committed=True, failures=(ConnectionError("broker down"),)
    ),
    "post-commit-uncommitted": lambda: PostCommitError(
        committed=False, failures=(ConnectionError("broker down"),)
    ),
    "stores-not-committed": lambda: StoresNotCommitted("audit=Committed, default=RolledBack"),
    "scope-ownership": lambda: ScopeOwnershipError("the invocation scope is already closed"),
    "uow-inside-scope": lambda: UnitOfWorkInsideScope.entering(object()),
    "one-store-uncertain": lambda: invocation_error(
        InvocationOutcome((Uncertain(DEFAULT_STORE, OSError("ack")),)), None
    ),
}
"""Each error a job can raise, by the name a test gives the job to raise it."""

NOT_RETRIED = ["post-commit-committed", "stores-not-committed", "scope-ownership"]
NOT_RETRIED += ["uow-inside-scope", "one-store-uncertain"]


class RaisingJob(Job[None]):
    __retry__ = BACKOFF

    def execute(self, kind: str) -> None:
        raise ERRORS[kind]()


class FailingJob(Job[None]):
    __retry__ = BACKOFF

    def execute(self) -> None:
        raise ValueError("err")


class PostponingJob(Job[None]):
    __retry__ = BACKOFF

    def execute(self, delay: int) -> None:
        raise JobPostponement("not yet", delay)


class AbandoningJob(Job[None]):
    def execute(self) -> None:
        raise JobAbandonment("never")


@pytest.fixture(autouse=True)
def _forget_what_jobs_saw() -> None:
    DeliveryJob.seen.clear()
    TraceJob.seen.clear()


@pytest.fixture
def worker() -> Iterator[CeleryWorker]:
    worker = CeleryWorker()
    yield worker
    worker.close()


class TestRegistration:
    def test_the_task_is_named_after_the_job(self, worker: CeleryWorker) -> None:
        assert worker.task(DoubleJob).name == f"{TASK_JOB_PREFIX}.{DoubleJob.__qualname__}"

    @pytest.mark.parametrize(
        ("option", "expected"),
        [
            ("acks_late", True),
            ("acks_on_failure_or_timeout", True),
            ("reject_on_worker_lost", True),
            ("max_retries", None),
        ],
        ids=["acks late", "acks on failure", "rejects on worker lost", "no attempt limit"],
    )
    def test_the_task_option(self, worker: CeleryWorker, option: str, expected: object) -> None:
        assert getattr(worker.task(DoubleJob), option) is expected

    @pytest.mark.parametrize(("job", "expected"), [(UpperJob, 300), (DoubleJob, None)])
    def test_the_soft_time_limit_is_the_timeout_of_the_job(
        self, worker: CeleryWorker, job: type[Job[Any]], expected: int | None
    ) -> None:
        assert worker.task(job).soft_time_limit == expected


class TestExecution:
    def test_a_sync_job_returns_its_result(self, worker: CeleryWorker) -> None:
        assert worker.run(DoubleJob, params={"value": 4}) == 8

    def test_an_async_job_returns_its_result(self, worker: CeleryWorker) -> None:
        assert worker.run(UpperJob, params={"msg": "hello"}) == "HELLO"

    def test_the_job_reads_its_delivery_with_the_attempt_and_the_policy(
        self, worker: CeleryWorker
    ) -> None:
        worker.run(DeliveryJob, attempt=2)

        (delivery,) = DeliveryJob.seen
        assert delivery is not None
        assert (delivery.task_id, delivery.attempt, delivery.postponements) == ("task-1", 2, 0)
        assert delivery.policy.backoff == BACKOFF

    def test_the_run_timeout_is_the_soft_time_limit(self, worker: CeleryWorker) -> None:
        worker.run(DeliveryJob)

        (delivery,) = DeliveryJob.seen
        assert delivery is not None
        assert delivery.timeout_s == pytest.approx(300.0)

    def test_the_job_acts_as_the_caller_of_the_envelope(self, worker: CeleryWorker) -> None:
        assert worker.run(CallerJob, identity={"subject": "user-1"}) == "user-1"

    def test_the_worker_publishes_the_caller_and_restores_the_context_afterwards(
        self, worker: CeleryWorker
    ) -> None:
        assert worker.run(ContextJob, identity={"subject": "user-1"}) == "user-1"
        assert not current_identity().is_authenticated

    def test_a_job_span_lands_in_the_trace_the_dispatcher_sent(self, worker: CeleryWorker) -> None:
        recorder = build_recorder()

        worker.run(TraceJob, trace_id=WIRE_TRACE, observability_runtime=recorder.runtime)

        span = recorder.one("job:TraceJob")
        assert hex_trace(span) == WIRE_TRACE
        assert span.parent is None

    def test_a_job_without_a_trace_id_gets_a_span_on_a_random_trace(
        self, worker: CeleryWorker
    ) -> None:
        recorder = build_recorder()

        worker.run(TraceJob, observability_runtime=recorder.runtime)

        assert hex_trace(recorder.one("job:TraceJob")) != WIRE_TRACE


class TestEagerRequest:
    def test_an_eager_request_runs_without_the_worker_bridge(self) -> None:
        worker = CeleryWorker(initialized=False)

        assert worker.run(DoubleJob, eager=True, params={"value": 3}) == 6

    def test_an_eager_request_of_a_job_with_a_time_limit_runs_within_it(self) -> None:
        worker = CeleryWorker(initialized=False)

        assert worker.run(UpperJob, eager=True, params={"msg": "ok"}) == "OK"

    def test_a_request_without_the_bridge_that_is_not_eager_is_refused(self) -> None:
        worker = CeleryWorker(initialized=False)

        with pytest.raises(RuntimeError, match="async bridge is not initialized"):
            worker.run(DoubleJob, params={"value": 1})

    @pytest.mark.parametrize(("always_eager", "runs"), [(True, True), (False, False)])
    def test_a_request_that_does_not_say_follows_the_eager_setting_of_the_application(
        self, always_eager: bool, runs: bool
    ) -> None:
        worker = CeleryWorker(initialized=False)
        worker.broker.app.conf.task_always_eager = always_eager

        if runs:
            assert worker.run(DoubleJob, eager=None, params={"value": 2}) == 4
        else:
            with pytest.raises(RuntimeError, match="async bridge is not initialized"):
                worker.run(DoubleJob, eager=None, params={"value": 2})


class TestWorkerThatDidNotOpen:
    def test_every_task_of_the_process_fails_without_running_the_job(
        self, worker: CeleryWorker
    ) -> None:
        worker.runtime.refuse(RuntimeError("clickhouse unreachable"))

        with pytest.raises(WorkerNotOpenError) as refused:
            worker.run(TraceJob)

        assert isinstance(refused.value.__cause__, RuntimeError)
        assert TraceJob.seen == []


class TestRetry:
    def test_a_failure_with_attempts_left_publishes_the_message_again_after_the_backoff(
        self, worker: CeleryWorker
    ) -> None:
        with pytest.raises(Retry):
            worker.run(FailingJob, payload={"value": 1})

        (message,) = worker.published(JOB_TASKS)
        assert (message.task_id, message.countdown, message.kwargs["payload"]) == (
            "task-1",
            3,
            {"value": 1},
        )

    def test_the_delay_grows_with_the_attempt(self) -> None:
        config = JobConfig(retry=RetryConfig(max_attempts=5))
        settings = JobSettings({FailingJob: JobOptions.resolve(FailingJob, config)})
        worker = CeleryWorker(settings=settings)

        with pytest.raises(Retry):
            worker.run(FailingJob, attempt=3)

        assert [message.countdown for message in worker.published(JOB_TASKS)] == [27]
        worker.close()

    def test_the_last_attempt_raises_the_error_and_publishes_nothing(
        self, worker: CeleryWorker
    ) -> None:
        with pytest.raises(ValueError, match="err"):
            worker.run(FailingJob, attempt=3)

        assert worker.published(JOB_TASKS) == []

    def test_a_postponement_publishes_again_after_its_delay_counting_one_more(
        self, worker: CeleryWorker
    ) -> None:
        with pytest.raises(Retry) as raised:
            worker.run(PostponingJob, postponements=2, params={"delay": 7}, payload={"value": 1})

        (message,) = worker.published(JOB_TASKS)
        assert isinstance(raised.value.exc, JobPostponement)
        assert (message.countdown, message.kwargs["postponements"]) == (7, 3)
        assert message.kwargs["payload"] == {"value": 1}

    def test_a_postponement_of_a_request_without_kwargs_carries_only_its_postponements(
        self, worker: CeleryWorker
    ) -> None:
        with pytest.raises(Retry):
            worker.run(PostponingJob, params={"delay": 7})

        (message,) = worker.published(JOB_TASKS)
        assert message.kwargs["postponements"] == 1

    def test_postponements_do_not_use_up_the_attempts(self, worker: CeleryWorker) -> None:
        with pytest.raises(Retry):
            worker.run(FailingJob, postponements=5)

    def test_a_job_abandonment_raises_without_publishing_again(self, worker: CeleryWorker) -> None:
        with pytest.raises(JobAbandonment, match="never"):
            worker.run(AbandoningJob)

        assert worker.published(JOB_TASKS) == []

    @pytest.mark.parametrize("kind", NOT_RETRIED)
    def test_an_error_a_retry_cannot_help_is_raised_without_publishing_again(
        self, worker: CeleryWorker, kind: str
    ) -> None:
        with pytest.raises(type(ERRORS[kind]())):
            worker.run(RaisingJob, params={"kind": kind})

        assert worker.published(JOB_TASKS) == []

    def test_a_post_commit_error_without_a_commit_is_retried(self, worker: CeleryWorker) -> None:
        with pytest.raises(Retry):
            worker.run(RaisingJob, params={"kind": "post-commit-uncommitted"})


class TestFailureCallback:
    CALLBACK = "loom.callback_error.AcmeCallback"

    def test_an_abandoned_job_sends_the_callback_once_with_the_error_class_and_message(
        self, worker: CeleryWorker
    ) -> None:
        with pytest.raises(ValueError):
            worker.run(
                FailingJob,
                attempt=3,
                payload={"value": 1},
                trace_id="acme-trace",
                failure_callback=self.CALLBACK,
            )

        (message,) = worker.published(self.CALLBACK)
        assert message.kwargs == {
            "job_id": "task-1",
            "context": {"value": 1},
            "trace_id": "acme-trace",
            "identity": None,
            "exc_type": "ValueError",
            "exc_msg": "err",
        }

    def test_a_job_abandonment_names_its_own_class_and_reason(self, worker: CeleryWorker) -> None:
        with pytest.raises(JobAbandonment):
            worker.run(AbandoningJob, failure_callback=self.CALLBACK)

        (message,) = worker.published(self.CALLBACK)
        assert (message.kwargs["exc_type"], message.kwargs["exc_msg"]) == (
            "JobAbandonment",
            "never",
        )

    def test_a_failure_that_is_retried_sends_nothing(self, worker: CeleryWorker) -> None:
        with pytest.raises(Retry):
            worker.run(FailingJob, failure_callback=self.CALLBACK)

        assert worker.published(self.CALLBACK) == []

    def test_an_abandoned_job_without_a_failure_callback_sends_nothing(
        self, worker: CeleryWorker
    ) -> None:
        with pytest.raises(ValueError):
            worker.run(FailingJob, attempt=3)

        assert worker.published(self.CALLBACK) == []


class TestEvents:
    def test_a_successful_run_emits_started_then_succeeded_with_a_duration(
        self, worker: CeleryWorker
    ) -> None:
        worker.run(TraceJob)

        started, succeeded = worker.metrics.events
        assert (started.kind, succeeded.kind) == (EventKind.JOB_STARTED, EventKind.JOB_SUCCEEDED)
        assert succeeded.duration_ms is not None
        assert succeeded.duration_ms >= 0

    def test_every_event_carries_the_trace_the_job_runs_in(self, worker: CeleryWorker) -> None:
        worker.run(TraceJob, trace_id=WIRE_TRACE)

        assert TraceJob.seen == [WIRE_TRACE]
        assert {event.trace_id for event in worker.metrics.events} == {WIRE_TRACE}
        assert get_trace_id() is None

    def test_a_trace_less_job_installs_no_context_and_emits_events_with_none(
        self, worker: CeleryWorker
    ) -> None:
        worker.run(TraceJob)

        assert TraceJob.seen == [None]
        assert worker.metrics.kinds == [EventKind.JOB_STARTED, EventKind.JOB_SUCCEEDED]
        assert {event.trace_id for event in worker.metrics.events} == {None}

    @pytest.mark.parametrize(
        ("attempt", "raised", "kind"),
        [(1, Retry, EventKind.JOB_FAILED), (3, ValueError, EventKind.JOB_ABANDONED)],
        ids=["attempt left", "last attempt"],
    )
    def test_a_failure_emits_failed_or_abandoned_without_a_trace(
        self, worker: CeleryWorker, attempt: int, raised: type[Exception], kind: EventKind
    ) -> None:
        with pytest.raises(raised):
            worker.run(FailingJob, attempt=attempt)

        assert kind in worker.metrics.kinds
        assert {event.trace_id for event in worker.metrics.events} == {None}

    def test_a_post_commit_error_that_committed_is_abandoned_not_failed(
        self, worker: CeleryWorker
    ) -> None:
        with pytest.raises(PostCommitError):
            worker.run(RaisingJob, params={"kind": "post-commit-committed"})

        assert EventKind.JOB_FAILED not in worker.metrics.kinds
        assert EventKind.JOB_ABANDONED in worker.metrics.kinds


class Hooks:
    """A callback recording what each hook received."""

    seen: ClassVar[list[tuple[str, dict[str, Any]]]] = []

    def on_success(self, job_id: str, result: object, **context: object) -> None:
        self.seen.append((job_id, {"result": result, **context}))

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: object) -> None:
        self.seen.append((job_id, {"exc_type": exc_type, "exc_msg": exc_msg, **context}))


class AsyncHooks(Hooks):
    async def on_success(self, job_id: str, result: object, **context: object) -> None:  # type: ignore[override]
        super().on_success(job_id, result, **context)

    async def on_failure(  # type: ignore[override]
        self, job_id: str, exc_type: str, exc_msg: str, **context: object
    ) -> None:
        super().on_failure(job_id, exc_type, exc_msg, **context)


class CoroutineFromDef(Hooks):
    """A callback whose plain ``def`` hooks return a coroutine."""

    def on_success(  # type: ignore[override]
        self, job_id: str, result: object, **context: object
    ) -> Coroutine[Any, Any, None]:
        return asyncio.sleep(0)

    def on_failure(  # type: ignore[override]
        self, job_id: str, exc_type: str, exc_msg: str, **context: object
    ) -> Coroutine[Any, Any, None]:
        return asyncio.sleep(0)


OUTCOMES = pytest.mark.parametrize(
    ("outcome", "job_result", "arrived", "details"),
    [
        (CallbackOutcome.SUCCESS, ("acme",), {}, {"result": "acme"}),
        (
            CallbackOutcome.FAILURE,
            (),
            {"exc_type": "ValueError", "exc_msg": "bad input"},
            {"exc_type": "ValueError", "exc_msg": "bad input"},
        ),
    ],
    ids=["success", "failure"],
)
"""Each outcome: what Celery passes positionally, the keyword arguments a failure is sent with,
and the details the hook receives."""


@pytest.fixture(autouse=True)
def _forget_what_hooks_saw() -> None:
    Hooks.seen.clear()


@OUTCOMES
class TestCallbackTask:
    def test_the_task_is_named_after_its_callback_and_outcome(
        self,
        worker: CeleryWorker,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
        arrived: dict[str, Any],
        details: dict[str, Any],
    ) -> None:
        task = worker.callback_task(Hooks, outcome)

        assert task.name == callback_task_name(Hooks, outcome)

    @pytest.mark.parametrize("hooks", [Hooks, AsyncHooks], ids=["sync", "async"])
    def test_the_hook_receives_the_job_id_the_details_and_the_context(
        self,
        worker: CeleryWorker,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
        arrived: dict[str, Any],
        details: dict[str, Any],
        hooks: type[Hooks],
    ) -> None:
        task = worker.callback_task(hooks, outcome)

        task.apply(
            args=job_result,
            kwargs={"job_id": "uuid-1", "context": {"order_id": 99}, **arrived},
        ).get()

        assert Hooks.seen == [("uuid-1", {**details, "order_id": 99})]

    def test_a_plain_def_hook_returning_an_awaitable_is_refused_and_closed(
        self,
        worker: CeleryWorker,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
        arrived: dict[str, Any],
        details: dict[str, Any],
    ) -> None:
        task = worker.callback_task(CoroutineFromDef, outcome)

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            result = task.apply(args=job_result, kwargs={"job_id": "j", "context": {}, **arrived})
            with pytest.raises(TypeError, match="async def"):
                result.get()

    def test_an_async_hook_needs_the_bridge_unless_the_request_is_eager(
        self,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
        arrived: dict[str, Any],
        details: dict[str, Any],
    ) -> None:
        worker = CeleryWorker(initialized=False)
        task = worker.callback_task(AsyncHooks, outcome)
        kwargs = {"job_id": "x", "context": {}, **arrived}

        with pytest.raises(RuntimeError, match="async bridge is not initialized"):
            task(*job_result, **kwargs)
        task.apply(args=job_result, kwargs=kwargs).get()

        assert [job_id for job_id, _ in Hooks.seen] == ["x"]


class TwinJob(Job[int]):
    """Registered on two applications under the same task name."""

    def execute(self) -> int:
        return 0


class TestTwoApplicationsInOneProcess:
    """Each application serves its own task, never one registered for another.

    A task shared through Celery's process-wide finalizers is registered on
    every application finalized afterwards, and the first one under a name
    wins: both applications would then run the same closure, bound to one
    runtime. Both register before either is finalized, so a shared task
    always lands on the wrong application for one of them.
    """

    def test_each_job_task_belongs_to_its_own_application(self) -> None:
        first, second = CeleryWorker(initialized=False), CeleryWorker(initialized=False)
        name = f"{TASK_JOB_PREFIX}.{TwinJob.__qualname__}"
        tasks = [worker.task(TwinJob) for worker in (first, second)]

        assert [first.broker.app.tasks[name], second.broker.app.tasks[name]] == tasks
        assert tasks[0] is not tasks[1]

    @OUTCOMES
    def test_each_callback_task_belongs_to_its_own_application(
        self,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
        arrived: dict[str, Any],
        details: dict[str, Any],
    ) -> None:
        workers = (CeleryWorker(initialized=False), CeleryWorker(initialized=False))
        tasks = [worker.callback_task(Hooks, outcome) for worker in workers]
        name = callback_task_name(Hooks, outcome)

        assert [worker.broker.app.tasks[name] for worker in workers] == tasks
        assert tasks[0] is not tasks[1]


class SyncSeen:
    seen: ClassVar[list[tuple[str, object]]] = []

    def on_success(self, job_id: str, result: object, **context: object) -> None:
        self.seen.append((job_id, result))

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: object) -> None:
        raise AssertionError


class AsyncSeen(SyncSeen):
    async def on_success(self, job_id: str, result: object, **context: object) -> None:  # type: ignore[override]
        self.seen.append((job_id, result))


class Builds:
    def build(self, callback_type: type[Any]) -> Any:
        return callback_type()


class TestLinkContextOnTheWorker:
    @pytest.mark.parametrize("callback", [SyncSeen, AsyncSeen], ids=["sync", "async"])
    def test_a_link_context_cannot_replace_the_reserved_arguments(
        self, callback: type[SyncSeen]
    ) -> None:
        app = Celery("acme")
        app.conf.task_always_eager = True
        callback.seen = []
        task = _make_callback_task(
            app,
            callback,
            CallbackOutcome.SUCCESS,
            Builds(),
            _CeleryAsyncRuntime(),  # type: ignore[arg-type]
        )

        task.apply(
            args=("acme-result",),
            kwargs={"job_id": "acme-job", "context": {"job_id": "forged", "result": "forged"}},
        ).get()

        assert callback.seen == [("acme-job", "acme-result")]
