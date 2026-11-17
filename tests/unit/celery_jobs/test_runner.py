"""Unit tests for Celery runner factories (Piece 7)."""

from __future__ import annotations

import asyncio
import warnings
from collections.abc import Coroutine, Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from celery import Celery

from loom.celery.constants import TASK_JOB_PREFIX, callback_task_name
from loom.celery.runner import (
    _CeleryAsyncRuntime,
    _make_callback_task,
    _make_job_task,
    _resolve_error_info,
)
from loom.core.engine.events import EventKind
from loom.core.errors import (
    PostCommitError,
    ScopeOwnershipError,
    StoresNotCommitted,
    UnitOfWorkInsideScope,
)
from loom.core.job.callback import CallbackOutcome
from loom.core.job.config import JobConfig, JobOptions, JobSettings, RetryConfig
from loom.core.job.delivery import JobDelivery
from loom.core.job.envelope import job_context
from loom.core.job.job import Job
from loom.core.job.outcome import JobRejection, JobRetry
from loom.core.job.policy import Backoff
from loom.core.observability.event import Scope
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.uow.coordinator import InvocationOutcome, Uncertain
from loom.core.stores.uow.scope import invocation_error
from loom.core.tracing import get_trace_id
from tests.helpers.spans import build_recorder, hex_trace

_SETTINGS = JobSettings()


def _settings_with_attempts(max_attempts: int) -> JobSettings:
    config = JobConfig(retry=RetryConfig(max_attempts=max_attempts))
    return JobSettings({_SyncJob: JobOptions.resolve(_SyncJob, config)})


def _executor_returning(value: object) -> MagicMock:
    return MagicMock(run=AsyncMock(return_value=value))


def _executor_raising(error: Exception) -> MagicMock:
    return MagicMock(run=AsyncMock(side_effect=error))


_WIRE_TRACE = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"

# ---------------------------------------------------------------------------
# Test fixtures / helpers
# ---------------------------------------------------------------------------


class _SyncJob(Job[int]):
    __queue__ = "default"
    __retry__ = Backoff(first_s=3, factor=3, max_s=300, jitter=False)
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self, value: int = 0) -> int:
        return value * 2


class _AsyncJob(Job[str]):
    __queue__ = "heavy"
    __countdown__ = 0
    __timeout__ = 300
    __priority__ = 0

    async def execute(self, msg: str = "") -> str:
        return msg.upper()


class _SyncCallback:
    def on_success(self, job_id: str, result: object, **ctx: object) -> None:
        pass  # intentional no-op stub

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: object) -> None:
        pass  # intentional no-op stub


class _AsyncCallback:
    async def on_success(self, job_id: str, result: object, **ctx: object) -> None:
        pass  # intentional no-op stub

    async def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: object) -> None:
        pass  # intentional no-op stub


class _RetryError(Exception):
    """Stands in for Celery's ``Retry`` raised by the mocked bound task."""


def _mock_celery_app() -> MagicMock:
    app = MagicMock()
    app.task = MagicMock(side_effect=lambda **kw: lambda fn: fn)
    app.conf.task_always_eager = False
    return app


def _mock_factory(instance: object) -> MagicMock:
    factory = MagicMock()
    factory.build = MagicMock(return_value=instance)
    return factory


def _factory_recording_trace(instance: object, sink: list[str | None]) -> MagicMock:
    """Factory whose ``build`` records the trace context visible inside the job."""
    factory = MagicMock()

    def _build(job_type: object) -> object:
        del job_type
        sink.append(get_trace_id())
        return instance

    factory.build = MagicMock(side_effect=_build)
    return factory


def _building_executor() -> MagicMock:
    """Executor whose ``run`` builds the job through the factory it is given, then returns 0."""

    async def run(job_type: object, *, factory: MagicMock, **_: object) -> int:
        factory.build(job_type)
        return 0

    return MagicMock(run=run)


def _mock_runtime(return_value: object = None, error: Exception | None = None) -> MagicMock:
    runtime = MagicMock()

    def _run(coro: object, *, timeout: float | None = None, eager_fallback: bool) -> object:
        del timeout, eager_fallback
        close = getattr(coro, "close", None)
        if callable(close):
            close()
        if error is not None:
            raise error
        return return_value

    runtime.run = MagicMock(side_effect=_run)
    runtime.initialize = MagicMock()
    runtime.shutdown = MagicMock()
    return runtime


def _executing_runtime() -> MagicMock:
    runtime = MagicMock()
    runtime.run = MagicMock(side_effect=lambda coro, **_: asyncio.run(coro))
    runtime.ensure_open = MagicMock()
    return runtime


def _mock_observability_runtime() -> tuple[
    MagicMock, list[tuple[tuple[object, ...], dict[str, object]]]
]:
    runtime = MagicMock()
    span_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    @contextmanager
    def _span(*args: object, **kwargs: object) -> Generator[None, None, None]:
        span_calls.append((args, kwargs))
        yield

    runtime.span = MagicMock(side_effect=_span)
    return runtime, span_calls


# ---------------------------------------------------------------------------
# Trace ID lifecycle
# ---------------------------------------------------------------------------


class TestTraceLifecycle:
    def test_absent_trace_preserves_current_context(self) -> None:
        before = get_trace_id()
        with job_context(None, None):
            assert get_trace_id() == before

    def test_trace_is_visible_during_job_context(self) -> None:
        with job_context("abc123", None):
            assert get_trace_id() == "abc123"

    def test_trace_is_restored_after_job_context(self) -> None:
        before = get_trace_id()
        with job_context("abc123", None):
            pass
        assert get_trace_id() == before


# ---------------------------------------------------------------------------
# _make_job_task — task registration
# ---------------------------------------------------------------------------


class TestMakeJobTaskRegistration:
    def test_task_registered_with_correct_name_sync(self) -> None:
        app = MagicMock()
        registered_kwargs: dict[str, object] = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _SyncJob, MagicMock(), MagicMock(), _mock_runtime(), settings=_SETTINGS)
        assert registered_kwargs["name"] == f"{TASK_JOB_PREFIX}.{_SyncJob.__qualname__}"

    def test_task_registered_with_correct_name_async(self) -> None:
        app = MagicMock()
        registered_kwargs: dict[str, object] = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(
            app, _AsyncJob, MagicMock(), MagicMock(), _mock_runtime(), settings=_SETTINGS
        )
        assert registered_kwargs["name"] == f"{TASK_JOB_PREFIX}.{_AsyncJob.__qualname__}"

    @pytest.mark.parametrize(
        "option",
        [
            pytest.param("acks_late", id="acks_late_is_true"),
            pytest.param("acks_on_failure_or_timeout", id="acks_on_failure_or_timeout_is_true"),
            pytest.param("reject_on_worker_lost", id="reject_on_worker_lost_is_true"),
        ],
    )
    def test_task_option_is_true(self, option: str) -> None:
        app = MagicMock()
        registered_kwargs: dict[str, object] = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _SyncJob, MagicMock(), MagicMock(), _mock_runtime(), settings=_SETTINGS)
        assert registered_kwargs[option] is True

    def test_the_task_leaves_the_attempt_limit_to_the_runner(self) -> None:
        app = MagicMock()
        registered_kwargs: dict[str, object] = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(app, _SyncJob, MagicMock(), MagicMock(), _mock_runtime(), settings=_SETTINGS)
        assert registered_kwargs["max_retries"] is None

    def test_soft_time_limit_uses_job_classvar(self) -> None:
        app = MagicMock()
        registered_kwargs: dict[str, object] = {}
        app.task = MagicMock(
            side_effect=lambda **kw: registered_kwargs.update(kw) or (lambda fn: fn)
        )
        _make_job_task(
            app, _AsyncJob, MagicMock(), MagicMock(), _mock_runtime(), settings=_SETTINGS
        )
        assert registered_kwargs["soft_time_limit"] == _AsyncJob.__timeout__


# ---------------------------------------------------------------------------
# _make_job_task — execution paths
# ---------------------------------------------------------------------------


def _mock_self(retries: int = 0, max_retries: int = 0) -> MagicMock:
    """Return a mock representing Celery's bound task self."""
    ms = MagicMock()
    ms.request.retries = retries
    ms.request.is_eager = False
    ms.max_retries = max_retries
    ms.app.conf.task_always_eager = False
    return ms


class TestMakeJobTaskExecution:
    def test_job_task_calls_async_runtime_run(self) -> None:
        """Every job (sync or async) is submitted to the async runtime."""
        runtime = _executing_runtime()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(10),
            runtime,
            settings=_SETTINGS,
        )
        task_fn(_mock_self(), payload={"value": 5})
        runtime.run.assert_called_once()
        _, kwargs = runtime.run.call_args
        assert kwargs["eager_fallback"] is False

    def test_job_task_hands_the_executor_every_input_of_the_envelope(self) -> None:
        """A8: the task hands the job type and its factory to the executor, which builds it."""
        factory = _mock_factory(_SyncJob())
        executor = _executor_returning(7)
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            factory,
            executor,
            _executing_runtime(),
            settings=_SETTINGS,
        )

        assert task_fn(_mock_self(), payload={"value": 5}, params={"p": 2}) == 7

        executor.run.assert_awaited_once()
        args, kwargs = executor.run.await_args
        assert args == (_SyncJob,)
        assert {key: kwargs[key] for key in ("factory", "params", "payload", "identity")} == {
            "factory": factory,
            "params": {"p": 2},
            "payload": {"value": 5},
            "identity": None,
        }

    def test_job_task_emits_observability_span(self) -> None:
        observability_runtime, span_calls = _mock_observability_runtime()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(10),
            _executing_runtime(),
            observability_runtime=observability_runtime,
            settings=_SETTINGS,
        )
        task_fn(_mock_self(), payload={"value": 5})

        observability_runtime.span.assert_called_once()
        args, kwargs = observability_runtime.span.call_args
        assert args[0] is Scope.JOB
        assert args[1] == _SyncJob.__qualname__
        assert kwargs["trace_id"] is None
        assert span_calls

    def test_the_job_reads_its_delivery_with_the_attempt_and_the_policy(self) -> None:
        seen: list[JobDelivery | None] = []

        async def run(*_: object, delivery: JobDelivery | None, **__: object) -> int:
            seen.append(delivery)
            return 1

        task_self = _mock_self(retries=1)
        task_self.request.id = "task-9"
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            MagicMock(run=run),
            _executing_runtime(),
            settings=_SETTINGS,
        )
        task_fn(task_self, deferrals=0)

        [delivery] = seen
        assert delivery is not None
        assert (delivery.task_id, delivery.attempt, delivery.deferrals) == ("task-9", 2, 0)
        assert delivery.policy.backoff == _SyncJob.__retry__

    def test_the_runner_gets_the_soft_time_limit_as_the_run_timeout(self) -> None:
        seen: list[float | None] = []

        async def run(*_: object, delivery: JobDelivery | None, **__: object) -> str:
            seen.append(None if delivery is None else delivery.timeout_s)
            return "ok"

        task_fn = _make_job_task(
            _mock_celery_app(),
            _AsyncJob,
            _mock_factory(MagicMock()),
            MagicMock(run=run),
            _executing_runtime(),
            settings=_SETTINGS,
        )
        task_fn(_mock_self(), payload={"msg": "hello"})
        assert seen == [pytest.approx(300.0)]

    def test_task_returns_the_result_of_the_job(self) -> None:
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(42),
            _executing_runtime(),
            settings=_SETTINGS,
        )
        assert task_fn(_mock_self(), payload={"value": 4}) == 42

    def test_job_task_marks_eager_fallback_for_eager_requests(self) -> None:
        runtime = _executing_runtime()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(10),
            runtime,
            settings=_SETTINGS,
        )
        task_self = _mock_self()
        task_self.request.is_eager = True

        result = task_fn(task_self, payload={"value": 5})
        assert result == 10
        runtime.run.assert_called_once()
        _, kwargs = runtime.run.call_args
        assert kwargs["eager_fallback"] is True


# ---------------------------------------------------------------------------
# _make_job_task — settlement of the runner's outcome
# ---------------------------------------------------------------------------


def _failing_task(
    error: Exception,
    *,
    metrics: MagicMock | None = None,
    settings: JobSettings = _SETTINGS,
) -> Any:
    return _make_job_task(
        _mock_celery_app(),
        _SyncJob,
        _mock_factory(MagicMock()),
        _executor_raising(error),
        _executing_runtime(),
        metrics,
        settings=settings,
    )


def _retrying_self(retries: int = 0) -> MagicMock:
    task_self = _mock_self(retries=retries)
    task_self.retry = MagicMock(side_effect=_RetryError)
    task_self.request.kwargs = {"payload": {"value": 1}}
    return task_self


class TestMakeJobTaskRetry:
    def test_a_failure_with_attempts_left_retries_after_the_backoff(self) -> None:
        task_self = _retrying_self(retries=0)
        with pytest.raises(_RetryError):
            _failing_task(ValueError("err"))(task_self, payload={"value": 1})
        task_self.retry.assert_called_once()
        assert task_self.retry.call_args.kwargs["countdown"] == 3

    def test_post_commit_error_is_not_retried(self) -> None:
        """US1 s3: the transaction committed, so a retry would run the job twice."""
        metrics = MagicMock()
        task_self = _retrying_self()
        error = PostCommitError(committed=True, failures=(ConnectionError("broker down"),))

        with pytest.raises(PostCommitError):
            _failing_task(error, metrics=metrics)(task_self, payload={"value": 1})

        task_self.retry.assert_not_called()
        kinds = [c.args[0].kind for c in metrics.on_event.call_args_list]
        assert EventKind.JOB_FAILED not in kinds
        assert EventKind.JOB_REJECTED in kinds

    def test_post_commit_error_without_a_commit_is_retried(self) -> None:
        """A1: nothing committed, so re-running the job cannot duplicate a write."""
        task_self = _retrying_self()
        error = PostCommitError(committed=False, failures=(ConnectionError("broker down"),))

        with pytest.raises(_RetryError):
            _failing_task(error)(task_self, payload={"value": 1})

        task_self.retry.assert_called_once()

    @pytest.mark.parametrize(
        "error",
        [
            StoresNotCommitted("audit=Committed, default=RolledBack"),
            ScopeOwnershipError("the invocation scope is already closed"),
            UnitOfWorkInsideScope.entering(object()),
            invocation_error(InvocationOutcome((Uncertain(DEFAULT_STORE, OSError("ack")),)), None),
        ],
        ids=["stores-not-committed", "scope-ownership", "uow-inside-scope", "one-store-uncertain"],
    )
    def test_a_non_retryable_invocation_error_is_not_retried(self, error: Exception) -> None:
        """A store already committed, or the job has a programming error: a retry cannot help."""
        task_self = _retrying_self()
        with pytest.raises(type(error)):
            _failing_task(error)(task_self, payload={"value": 1})

        task_self.retry.assert_not_called()

    def test_retry_countdown_grows_with_the_attempt(self) -> None:
        task_self = _retrying_self(retries=2)
        with pytest.raises(_RetryError):
            _failing_task(ValueError("err"), settings=_settings_with_attempts(5))(
                task_self, payload={"value": 1}
            )
        assert task_self.retry.call_args.kwargs["countdown"] == 27

    def test_a_job_retry_defers_without_spending_an_attempt(self) -> None:
        task_self = _retrying_self(retries=2)
        with pytest.raises(_RetryError):
            _failing_task(JobRetry("not yet", 7))(task_self, payload={"value": 1}, deferrals=2)
        kwargs = task_self.retry.call_args.kwargs
        assert kwargs["countdown"] == 7
        assert kwargs["kwargs"] == {"payload": {"value": 1}, "deferrals": 3}

    def test_deferrals_do_not_exhaust_the_attempts(self) -> None:
        task_self = _retrying_self(retries=5)
        with pytest.raises(_RetryError):
            _failing_task(ValueError("err"))(task_self, payload={"value": 1}, deferrals=5)
        task_self.retry.assert_called_once()

    def test_a_job_rejection_raises_without_retrying(self) -> None:
        task_self = _retrying_self()
        with pytest.raises(JobRejection, match="never"):
            _failing_task(JobRejection("never"))(task_self, payload={"value": 1})
        task_self.retry.assert_not_called()

    def test_a_deferral_of_a_request_without_kwargs_carries_only_its_deferrals(self) -> None:
        task_self = _retrying_self(retries=0)
        task_self.request.kwargs = None
        with pytest.raises(_RetryError):
            _failing_task(JobRetry("not yet", 7))(task_self)
        assert task_self.retry.call_args.kwargs["kwargs"] == {"deferrals": 1}

    def test_a_deferral_carries_the_signal_that_asked_for_it(self) -> None:
        task_self = _retrying_self()
        with pytest.raises(_RetryError):
            _failing_task(JobRetry("not yet", 7))(task_self, payload={"value": 1})
        assert isinstance(task_self.retry.call_args.kwargs["exc"], JobRetry)

    def test_job_started_event_emitted_before_execution(self) -> None:
        """JOB_STARTED must arrive before the job runs."""
        metrics = MagicMock()
        order: list[str] = []
        metrics.on_event.side_effect = lambda event: order.append(event.kind)

        async def run(*_: object, **__: object) -> int:
            order.append("executed")
            return 0

        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            MagicMock(run=run),
            _executing_runtime(),
            metrics,
            settings=_SETTINGS,
        )
        task_fn(_mock_self())
        assert order[:2] == [EventKind.JOB_STARTED, "executed"]

    def test_job_succeeded_event_emitted_on_success_with_a_duration(self) -> None:
        metrics = MagicMock()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(0),
            _executing_runtime(),
            metrics,
            settings=_SETTINGS,
        )
        task_fn(_mock_self())
        succeeded = next(
            c.args[0]
            for c in metrics.on_event.call_args_list
            if c.args[0].kind == EventKind.JOB_SUCCEEDED
        )
        assert succeeded.duration_ms is not None
        assert succeeded.duration_ms >= 0

    def test_job_started_event_carries_trace_id(self) -> None:
        metrics = MagicMock()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(0),
            _executing_runtime(),
            metrics,
            settings=_SETTINGS,
        )
        task_fn(_mock_self(), trace_id="tid-999")
        started = next(
            c.args[0]
            for c in metrics.on_event.call_args_list
            if c.args[0].kind == EventKind.JOB_STARTED
        )
        assert started.trace_id == "tid-999"

    def test_the_worker_job_span_lands_in_the_trace_the_dispatcher_sent(self) -> None:
        """Criterion: a job dispatched with a trace id is traced under it.

        No payload-format change is involved — ``trace_id`` already crosses the
        broker as a task kwarg. The worker installs it in the contextvar, and
        the id generator turns it into the OTEL trace id of the root JOB span.
        """
        recorder = build_recorder()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(0),
            _executing_runtime(),
            None,
            recorder.runtime,
            settings=_SETTINGS,
        )

        task_fn(_mock_self(), trace_id=_WIRE_TRACE)

        span = recorder.one("job:_SyncJob")
        assert hex_trace(span) == _WIRE_TRACE
        assert span.parent is None, (
            "no remote parent span id crosses the broker, so the worker span "
            "shares the trace but has no parent edge"
        )

    def test_a_job_without_a_trace_id_still_gets_a_span_on_a_random_trace(self) -> None:
        recorder = build_recorder()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(0),
            _executing_runtime(),
            None,
            recorder.runtime,
            settings=_SETTINGS,
        )

        task_fn(_mock_self())

        assert hex_trace(recorder.one("job:_SyncJob")) != _WIRE_TRACE

    def test_a_trace_less_job_installs_no_context_and_emits_events_with_none(self) -> None:
        """A task dispatched outside a request must not mint a trace id here.

        ``trace_id`` reaches the worker only when the job came from an API
        request.  Without one, nothing is installed in the context and every
        event carries ``trace_id=None``: the span still gets its own id
        downstream, but that id must never leak back into the events.
        """
        seen: list[str | None] = []
        metrics = MagicMock()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _factory_recording_trace(_SyncJob(), seen),
            _building_executor(),
            _executing_runtime(),
            metrics,
            settings=_SETTINGS,
        )

        task_fn(_mock_self())

        assert seen == [None], "no trace context may be installed for a trace-less job"
        events = [c.args[0] for c in metrics.on_event.call_args_list]
        assert [e.kind for e in events] == [EventKind.JOB_STARTED, EventKind.JOB_SUCCEEDED]
        assert all(e.trace_id is None for e in events)

    @pytest.mark.parametrize(
        ("retries", "expected_error", "expected_kind"),
        [
            (0, _RetryError, EventKind.JOB_FAILED),
            (2, ValueError, EventKind.JOB_REJECTED),
        ],
    )
    def test_failure_events_of_a_trace_less_job_also_carry_none(
        self, retries: int, expected_error: type[Exception], expected_kind: EventKind
    ) -> None:
        metrics = MagicMock()
        task_self = _retrying_self(retries=retries)
        with pytest.raises(expected_error):
            _failing_task(ValueError("err"), metrics=metrics)(task_self, payload={"value": 1})

        events = [c.args[0] for c in metrics.on_event.call_args_list]
        assert expected_kind in [e.kind for e in events]
        assert all(e.trace_id is None for e in events)

    def test_a_job_with_a_trace_id_installs_it_and_stamps_every_event(self) -> None:
        seen: list[str | None] = []
        metrics = MagicMock()
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _factory_recording_trace(_SyncJob(), seen),
            _building_executor(),
            _executing_runtime(),
            metrics,
            settings=_SETTINGS,
        )

        task_fn(_mock_self(), trace_id=_WIRE_TRACE)

        assert seen == [_WIRE_TRACE]
        events = [c.args[0] for c in metrics.on_event.call_args_list]
        assert all(e.trace_id == _WIRE_TRACE for e in events)
        assert get_trace_id() is None, "the trace context is restored when the task returns"

    def test_no_metrics_does_not_raise(self) -> None:
        task_fn = _make_job_task(
            _mock_celery_app(),
            _SyncJob,
            _mock_factory(_SyncJob()),
            _executor_returning(0),
            _executing_runtime(),
            metrics=None,
            settings=_SETTINGS,
        )
        task_fn(_mock_self())

    def test_raises_original_exc_when_attempts_are_exhausted(self) -> None:
        task_self = _retrying_self(retries=2)
        with pytest.raises(ValueError, match="final"):
            _failing_task(ValueError("final"))(task_self, payload={"value": 1})
        task_self.retry.assert_not_called()

    def test_job_failed_event_emitted_when_an_attempt_remains(self) -> None:
        metrics = MagicMock()
        with pytest.raises(_RetryError):
            _failing_task(ValueError("boom"), metrics=metrics)(_retrying_self())
        kinds = [c.args[0].kind for c in metrics.on_event.call_args_list]
        assert EventKind.JOB_FAILED in kinds

    def test_job_rejected_event_emitted_when_attempts_are_exhausted(self) -> None:
        metrics = MagicMock()
        with pytest.raises(ValueError):
            _failing_task(ValueError("final"), metrics=metrics)(_retrying_self(retries=2))
        kinds = [c.args[0].kind for c in metrics.on_event.call_args_list]
        assert EventKind.JOB_REJECTED in kinds


# ---------------------------------------------------------------------------
# _resolve_error_info
# ---------------------------------------------------------------------------


class TestResolveErrorInfo:
    def test_looks_up_result_on_the_worker_app(self) -> None:
        app = Celery("worker", set_as_current=False)
        mock_result = MagicMock()
        mock_result.result = ValueError("bad")
        with patch("loom.celery.runner.AsyncResult", return_value=mock_result) as result:
            assert _resolve_error_info("some-task-id", app) == {
                "exc_type": "ValueError",
                "exc_msg": "bad",
            }
        result.assert_called_once_with("some-task-id", app=app)

    def test_returns_unknown_when_result_is_not_exception(self) -> None:
        mock_result = MagicMock()
        mock_result.result = None
        with patch("loom.celery.runner.AsyncResult", return_value=mock_result):
            details = _resolve_error_info("some-task-id", Celery(set_as_current=False))
        assert details == {"exc_type": "Unknown", "exc_msg": ""}


# ---------------------------------------------------------------------------
# _make_callback_task
# ---------------------------------------------------------------------------

_BACKEND_ERROR = {"exc_type": "ValueError", "exc_msg": "bad input"}

OUTCOMES = pytest.mark.parametrize(
    ("outcome", "job_result", "details"),
    [
        (CallbackOutcome.SUCCESS, ("acme",), {"result": "acme"}),
        (CallbackOutcome.FAILURE, (), _BACKEND_ERROR),
    ],
    ids=["success", "failure"],
)
"""Each outcome: what Celery passes positionally, and the hook's details."""


@pytest.fixture
def backend_error() -> Generator[None]:
    """The result backend holds the failed job's exception."""
    with patch("loom.celery.runner._resolve_error_info", return_value=_BACKEND_ERROR):
        yield


@pytest.fixture
def celery_app() -> Celery:
    return Celery("acme", set_as_current=False)


class _CoroutineFromDef:
    """A callback whose plain ``def`` hooks return a coroutine."""

    def on_success(self, job_id: str, result: object, **ctx: object) -> Coroutine[Any, Any, None]:
        return asyncio.sleep(0)

    def on_failure(
        self, job_id: str, exc_type: str, exc_msg: str, **ctx: object
    ) -> Coroutine[Any, Any, None]:
        return asyncio.sleep(0)


@pytest.mark.usefixtures("backend_error")
@OUTCOMES
class TestMakeCallbackTask:
    def test_task_registered_under_its_callback_task_name(
        self, outcome: CallbackOutcome, job_result: tuple[Any, ...], details: Any
    ) -> None:
        app = MagicMock()
        registered_name: list[str] = []

        def _register(**kw: object) -> object:
            registered_name.append(str(kw["name"]))
            return lambda fn: fn

        app.task = MagicMock(side_effect=_register)
        _make_callback_task(app, _SyncCallback, outcome, MagicMock(), _mock_runtime())
        assert registered_name == [callback_task_name(_SyncCallback, outcome)]

    def test_sync_hook_called_with_job_id_details_and_context(
        self,
        celery_app: Celery,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
        details: Any,
    ) -> None:
        cb = MagicMock(spec=_SyncCallback)
        outcome.hook(cb).return_value = None
        task = _make_callback_task(
            celery_app, _SyncCallback, outcome, _mock_factory(cb), _mock_runtime()
        )
        task.apply(args=job_result, kwargs={"job_id": "uuid-1", "context": {"order_id": 99}}).get()
        outcome.hook(cb).assert_called_once_with(job_id="uuid-1", **details, order_id=99)

    def test_a_plain_def_hook_returning_an_awaitable_is_refused_and_closed(
        self,
        celery_app: Celery,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
        details: Any,
    ) -> None:
        task = _make_callback_task(
            celery_app,
            _CoroutineFromDef,
            outcome,
            _mock_factory(_CoroutineFromDef()),
            _mock_runtime(),
        )

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            result = task.apply(args=job_result, kwargs={"job_id": "j", "context": {}})
            with pytest.raises(TypeError, match="async def"):
                result.get()

    @pytest.mark.parametrize("eager", [False, True], ids=["worker", "eager"])
    def test_async_hook_runs_on_the_runtime_eagerly_only_for_an_eager_request(
        self,
        celery_app: Celery,
        outcome: CallbackOutcome,
        job_result: tuple[Any, ...],
        details: Any,
        eager: bool,
    ) -> None:
        cb = MagicMock()
        setattr(cb, f"on_{outcome}", AsyncMock())
        runtime = _mock_runtime()
        task = _make_callback_task(celery_app, _AsyncCallback, outcome, _mock_factory(cb), runtime)
        kwargs = {"job_id": "x", "context": {}}
        if eager:
            task.apply(args=job_result, kwargs=kwargs).get()
        else:
            task(*job_result, **kwargs)
        runtime.run.assert_called_once()
        assert runtime.run.call_args.kwargs["eager_fallback"] is eager


# ---------------------------------------------------------------------------
# Two Celery applications in one process
# ---------------------------------------------------------------------------


class _TwinJob(Job[int]):
    """Registered on two applications under the same task name."""

    def execute(self) -> int:
        return 0


class _TwinCallback:
    def on_success(self, job_id: str, result: object, **ctx: object) -> None:
        pass  # intentional no-op stub

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: object) -> None:
        pass  # intentional no-op stub


def _two_apps() -> tuple[Celery, Celery]:
    """Two applications, neither finalized: both register before either reads its tasks."""
    return (
        Celery("first", set_as_current=False),
        Celery("second", set_as_current=False),
    )


class TestTwoApplicationsInOneProcess:
    """Each application serves its own task, never one registered for another.

    A task shared through Celery's process-wide finalizers is registered on
    every application finalized afterwards, and the first one under a name
    wins: both applications would then run the same closure, bound to one
    runtime. Both register before either is finalized, so a shared task
    always lands on the wrong application for one of them.
    """

    def test_each_job_task_runs_against_its_own_runtime(self) -> None:
        first, second = _two_apps()
        for app, answer in ((first, 1), (second, 2)):
            _make_job_task(
                app,
                _TwinJob,
                _mock_factory(_TwinJob()),
                _executor_returning(answer),
                _executing_runtime(),
                settings=_SETTINGS,
            )
        name = f"{TASK_JOB_PREFIX}.{_TwinJob.__qualname__}"

        assert first.tasks[name] is not second.tasks[name]
        assert first.tasks[name].apply().get() == 1
        assert second.tasks[name].apply().get() == 2

    @pytest.mark.usefixtures("backend_error")
    @OUTCOMES
    def test_each_callback_builds_from_its_own_factory(
        self, outcome: CallbackOutcome, job_result: tuple[Any, ...], details: Any
    ) -> None:
        first, second = _two_apps()
        factories = (_mock_factory(MagicMock()), _mock_factory(MagicMock()))
        tasks = [
            _make_callback_task(app, _TwinCallback, outcome, factory, _mock_runtime())
            for app, factory in zip((first, second), factories, strict=True)
        ]

        assert tasks[0] is not tasks[1]
        for task, factory in zip(tasks, factories, strict=True):
            task.apply(args=job_result, kwargs={"job_id": "j", "context": {}}).get()

            factory.build.assert_called_once_with(_TwinCallback)


class _SyncSeen:
    seen: list[tuple[str, object]] = []

    def on_success(self, job_id: str, result: object, **context: object) -> None:
        self.seen.append((job_id, result))

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: object) -> None:
        raise AssertionError


class _AsyncSeen(_SyncSeen):
    async def on_success(self, job_id: str, result: object, **context: object) -> None:  # type: ignore[override]
        self.seen.append((job_id, result))


class _Builds:
    def build(self, callback_type: type[Any]) -> Any:
        return callback_type()


class TestLinkContextOnTheWorker:
    @pytest.mark.parametrize("callback", [_SyncSeen, _AsyncSeen], ids=["sync", "async"])
    def test_a_link_context_cannot_replace_the_reserved_arguments(
        self, callback: type[_SyncSeen]
    ) -> None:
        app = Celery("acme")
        app.conf.task_always_eager = True
        callback.seen = []
        task = _make_callback_task(
            app, callback, CallbackOutcome.SUCCESS, _Builds(), _CeleryAsyncRuntime()
        )

        task.apply(
            args=("acme-result",),
            kwargs={"job_id": "acme-job", "context": {"job_id": "forged", "result": "forged"}},
        ).get()

        assert callback.seen == [("acme-job", "acme-result")]
