"""Unit tests for the Celery job transport."""

from __future__ import annotations

from collections.abc import Generator
from unittest.mock import MagicMock

import pytest

from loom.celery.constants import TASK_JOB_PREFIX
from loom.celery.service import CallbackLink
from loom.core.engine.events import EventKind
from loom.core.job.callback import DispatchableCallbacks, JobCallback
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.handle import JobGroup, JobHandle
from loom.core.job.job import Job
from loom.core.observability.event import Scope
from loom.core.tracing import reset_trace_id, set_trace_id
from tests.helpers.jobs import CeleryServiceFixture

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _EmailJob(Job[None]):
    __queue__ = "email"
    __countdown__ = 0
    __timeout__ = None
    __priority__ = 0

    def execute(self) -> None:
        # Intentional no-op: the test only needs a concrete Job subclass.
        return None


class _HeavyJob(Job[int]):
    __queue__ = "heavy"
    __countdown__ = 10
    __timeout__ = 300
    __priority__ = 5

    def execute(self) -> int:
        return 42


class _SuccessCallback(JobCallback):
    pass


class _FailureCallback(JobCallback):
    pass


def _make_service() -> tuple[CeleryServiceFixture, MagicMock]:
    mock_app = MagicMock()
    service = CeleryServiceFixture(
        mock_app, callbacks=DispatchableCallbacks([_SuccessCallback, _FailureCallback])
    )
    return service, mock_app


def _make_runtime() -> MagicMock:
    runtime = MagicMock()
    runtime.span.return_value.__enter__.return_value = None
    runtime.span.return_value.__exit__.return_value = None
    return runtime


@pytest.fixture(autouse=True)
def _clean_pending() -> Generator[None, None, None]:
    """Isolate the pending-dispatch ContextVar for every test.

    Runs before and after each test (sync or async) to prevent leakage
    between tests that dispatch without flushing.
    """
    clear_pending_dispatches()
    yield
    clear_pending_dispatches()


# ---------------------------------------------------------------------------
# dispatch_after_commit() — deferred send
# ---------------------------------------------------------------------------


class TestDispatchDeferred:
    def test_dispatch_does_not_call_send_task_immediately(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob, payload={"email": "a@b.com"})
        mock_app.send_task.assert_not_called()

    async def test_send_task_called_after_flush(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob, payload={"email": "a@b.com"})
        await flush_pending_dispatches()
        mock_app.send_task.assert_called_once()

    async def test_send_task_emits_transport_span(self) -> None:
        runtime = _make_runtime()
        service = CeleryServiceFixture(MagicMock(), observability_runtime=runtime)
        service.dispatch_after_commit(_EmailJob, payload={"email": "a@b.com"})
        await flush_pending_dispatches()
        runtime.span.assert_called_once()
        args, kwargs = runtime.span.call_args
        assert args[0] is Scope.TRANSPORT
        assert args[1] == f"{TASK_JOB_PREFIX}.{_EmailJob.__qualname__}"
        assert kwargs["trace_id"] is None

    async def test_send_task_receives_correct_task_name(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.args[0] == f"{TASK_JOB_PREFIX}.{_EmailJob.__qualname__}"

    async def test_send_task_receives_payload_in_kwargs(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob, payload={"email": "x@y.com"})
        await flush_pending_dispatches()
        sent_kwargs = mock_app.send_task.call_args.kwargs["kwargs"]
        assert sent_kwargs["payload"] == {"email": "x@y.com"}

    async def test_send_task_receives_params_in_kwargs(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob, params={"user_id": 7})
        await flush_pending_dispatches()
        sent_kwargs = mock_app.send_task.call_args.kwargs["kwargs"]
        assert sent_kwargs["params"] == {"user_id": 7}


# ---------------------------------------------------------------------------
# dispatch_after_commit() — JobHandle identity
# ---------------------------------------------------------------------------


class TestDispatchHandleIdentity:
    async def test_handle_job_id_matches_send_task_task_id(self) -> None:
        service, mock_app = _make_service()
        handle = service.dispatch_after_commit(_EmailJob)
        await flush_pending_dispatches()
        sent_task_id = mock_app.send_task.call_args.kwargs["task_id"]
        assert handle.job_id == sent_task_id

    def test_handle_queue_uses_job_classvar_by_default(self) -> None:
        service, _ = _make_service()
        handle = service.dispatch_after_commit(_EmailJob)
        assert handle.queue == "email"

    async def test_handle_queue_respects_override(self) -> None:
        service, mock_app = _make_service()
        handle = service.dispatch_after_commit(_EmailJob, queue="priority")
        assert handle.queue == "priority"
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["queue"] == "priority"

    def test_handle_is_jobhandle_instance(self) -> None:
        service, _ = _make_service()
        handle = service.dispatch_after_commit(_EmailJob)
        assert isinstance(handle, JobHandle)

    def test_each_dispatch_generates_unique_job_id(self) -> None:
        service, _ = _make_service()
        h1 = service.dispatch_after_commit(_EmailJob)
        h2 = service.dispatch_after_commit(_EmailJob)
        assert h1.job_id != h2.job_id


# ---------------------------------------------------------------------------
# dispatch_after_commit() — routing ClassVar defaults
# ---------------------------------------------------------------------------


class TestDispatchRoutingDefaults:
    @pytest.mark.parametrize(
        ("expected", "option"),
        [
            pytest.param(10, "countdown", id="countdown_uses_job_classvar"),
            pytest.param(5, "priority", id="priority_uses_job_classvar"),
            pytest.param(300, "soft_time_limit", id="soft_time_limit_uses_job_classvar"),
        ],
    )
    async def test_routing_option_uses_job_classvar(self, expected: int, option: str) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_HeavyJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs[option] == expected

    async def test_countdown_override_takes_precedence(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_HeavyJob, countdown=0)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["countdown"] == 0

    async def test_priority_override_takes_precedence(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_HeavyJob, priority=1)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["priority"] == 1

    async def test_soft_time_limit_none_when_no_timeout(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["soft_time_limit"] is None


# ---------------------------------------------------------------------------
# dispatch_after_commit() — callbacks (link / failure callback)
# ---------------------------------------------------------------------------


class TestDispatchCallbacks:
    async def test_no_callbacks_sends_no_link_and_no_failure_callback(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["link"] is None
        assert mock_app.send_task.call_args.kwargs["kwargs"]["failure_callback"] is None

    async def test_on_success_produces_non_none_link(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob, on_success=_SuccessCallback)
        await flush_pending_dispatches()
        assert mock_app.send_task.call_args.kwargs["link"] is not None

    async def test_on_failure_carries_the_callback_task_name_and_no_link_error(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob, on_failure=_FailureCallback)
        await flush_pending_dispatches()
        sent = mock_app.send_task.call_args.kwargs
        assert sent["kwargs"]["failure_callback"] == "loom.callback_error._FailureCallback"
        assert "link_error" not in sent


class _UndiscoveredCallback(JobCallback):
    pass


class TestUndiscoveredCallback:
    @pytest.mark.parametrize("hook", ["on_success", "on_failure"])
    async def test_dispatch_refuses_a_callback_the_scan_did_not_find(self, hook: str) -> None:
        service, mock_app = _make_service()

        with pytest.raises(
            ValueError, match="_UndiscoveredCallback was not discovered.*app.packages"
        ):
            service.dispatch_after_commit(_EmailJob, **{hook: _UndiscoveredCallback})

        await flush_pending_dispatches()
        mock_app.send_task.assert_not_called()


# ---------------------------------------------------------------------------
# Callback links
# ---------------------------------------------------------------------------


class TestSuccessLink:
    """The success callback is a mutable signature naming its task and carrying the dispatch."""

    def test_names_the_success_task_and_is_mutable(self) -> None:
        mock_app = MagicMock()
        CeleryServiceFixture(
            mock_app, callbacks=DispatchableCallbacks([_SuccessCallback])
        ).dispatch_after_commit(_EmailJob, on_success=_SuccessCallback)

        assert mock_app.signature.call_args.args == ("loom.callback._SuccessCallback",)
        assert "immutable" not in mock_app.signature.call_args.kwargs

    @pytest.mark.parametrize("trace_id", [None, "acme-trace"], ids=["no-trace", "trace"])
    def test_carries_the_job_id_payload_trace_and_no_caller(self, trace_id: str | None) -> None:
        mock_app = MagicMock()
        token = set_trace_id(trace_id)
        try:
            handle = CeleryServiceFixture(
                mock_app, callbacks=DispatchableCallbacks([_SuccessCallback])
            ).dispatch_after_commit(
                _EmailJob, payload={"order_id": 99}, on_success=_SuccessCallback
            )
        finally:
            reset_trace_id(token)

        assert mock_app.signature.call_args.kwargs["kwargs"] == {
            "job_id": handle.job_id,
            "context": {"order_id": 99},
            "trace_id": trace_id,
            "identity": None,
        }


class TestFailureCallbackSend:
    """The worker sends the failure callback with the dispatch's link and the error."""

    def test_sends_the_task_with_the_link_the_class_and_the_message(self) -> None:
        mock_app = MagicMock()
        link = CallbackLink("job-1", {"order_id": 99}, "acme-trace", None)

        link.send_failure(mock_app, "loom.callback_error.Acme", ValueError("bad input"))

        mock_app.send_task.assert_called_once_with(
            "loom.callback_error.Acme",
            kwargs={
                "job_id": "job-1",
                "context": {"order_id": 99},
                "trace_id": "acme-trace",
                "identity": None,
                "exc_type": "ValueError",
                "exc_msg": "bad input",
            },
        )


# ---------------------------------------------------------------------------
# dispatch_parallel()
# ---------------------------------------------------------------------------


class TestDispatchParallel:
    def test_returns_jobgroup(self) -> None:
        service, _ = _make_service()
        group = service.dispatch_parallel([(_EmailJob, {"a": 1}), (_HeavyJob, {"b": 2})])
        assert isinstance(group, JobGroup)

    def test_group_has_one_handle_per_job(self) -> None:
        service, _ = _make_service()
        group = service.dispatch_parallel([(_EmailJob, {}), (_HeavyJob, {})])
        assert len(group.handles) == 2

    async def test_send_task_called_once_per_job_after_flush(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_parallel([(_EmailJob, {}), (_HeavyJob, {})])
        await flush_pending_dispatches()
        assert mock_app.send_task.call_count == 2

    async def test_send_task_called_with_correct_task_names(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_parallel([(_EmailJob, {}), (_HeavyJob, {})])
        await flush_pending_dispatches()
        names = {c.args[0] for c in mock_app.send_task.call_args_list}
        assert names == {
            f"{TASK_JOB_PREFIX}.{_EmailJob.__qualname__}",
            f"{TASK_JOB_PREFIX}.{_HeavyJob.__qualname__}",
        }

    def test_handles_have_unique_job_ids(self) -> None:
        service, _ = _make_service()
        group = service.dispatch_parallel([(_EmailJob, {}), (_EmailJob, {})])
        ids = [h.job_id for h in group.handles]
        assert len(set(ids)) == 2

    def test_does_not_call_send_task_before_flush(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_parallel([(_EmailJob, {}), (_HeavyJob, {})])
        mock_app.send_task.assert_not_called()


# ---------------------------------------------------------------------------
# run() — not supported
# ---------------------------------------------------------------------------


class TestDispatchMetrics:
    def test_dispatch_emits_job_dispatched_event(self) -> None:
        mock_app = MagicMock()
        metrics = MagicMock()
        service = CeleryServiceFixture(mock_app, metrics=metrics)
        service.dispatch_after_commit(_EmailJob)
        assert metrics.on_event.call_count == 1
        event = metrics.on_event.call_args.args[0]
        assert event.kind == EventKind.JOB_DISPATCHED
        assert event.use_case_name == _EmailJob.__qualname__

    def test_dispatch_no_metrics_does_not_raise(self) -> None:
        mock_app = MagicMock()
        service = CeleryServiceFixture(mock_app, metrics=None)
        service.dispatch_after_commit(_EmailJob)  # must not raise

    def test_dispatch_emits_trace_id_in_event(self) -> None:
        mock_app = MagicMock()
        metrics = MagicMock()
        service = CeleryServiceFixture(mock_app, metrics=metrics)
        token = set_trace_id("dispatch-trace")
        try:
            service.dispatch_after_commit(_EmailJob)
        finally:
            reset_trace_id(token)
        event = metrics.on_event.call_args.args[0]
        assert event.trace_id == "dispatch-trace"


class TestRunNotSupported:
    async def test_run_raises_not_implemented(self) -> None:
        service, _ = _make_service()
        with pytest.raises(NotImplementedError):
            await service.run(_EmailJob)

    async def test_run_error_message_mentions_factory(self) -> None:
        service, _ = _make_service()
        with pytest.raises(NotImplementedError, match="factory"):
            await service.run(_EmailJob)


# ---------------------------------------------------------------------------
# Pending queue isolation
# ---------------------------------------------------------------------------


class TestPendingQueueIsolation:
    async def test_clear_prevents_send_task_after_rollback(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob)
        clear_pending_dispatches()
        await flush_pending_dispatches()
        mock_app.send_task.assert_not_called()

    async def test_flush_empties_queue(self) -> None:
        service, mock_app = _make_service()
        service.dispatch_after_commit(_EmailJob)
        await flush_pending_dispatches()
        mock_app.reset_mock()
        await flush_pending_dispatches()
        mock_app.send_task.assert_not_called()
