"""What every transport that sends a message does with a dispatch, over the same jobs."""

from __future__ import annotations

import pytest

from loom.core.identity import Identity, reset_identity, set_identity
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.job.policy import Countdown
from loom.core.tracing import reset_trace_id, set_trace_id
from tests.contract.jobs.conftest import DelayedJob, NotifyJob, ReportJob, Wire


class TestAfterCommit:
    async def test_nothing_leaves_before_the_commit(self, wire: Wire) -> None:
        wire.service.dispatch_after_commit(NotifyJob)

        assert wire.sent() == []

    async def test_a_committed_dispatch_leaves_once(self, wire: Wire) -> None:
        wire.service.dispatch_after_commit(NotifyJob)
        await flush_pending_dispatches()
        await flush_pending_dispatches()

        assert [sent.job_type for sent in wire.sent()] == [NotifyJob]

    async def test_a_rolled_back_dispatch_leaves_nothing(self, wire: Wire) -> None:
        wire.service.dispatch_after_commit(NotifyJob)
        clear_pending_dispatches()
        await flush_pending_dispatches()

        assert wire.sent() == []

    async def test_jobs_leave_in_dispatch_order(self, wire: Wire) -> None:
        handles = [
            wire.service.dispatch_after_commit(NotifyJob, payload={"n": n}) for n in range(3)
        ]
        await flush_pending_dispatches()

        assert [(sent.task_id, sent.payload) for sent in wire.sent()] == [
            (handle.job_id, {"n": n}) for n, handle in enumerate(handles)
        ]


class TestMessage:
    async def test_the_message_carries_the_id_and_queue_of_the_handle(self, wire: Wire) -> None:
        handle = wire.service.dispatch_after_commit(NotifyJob)
        await flush_pending_dispatches()

        (sent,) = wire.sent()
        assert (sent.task_id, sent.queue) == (handle.job_id, handle.queue)

    async def test_every_dispatch_has_its_own_id(self, wire: Wire) -> None:
        first = wire.service.dispatch_after_commit(NotifyJob)
        second = wire.service.dispatch_after_commit(NotifyJob)

        assert first.job_id != second.job_id

    async def test_the_message_carries_payload_params_and_trace(self, wire: Wire) -> None:
        token = set_trace_id("trace-1")
        try:
            wire.service.dispatch_after_commit(
                NotifyJob, payload={"to": "a@b.c"}, params={"user_id": 7}
            )
        finally:
            reset_trace_id(token)
        await flush_pending_dispatches()

        (sent,) = wire.sent()
        assert (sent.payload, sent.params, sent.trace_id) == (
            {"to": "a@b.c"},
            {"user_id": 7},
            "trace-1",
        )

    async def test_the_message_carries_the_caller_of_the_dispatch(self, wire: Wire) -> None:
        token = set_identity(Identity(subject="alice"))
        try:
            wire.service.dispatch_after_commit(NotifyJob)
        finally:
            reset_identity(token)
        await flush_pending_dispatches()

        (sent,) = wire.sent()
        assert sent.identity is not None
        assert sent.identity.subject == "alice"

    async def test_a_dispatch_without_a_caller_carries_none(self, wire: Wire) -> None:
        wire.service.dispatch_after_commit(NotifyJob)
        await flush_pending_dispatches()

        assert [sent.identity for sent in wire.sent()] == [None]

    @pytest.mark.parametrize(
        ("job", "queue", "expected"),
        [
            (NotifyJob, None, "emails"),
            (NotifyJob, "reports", "reports"),
            (ReportJob, None, "reports"),
        ],
        ids=["job queue", "override", "other job queue"],
    )
    async def test_the_queue_is_the_one_of_the_job_unless_overridden(
        self, wire: Wire, job: type[NotifyJob] | type[ReportJob], queue: str | None, expected: str
    ) -> None:
        wire.service.dispatch_after_commit(job, queue=queue)
        await flush_pending_dispatches()

        assert [sent.queue for sent in wire.sent()] == [expected]


class TestCountdown:
    @pytest.mark.parametrize(
        ("job", "given", "expected"),
        [(NotifyJob, None, 0), (DelayedJob, None, 12), (DelayedJob, 4, 4), (NotifyJob, 900, 900)],
        ids=["none", "class countdown", "call countdown", "bound"],
    )
    async def test_the_message_waits_the_effective_countdown(
        self, wire: Wire, job: type[NotifyJob] | type[DelayedJob], given: int | None, expected: int
    ) -> None:
        wire.service.dispatch_after_commit(job, countdown=given)
        await flush_pending_dispatches()

        assert [sent.countdown for sent in wire.sent()] == [expected]

    @pytest.mark.parametrize("seconds", [-1, Countdown.MAX_SECONDS + 1])
    async def test_a_countdown_outside_the_bound_is_refused_and_sends_nothing(
        self, wire: Wire, seconds: int
    ) -> None:
        with pytest.raises(ValueError, match=str(Countdown.MAX_SECONDS)):
            wire.service.dispatch_after_commit(NotifyJob, countdown=seconds)
        with pytest.raises(ValueError, match=str(Countdown.MAX_SECONDS)):
            await wire.service.dispatch_now(NotifyJob, countdown=seconds)
        await flush_pending_dispatches()

        assert wire.sent() == []


class TestDispatchNow:
    async def test_the_message_leaves_at_once_and_is_not_sent_again(self, wire: Wire) -> None:
        handle = await wire.service.dispatch_now(NotifyJob, payload={"n": 1})
        sent_before_flush = wire.sent()
        await flush_pending_dispatches()

        assert [(s.task_id, s.payload) for s in sent_before_flush] == [(handle.job_id, {"n": 1})]
        assert wire.sent() == sent_before_flush


class TestParallel:
    async def test_each_job_leaves_with_a_handle_in_input_order(self, wire: Wire) -> None:
        group = wire.service.dispatch_parallel([(NotifyJob, {"a": 1}), (NotifyJob, {"b": 2})])
        assert wire.sent() == []
        await flush_pending_dispatches()

        assert [(s.task_id, s.payload) for s in wire.sent()] == [
            (handle.job_id, body)
            for handle, body in zip(group.handles, ({"a": 1}, {"b": 2}), strict=True)
        ]
