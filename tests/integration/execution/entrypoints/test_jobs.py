"""The inline job service, an inline dispatch and the job runner build inside the execution.

The job runner is the transport-neutral body the Celery worker and the Lambda
``jobs`` handler share (``loom.core.job.run.EnvelopeInvocation``).
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.post_commit import PostCommitChannel, bind_channel, reset_channel
from loom.core.errors import DelegationExpired, PostCommitError, Unauthenticated
from loom.core.execution import ExecutionScope
from loom.core.identity import ANONYMOUS, Delegation, IdentityFixed, captured_delegation
from loom.core.identity.wire import encode_identity
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.envelope import JobEnvelope, job_context
from loom.core.job.handle import JobHandle
from loom.core.job.run import EnvelopeInvocation
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.use_case.factory import UseCaseFactory
from loom.testing import JobTestData, make
from tests.helpers.celery import CeleryBroker
from tests.helpers.delegation import acting_as, assert_new_delegation
from tests.helpers.jobs import CeleryServiceFixture, InlineServiceFixture

from .doubles import (
    ALICE,
    AS_ALICE,
    BOB,
    NO_CALLER,
    AcmeCallback,
    AcmeProbeJob,
    AcmeRun,
    CallbackLog,
)


@pytest.fixture
def channel() -> Iterator[PostCommitChannel]:
    """The post-commit channel an execution would bind, drained by the test where it chooses."""
    channel = PostCommitChannel()
    token = bind_channel(channel)
    try:
        yield channel
    finally:
        reset_channel(token)


async def run_envelope(
    envelope: JobEnvelope, factory: UseCaseFactory, executor: RuntimeExecutor
) -> Any:
    delivery = JobTestData().with_job_type(AcmeProbeJob).delivery(envelope=envelope)
    return await EnvelopeInvocation(factory, executor, ObservabilityRuntime.noop())(delivery)


class TestInlineJobTransport:
    async def test_run_builds_inside_an_execution_as_the_caller_acting(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        with acting_as(ALICE):
            job = await InlineServiceFixture(factory, executor).run(AcmeProbeJob)

        assert job.session.caller is ALICE
        assert job.session.execution is job.ran_in

    async def test_run_with_nothing_acting_has_no_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        jobs = InlineServiceFixture(factory, executor)

        with pytest.raises(Unauthenticated):
            await jobs.run(AcmeProbeJob)

    async def test_a_dispatch_runs_as_its_dispatcher_in_an_execution_of_its_own(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        jobs = InlineServiceFixture(factory, executor)
        handles: list[JobHandle[AcmeProbeJob]] = []

        async def dispatch() -> None:
            handles.append(jobs.dispatch_after_commit(AcmeProbeJob))

        await executor.execute(AcmeRun(dispatch), identity=ALICE)

        job = handles[0].wait()
        assert job.session.caller is ALICE
        assert job.session.execution is job.ran_in


class TestJobRunner:
    async def test_builds_inside_an_execution_as_the_envelope_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        envelope = make(JobEnvelope, job="AcmeProbeJob", identity=encode_identity(ALICE))

        job = await run_envelope(envelope, factory, executor)

        assert job.session.caller == ALICE
        assert job.session.execution is job.ran_in

    async def test_acts_as_the_envelope_caller_within_its_delegation(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        envelope = make(
            JobEnvelope, job="AcmeProbeJob", identity=Delegation(ALICE, time.time() + 60).encoded()
        )

        job = await run_envelope(envelope, factory, executor)

        assert job.session.caller == ALICE

    @pytest.mark.parametrize(
        "deadline",
        [time.time() - 1, math.inf, math.nan, "soon"],
        ids=["passed", "infinite", "nan", "not-a-number"],
    )
    async def test_refuses_an_envelope_whose_deadline_is_passed_or_unreadable(
        self, factory: UseCaseFactory, executor: RuntimeExecutor, deadline: Any
    ) -> None:
        identity = {**(encode_identity(ALICE) or {}), "until": deadline}
        envelope = make(JobEnvelope, job="AcmeProbeJob", identity=identity)

        with pytest.raises(Unauthenticated, match="system identity"):
            await run_envelope(envelope, factory, executor)


class TestJobContext:
    """The worker resumes the envelope's delegation; a caller without one gets a window."""

    def test_a_caller_with_no_deadline_gets_a_delegation_of_the_window(self, window: float) -> None:
        before = time.time()
        with job_context(None, encode_identity(ALICE)):
            caller, deadline = captured_delegation()

        assert caller == ALICE
        assert_new_delegation(deadline, before, window)

    def test_resumes_the_deadline_the_envelope_carries(self) -> None:
        later = time.time() + 30
        with job_context(None, Delegation(ALICE, later).encoded()):
            assert captured_delegation() == Delegation(ALICE, later)


class TestAQueuedDispatch:
    """Dispatched outside any work, it runs under the delegation captured at dispatch."""

    @pytest.fixture
    def queued(
        self, factory: UseCaseFactory, executor: RuntimeExecutor, window: float
    ) -> JobHandle[Any]:
        """Alice's dispatch, with a callback for each outcome, queued until a flush."""
        with acting_as(ALICE):
            return InlineServiceFixture(factory, executor).dispatch_after_commit(
                AcmeProbeJob, on_success=AcmeCallback, on_failure=AcmeCallback
            )

    @pytest.fixture
    def pending(
        self, factory: UseCaseFactory, executor: RuntimeExecutor, channel: PostCommitChannel
    ) -> PostCommitChannel:
        """Alice's dispatch, kept on a channel the test drains where it chooses."""
        with acting_as(ALICE):
            InlineServiceFixture(factory, executor).dispatch_after_commit(
                AcmeProbeJob, on_success=AcmeCallback
            )
        return channel

    async def test_runs_as_its_dispatcher_within_the_deadline(
        self, queued: JobHandle[Any], callback_log: CallbackLog
    ) -> None:
        await flush_pending_dispatches()

        assert queued.wait().session.caller is ALICE
        assert callback_log == [("built", AS_ALICE), ("on_success", AS_ALICE)]

    async def test_is_refused_past_the_deadline_not_given_a_fresh_window(
        self,
        queued: JobHandle[Any],
        callback_log: CallbackLog,
        past_the_window: Callable[[], None],
    ) -> None:
        """The job's execution is refused, and so is its failure callback: nothing runs."""
        past_the_window()

        with pytest.raises(PostCommitError) as refused:
            await flush_pending_dispatches()

        [failure] = refused.value.failures
        assert isinstance(failure, Unauthenticated)
        assert callback_log == []

    async def test_flushed_under_a_later_delegation_of_its_dispatcher_keeps_the_captured_deadline(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        callback_log: CallbackLog,
    ) -> None:
        """Alice acting again with a fresh window does not extend the dispatch's delegation."""
        with acting_as(ALICE, until=time.time() - 1):
            InlineServiceFixture(factory, executor).dispatch_after_commit(
                AcmeProbeJob, on_success=AcmeCallback, on_failure=AcmeCallback
            )

        with acting_as(ALICE), pytest.raises(PostCommitError) as refused:
            await flush_pending_dispatches()

        [failure] = refused.value.failures
        assert isinstance(failure, Unauthenticated)
        assert callback_log == []

    async def test_flushed_by_a_task_outliving_work_switched_back_to_its_dispatcher_is_refused(
        self, factory: UseCaseFactory, executor: RuntimeExecutor, callback_log: CallbackLog
    ) -> None:
        """Alice's expired dispatch stays expired inside work that started her afresh."""
        with acting_as(ALICE, until=time.time() - 1):
            InlineServiceFixture(factory, executor).dispatch_after_commit(
                AcmeProbeJob, on_failure=AcmeCallback
            )
        released = asyncio.Event()

        async def flush_later() -> None:
            await released.wait()
            await flush_pending_dispatches()

        with ExecutionScope(BOB), ExecutionScope(ALICE):
            outlived = asyncio.create_task(flush_later())
        released.set()

        with pytest.raises(PostCommitError) as refused:
            await outlived

        [failure] = refused.value.failures
        assert isinstance(failure, DelegationExpired)
        assert callback_log == []

    async def test_drained_inside_work_acting_as_its_dispatcher_runs(
        self, pending: PostCommitChannel, callback_log: CallbackLog
    ) -> None:
        with ExecutionScope(ALICE):
            await pending.drain(committed=True)

        assert callback_log == [("built", AS_ALICE), ("on_success", AS_ALICE)]

    async def test_drained_inside_work_acting_as_another_is_refused(
        self, pending: PostCommitChannel, callback_log: CallbackLog
    ) -> None:
        with ExecutionScope(BOB), pytest.raises(PostCommitError) as refused:
            await pending.drain(committed=True)

        [failure] = refused.value.failures
        assert isinstance(failure, IdentityFixed)
        assert callback_log == []


class TestADispatchWithNoCaller:
    """Inline and Celery agree: the job a dispatch with no caller causes has none."""

    async def test_an_anonymous_dispatcher_is_captured_as_no_caller(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        callback_log: CallbackLog,
        channel: PostCommitChannel,
    ) -> None:
        """As the Celery and SQS wire carry it: its job and callbacks have none."""
        with acting_as(ANONYMOUS):
            InlineServiceFixture(factory, executor).dispatch_after_commit(
                AcmeProbeJob, on_failure=AcmeCallback
            )

        await channel.drain(committed=True)

        assert callback_log == [("built", NO_CALLER), ("on_failure", NO_CALLER)]

    async def test_drained_inside_work_acting_as_someone_never_runs_as_them(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        callback_log: CallbackLog,
        channel: PostCommitChannel,
    ) -> None:
        """As on Celery, a dispatch with no caller never borrows the caller of the work draining."""
        InlineServiceFixture(factory, executor).dispatch_after_commit(
            AcmeProbeJob, on_success=AcmeCallback
        )

        with ExecutionScope(ALICE), pytest.raises(PostCommitError) as refused:
            await channel.drain(committed=True)

        [failure] = refused.value.failures
        assert isinstance(failure, IdentityFixed)
        assert callback_log == []

    async def test_inline_runs_the_job_with_no_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor, callback_log: CallbackLog
    ) -> None:
        jobs = InlineServiceFixture(factory, executor)
        failures: list[str] = []

        class AcmeFailureNoted(AcmeCallback):
            def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
                failures.append(exc_type)

        async def dispatch() -> None:
            jobs.dispatch_after_commit(AcmeProbeJob, on_failure=AcmeFailureNoted)

        await executor.execute(AcmeRun(dispatch))

        assert failures == ["Unauthenticated"]

    async def test_celery_runs_the_job_with_no_caller(
        self, factory: UseCaseFactory, executor: RuntimeExecutor
    ) -> None:
        broker = CeleryBroker([AcmeProbeJob.__queue__])
        jobs = CeleryServiceFixture(broker.app, factory=factory, executor=executor)

        async def dispatch() -> None:
            jobs.dispatch_after_commit(AcmeProbeJob)

        await executor.execute(AcmeRun(dispatch))
        (message,) = broker.sent()
        sent = {**message.kwargs}
        del sent["failure_callback"]
        envelope = make(JobEnvelope, job="AcmeProbeJob", **sent)

        with pytest.raises(Unauthenticated):
            await run_envelope(envelope, factory, executor)
