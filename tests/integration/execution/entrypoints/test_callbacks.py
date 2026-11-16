"""Inline job callbacks act as the identity captured at dispatch (D7 revised, T09a).

A callback is work its job's dispatch caused, so it acts as the dispatcher:
a handle (which reads the execution identity at call time, D2), a constructor
``Caller()`` and ``current_identity()`` all see that caller. A dispatch with
no caller gives callbacks none, and they fail closed.
"""

from __future__ import annotations

import time

import pytest

from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import Unauthenticated
from loom.core.execution import current_execution
from loom.core.job.callback import CallbackOutcome, notify
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService
from loom.core.use_case.factory import UseCaseFactory
from tests.helpers.delegation import acting_as

from .doubles import (
    ALICE,
    AS_ALICE,
    CALLBACKS,
    NO_CALLER,
    AcmeCallback,
    AcmeCallerCallback,
    AcmeFailingJob,
    AcmeProbeJob,
    AcmeRun,
    AcmeSuccessOnlyCallback,
    CallbackLog,
)

OUTCOMES = pytest.mark.parametrize(
    ("job", "moment"),
    [(AcmeProbeJob, "on_success"), (AcmeFailingJob, "on_failure")],
    ids=["success", "failure"],
)


class TestInlineCallbacks:
    """Dispatched by an execution and run by its drain."""

    @CALLBACKS
    @OUTCOMES
    async def test_act_as_their_dispatcher(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        callback_log: CallbackLog,
        callback: type[AcmeCallback],
        job: type[Job[object]],
        moment: str,
    ) -> None:
        jobs = InlineJobService(factory, executor)

        async def dispatch() -> None:
            jobs.dispatch(job, on_success=callback, on_failure=callback)

        await executor.execute(AcmeRun(dispatch), identity=ALICE)

        assert callback_log == [("built", AS_ALICE), (moment, AS_ALICE)]

    @pytest.mark.parametrize(
        ("job", "moments"),
        [(AcmeProbeJob, ["built", "on_success"]), (AcmeFailingJob, ["built"])],
        ids=["success-runs-the-override", "failure-runs-the-base-no-op"],
    )
    async def test_a_callback_overriding_one_hook_serves_both_outcomes(
        self,
        factory: UseCaseFactory,
        executor: RuntimeExecutor,
        callback_log: CallbackLog,
        job: type[Job[object]],
        moments: list[str],
    ) -> None:
        jobs = InlineJobService(factory, executor)

        async def dispatch() -> None:
            jobs.dispatch(
                job, on_success=AcmeSuccessOnlyCallback, on_failure=AcmeSuccessOnlyCallback
            )

        await executor.execute(AcmeRun(dispatch), identity=ALICE)

        assert [moment for moment, _ in callback_log] == moments

    async def test_a_dispatch_with_no_caller_gives_the_job_and_them_none(
        self, factory: UseCaseFactory, executor: RuntimeExecutor, callback_log: CallbackLog
    ) -> None:
        """The job's constructor ``Caller()`` fails closed, and its failure callback has none."""
        jobs = InlineJobService(factory, executor)

        async def dispatch() -> None:
            jobs.dispatch(AcmeProbeJob, on_success=AcmeCallback, on_failure=AcmeCallback)

        await executor.execute(AcmeRun(dispatch))

        assert callback_log == [("built", NO_CALLER), ("on_failure", NO_CALLER)]


class TestNotify:
    """``notify``: how loom builds and calls a callback, as the caller acting where it runs.

    That caller is the one the inline dispatch or the Celery link envelope
    installed, the dispatcher's.
    """

    @CALLBACKS
    async def test_act_as_the_caller_acting_and_leave_nothing_behind(
        self, factory: UseCaseFactory, callback_log: CallbackLog, callback: type[AcmeCallback]
    ) -> None:
        with acting_as(ALICE):
            await notify(factory, callback, CallbackOutcome.SUCCESS, job_id="acme-job", result=None)
            await notify(
                factory,
                callback,
                CallbackOutcome.FAILURE,
                job_id="acme-job",
                exc_type="LookupError",
                exc_msg="acme",
            )
            assert current_execution() is None

        assert callback_log == [
            ("built", AS_ALICE),
            ("on_success", AS_ALICE),
            ("built", AS_ALICE),
            ("on_failure", AS_ALICE),
        ]

    async def test_a_constructor_caller_receives_the_dispatcher(
        self, factory: UseCaseFactory, callback_log: CallbackLog
    ) -> None:
        with acting_as(ALICE):
            await notify(
                factory, AcmeCallerCallback, CallbackOutcome.SUCCESS, job_id="acme-job", result=None
            )

        assert callback_log == [("caller", AS_ALICE)]

    async def test_a_constructor_caller_is_refused_when_the_dispatch_had_none(
        self, factory: UseCaseFactory
    ) -> None:
        with pytest.raises(Unauthenticated, match="identity="):
            await notify(
                factory, AcmeCallerCallback, CallbackOutcome.SUCCESS, job_id="acme-job", result=None
            )

    async def test_past_the_dispatcher_s_deadline_nothing_is_built(
        self, factory: UseCaseFactory, callback_log: CallbackLog
    ) -> None:
        with (
            acting_as(ALICE, until=time.time() - 1),
            pytest.raises(Unauthenticated, match="system identity"),
        ):
            await notify(
                factory, AcmeCallback, CallbackOutcome.SUCCESS, job_id="acme-job", result=None
            )

        assert callback_log == []
