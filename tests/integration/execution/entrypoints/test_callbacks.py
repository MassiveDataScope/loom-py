"""Inline job callbacks are built and called inside an execution with no caller (D7).

A callback reacts to a job; it does not act for the job's caller. So inside
it the execution identity is ``None``: a handle (which reads it at call time,
D2) and a constructor ``Caller()`` fail closed with ``Unauthenticated``, and
the ambient identity is ``ANONYMOUS`` (F3), never the caller of the execution
whose drain runs the callback.
"""

from __future__ import annotations

import pytest

from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import Unauthenticated
from loom.core.job.callback import run_on_failure, run_on_success
from loom.core.job.job import Job
from loom.core.job.service import InlineJobService
from loom.core.use_case.factory import UseCaseFactory

from .doubles import (
    ALICE,
    CALLBACKS,
    NO_CALLER,
    AcmeCallback,
    AcmeCallerCallback,
    AcmeFailingJob,
    AcmeProbeJob,
    AcmeRun,
    CallbackLog,
)


class TestInlineCallbacks:
    """Dispatched by an execution as alice and run by its drain."""

    @CALLBACKS
    @pytest.mark.parametrize(
        ("job", "moment"),
        [(AcmeProbeJob, "on_success"), (AcmeFailingJob, "on_failure")],
        ids=["success", "failure"],
    )
    async def test_are_built_and_called_with_no_caller(
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

        assert callback_log == [("built", NO_CALLER), (moment, NO_CALLER)]


class TestCallbackRunners:
    """``run_on_success`` / ``run_on_failure``: the one way loom builds and calls a callback."""

    @CALLBACKS
    async def test_leave_no_execution_or_identity_behind(
        self, factory: UseCaseFactory, callback_log: CallbackLog, callback: type[AcmeCallback]
    ) -> None:
        await run_on_success(factory, callback, "acme-job", None, {})
        await run_on_failure(factory, callback, "acme-job", "LookupError", "acme", {})

        assert [moment for moment, _ in callback_log] == [
            "built",
            "on_success",
            "built",
            "on_failure",
        ]

    async def test_a_constructor_caller_is_refused_as_a_callback_has_none(
        self, factory: UseCaseFactory
    ) -> None:
        with pytest.raises(Unauthenticated, match="job callback"):
            await run_on_success(factory, AcmeCallerCallback, "acme-job", None, {})
