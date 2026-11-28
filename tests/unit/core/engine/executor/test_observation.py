"""Each attempted run reports one terminal outcome: events, observer and one log line.

``loom.core.engine.observation``: ``EXEC_START`` once the use case is built,
then ``use_case_done``/``EXEC_DONE`` or ``use_case_failed``/``EXEC_ERROR``; a
constructor failure reports ``use_case_failed`` with the phase ``build`` and no event.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from loom.core.engine.compiler import CompilationError
from loom.core.engine.events import EventKind, ExecutionEvent
from loom.core.engine.executor import UseCaseExecutor
from loom.core.engine.observation import UseCaseEvent, UseCaseKey
from loom.core.errors import (
    DelegationExpired,
    RuleViolationError,
    RuleViolationsError,
    UndeclaredStoreWrite,
)
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.job.context import flush_pending_dispatches
from loom.core.log_names import LogField
from loom.core.tracing import reset_trace_id, set_trace_id
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.markers import Input, LoadById
from loom.core.use_case.use_case import UseCase
from tests.helpers.delegation import acting_as
from tests.helpers.jobs import InlineServiceFixture
from tests.helpers.loggers import CapturedLog

from .._lifecycle_doubles import RecordingExecutionObserver
from .conftest import MakeExecutor
from .doubles import (
    ALICE,
    JOURNAL,
    AcmeBroken,
    AcmeBrokenJob,
    AcmeCreateUserCommand,
    AcmeInputOnlyUseCase,
    AcmeNoMarkersUseCase,
    AcmeOnFailure,
    AcmeSlowStart,
    AcmeTracedUseCase,
    AcmeUser,
    AcmeWithLoadUseCase,
    fake_repo,
)


class TestLogs:
    """Every execution logs one terminal line, with its steps in debug mode and its broken rules."""

    async def test_a_run_logs_only_its_outcome_with_its_duration(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor()
        await executor.execute(AcmeNoMarkersUseCase(), params={"value": 1})

        [(event, fields)] = logger.events
        assert event == UseCaseEvent.DONE
        assert fields[LogField.USE_CASE] == "AcmeNoMarkersUseCase"
        assert fields[LogField.DURATION_MS] >= 0

    async def test_outside_debug_mode_the_log_lists_no_steps(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor(debug_execution=False)
        await executor.execute(AcmeInputOnlyUseCase(), payload={"email": "a@b.com", "name": "X"})

        [(_, fields)] = logger.events
        assert UseCaseKey.STEPS not in fields

    async def test_in_debug_mode_the_log_lists_every_step_in_order(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor(debug_execution=True)
        user = AcmeUser(id=1, email="u@u.com")
        await executor.execute(
            AcmeWithLoadUseCase(), params={"user_id": 1}, dependencies={AcmeUser: fake_repo(user)}
        )

        [(_, fields)] = logger.events
        assert fields[UseCaseKey.STEPS] == ["Load AcmeUser", "Execute core logic"]

    async def test_in_debug_mode_the_log_lists_the_command_bind(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor(debug_execution=True)
        await executor.execute(AcmeInputOnlyUseCase(), payload={"email": "a@b.com", "name": "X"})

        [(_, fields)] = logger.events
        assert fields[UseCaseKey.STEPS] == ["Bind Input", "Execute core logic"]

    async def test_a_rule_failure_is_one_warning_listing_every_broken_rule(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        def bad_email(cmd: Any, fs: Any) -> None:
            raise RuleViolationError("email", "bad")

        def bad_name(cmd: Any, fs: Any) -> None:
            raise RuleViolationError("name", "short")

        class RuleUseCase(UseCase[Any, None]):
            rules = [bad_email, bad_name]

            async def execute(self, cmd: AcmeCreateUserCommand = Input()) -> None:
                pass

        executor = make_executor(debug_execution=False)
        use_case = RuleUseCase()
        with pytest.raises(RuleViolationsError):
            await executor.execute(use_case, payload={"email": "x@x.com", "name": "X"})

        [(event, fields)] = logger.events
        assert event == UseCaseEvent.FAILED
        assert fields[LogField.STATUS] == "rule_failure"
        assert fields[UseCaseKey.BROKEN_RULES] == ["bad_email:email", "bad_name:name"]

    async def test_violations_raised_by_execute_are_listed_without_a_rule_name(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        class RaisesViolations(UseCase[Any, None]):
            async def execute(self) -> None:
                raise RuleViolationsError([RuleViolationError("email", "taken")])

        executor = make_executor()
        use_case = RaisesViolations()

        with pytest.raises(RuleViolationsError):
            await executor.execute(use_case)

        [(_, fields)] = logger.events
        assert fields[LogField.STATUS] == "rule_failure"
        assert fields[UseCaseKey.BROKEN_RULES] == ["email"]


class TestTraceId:
    """The trace id in flight reaches every event."""

    async def test_exec_start_carries_trace_id_when_set(
        self, make_executor: MakeExecutor, observer: RecordingExecutionObserver
    ) -> None:
        executor = make_executor(execution_observer=observer)

        token = set_trace_id("trace-abc")
        try:
            await executor.execute(AcmeTracedUseCase())
        finally:
            reset_trace_id(token)

        start_events = [e for e in observer.events if e.kind == EventKind.EXEC_START]
        assert start_events[0].trace_id == "trace-abc"

    async def test_exec_done_carries_trace_id_when_set(
        self, make_executor: MakeExecutor, observer: RecordingExecutionObserver
    ) -> None:
        executor = make_executor(execution_observer=observer)

        token = set_trace_id("trace-xyz")
        try:
            await executor.execute(AcmeTracedUseCase())
        finally:
            reset_trace_id(token)

        done_events = [e for e in observer.events if e.kind == EventKind.EXEC_DONE]
        assert done_events[0].trace_id == "trace-xyz"

    async def test_trace_id_is_none_when_not_set(
        self, make_executor: MakeExecutor, observer: RecordingExecutionObserver
    ) -> None:
        executor = make_executor(execution_observer=observer)

        await executor.execute(AcmeTracedUseCase())

        for ev in observer.events:
            assert ev.trace_id is None


class TestBuildFailure:
    """A constructor failure raises as is, before ``EXEC_START``, logged with its kind.

    ``use_case_failed`` with the phase ``build``, like an execution failing.
    """

    async def test_a_constructor_failure_raises_before_the_execution_starts(
        self, executor: UseCaseExecutor, factory: UseCaseFactory
    ) -> None:
        with pytest.raises(LookupError, match="acme constructor failed"):
            await executor.run(AcmeBroken, factory=factory, identity=ALICE)

        assert JOURNAL.entries == []
        assert current_execution() is None
        assert current_identity() is ANONYMOUS

    async def test_a_constructor_failure_is_logged_as_the_execution_failing(
        self, make_executor: MakeExecutor, factory: UseCaseFactory, logger: CapturedLog
    ) -> None:
        executor = make_executor()

        with pytest.raises(LookupError):
            await executor.run(AcmeBroken, factory=factory)

        ((event, fields),) = logger.errors
        assert event == UseCaseEvent.FAILED
        assert fields[LogField.USE_CASE] == "AcmeBroken"
        assert fields[UseCaseKey.PHASE] == "build"
        assert logger.exception_types(UseCaseEvent.FAILED) == [LookupError]

    @pytest.mark.parametrize("on_failure", [None, AcmeOnFailure], ids=["no-callback", "callback"])
    async def test_an_inline_job_that_fails_to_build_is_reported_like_one_that_fails_to_run(
        self,
        make_executor: MakeExecutor,
        factory: UseCaseFactory,
        on_failure: type[AcmeOnFailure] | None,
        logger: CapturedLog,
    ) -> None:
        AcmeOnFailure.calls.clear()
        factory.register(AcmeOnFailure)
        service = InlineServiceFixture(factory, make_executor())

        service.dispatch_after_commit(AcmeBrokenJob, on_failure=on_failure)
        await flush_pending_dispatches()
        await service.wait_until_idle()

        assert [(event, fields[LogField.USE_CASE]) for event, fields in logger.errors] == [
            (UseCaseEvent.FAILED, "AcmeBrokenJob")
        ]
        assert AcmeOnFailure.calls == (
            []
            if on_failure is None
            else [(f"{AcmeBrokenJob.__module__}.AcmeUnrecoverableError", None)]
        )


class TestDuration:
    """``duration_ms`` covers the whole execution, from before ``EXEC_START``."""

    async def test_a_slow_exec_start_observer_is_included_in_the_duration(
        self, make_executor: MakeExecutor
    ) -> None:
        observer = AcmeSlowStart()

        await make_executor(execution_observer=observer).execute(AcmeTracedUseCase())

        assert observer.done_ms is not None
        assert observer.done_ms >= AcmeSlowStart.DELAY_S * 1000


class TestOneTerminalOutcome:
    """Exactly one done or failed record per attempted run, built or not."""

    async def test_a_run_that_succeeds_logs_one_done_and_no_fail(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        await make_executor().execute(AcmeNoMarkersUseCase(), params={"value": 1})

        assert [_outcome(m) for m in logger.messages if _outcome(m)] == [UseCaseEvent.DONE]

    async def test_a_run_that_fails_logs_one_fail_and_no_done(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor()
        use_case = AcmeNoMarkersUseCase()

        with pytest.raises(ValueError, match="missing required parameter"):
            await executor.execute(use_case, params={})

        assert [_outcome(m) for m in logger.messages if _outcome(m)] == [UseCaseEvent.FAILED]

    async def test_a_run_that_fails_to_build_logs_one_fail_and_emits_no_event(
        self, make_executor: MakeExecutor, logger: CapturedLog, factory: UseCaseFactory
    ) -> None:
        executor = make_executor()

        with pytest.raises(LookupError):
            await executor.run(AcmeBroken, factory=factory)

        assert [_outcome(m) for m in logger.messages if _outcome(m)] == [UseCaseEvent.FAILED]
        assert JOURNAL.entries == []


class _RaisesOnStart(RecordingExecutionObserver):
    def on_event(self, event: ExecutionEvent) -> None:
        if event.kind is EventKind.EXEC_START:
            raise RuntimeError("acme observer down")
        super().on_event(event)


class _DoesNotCompile(UseCase[Any, None]):
    async def execute(self, user: AcmeUser = LoadById(AcmeUser, by="missing")) -> None:
        pass


class TestUnstartedRunsLogOneTerminalLine:
    """A run refused before its unit of work opens logs one failure with the phase ``begin``."""

    async def test_a_plan_that_fails_to_compile_on_demand(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor()
        use_case = _DoesNotCompile()

        with pytest.raises(CompilationError):
            await executor.execute(use_case)

        assert _terminal_lines(logger, "_DoesNotCompile") == [(UseCaseEvent.FAILED, "begin")]

    @pytest.mark.usefixtures("switches")
    async def test_a_caller_past_its_delegation(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor()
        use_case = AcmeNoMarkersUseCase()

        with acting_as(ALICE, until=time.time() - 1), pytest.raises(DelegationExpired):
            await executor.execute(use_case, params={"value": 1})

        assert _terminal_lines(logger, "AcmeNoMarkersUseCase") == [(UseCaseEvent.FAILED, "begin")]

    async def test_a_writer_under_a_read_only_execution(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor()

        class ReadsThenWrites(UseCase[Any, None]):
            read_only = True

            async def execute(self) -> None:
                await executor.execute(AcmeNoMarkersUseCase(), params={"value": 1})

        use_case = ReadsThenWrites()

        with pytest.raises(UndeclaredStoreWrite):
            await executor.execute(use_case)

        assert _terminal_lines(logger, "AcmeNoMarkersUseCase") == [(UseCaseEvent.FAILED, "begin")]

    async def test_a_execution_observer_raising_on_start(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        observer = _RaisesOnStart()

        executor = make_executor(execution_observer=observer)
        use_case = AcmeNoMarkersUseCase()

        with pytest.raises(RuntimeError, match="acme observer down"):
            await executor.execute(use_case, params={"value": 1})

        assert _terminal_lines(logger, "AcmeNoMarkersUseCase") == [(UseCaseEvent.FAILED, "begin")]
        assert observer.events == []


def _terminal_lines(logger: CapturedLog, use_case: str) -> list[tuple[str, str | None]]:
    """The event and phase of each terminal record *use_case* logged."""
    return [
        (event, fields.get(UseCaseKey.PHASE))
        for event, fields in logger.events
        if fields.get(LogField.USE_CASE) == use_case and _outcome(event) is not None
    ]


def _outcome(message: str) -> str | None:
    """*message* when it is a terminal outcome record."""
    return next((e for e in (UseCaseEvent.DONE, UseCaseEvent.FAILED) if message == e), None)
