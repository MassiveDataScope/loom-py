"""Each attempted run reports one terminal outcome: its events and its scope's one end.

``loom.core.engine.observation``: ``EXEC_START`` once the use case is built,
then ``EXEC_DONE`` or ``EXEC_ERROR``; the record of the run is its ``use_case``
scope's end, written by the lifecycle observer, built or not.
"""

from __future__ import annotations

import time
from typing import Any

import pytest

from loom.core.engine.compiler import CompilationError
from loom.core.engine.events import EventKind, ExecutionEvent
from loom.core.engine.executor import UseCaseExecutor
from loom.core.engine.log_events import EngineLogEvent, EngineLogField
from loom.core.errors import (
    DelegationExpired,
    RuleViolationError,
    RuleViolationsError,
    UndeclaredStoreWrite,
)
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.job.context import flush_pending_dispatches
from loom.core.logger import LogField
from loom.core.logger.failure import exception_type
from loom.core.observability.log_events import ObservabilityLogEvent
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
    AcmeUnrecoverableError,
    AcmeUser,
    AcmeWithLoadUseCase,
    fake_repo,
)


class TestLogs:
    """A run's record is its ``use_case`` scope's end; debug mode adds its steps as one record."""

    async def test_a_run_is_recorded_by_its_scope_with_its_duration(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        await make_executor().execute(AcmeNoMarkersUseCase(), params={"value": 1})

        [ended] = logger.named(ObservabilityLogEvent.LIFECYCLE_ENDED)
        assert (ended[LogField.NAME], ended[LogField.STATUS]) == ("AcmeNoMarkersUseCase", "success")
        assert ended[LogField.DURATION_MS] >= 0

    async def test_outside_debug_mode_no_steps_are_recorded(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor(debug_execution=False)
        await executor.execute(AcmeInputOnlyUseCase(), payload={"email": "a@b.com", "name": "X"})

        assert logger.named(EngineLogEvent.EXECUTION_STEPS) == []

    async def test_in_debug_mode_every_step_is_recorded_in_order(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor(debug_execution=True)
        user = AcmeUser(id=1, email="u@u.com")
        await executor.execute(
            AcmeWithLoadUseCase(), params={"user_id": 1}, dependencies={AcmeUser: fake_repo(user)}
        )

        [steps] = logger.named(EngineLogEvent.EXECUTION_STEPS)
        assert steps[EngineLogField.STEPS] == ["Load AcmeUser", "Execute core logic"]

    async def test_in_debug_mode_the_command_bind_is_a_step(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor(debug_execution=True)
        await executor.execute(AcmeInputOnlyUseCase(), payload={"email": "a@b.com", "name": "X"})

        [steps] = logger.named(EngineLogEvent.EXECUTION_STEPS)
        assert steps[EngineLogField.STEPS] == ["Bind Input", "Execute core logic"]

    async def test_a_rule_failure_is_an_expected_end_with_no_stack_and_no_message(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        def bad_email(cmd: Any, fs: Any) -> None:
            raise RuleViolationError("email", "acme-secret-email")

        class RuleUseCase(UseCase[Any, None]):
            rules = [bad_email]

            async def execute(self, cmd: AcmeCreateUserCommand = Input()) -> None:
                pass

        with pytest.raises(RuleViolationsError):
            await make_executor().execute(RuleUseCase(), payload={"email": "x@x", "name": "X"})

        [(event, ended)] = [
            c for c in logger.calls if c[0] == ObservabilityLogEvent.LIFECYCLE_ENDED
        ]
        assert ended[LogField.EXCEPTION_TYPE].endswith("RuleViolationsError")
        assert LogField.EXCEPTION_STACKTRACE not in ended
        assert logger.errors == []
        assert "acme-secret-email" not in repr(logger.calls)


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
    """A constructor failure raises as is, before ``EXEC_START``, and ends its scope failed."""

    async def test_a_constructor_failure_raises_before_the_execution_starts(
        self, executor: UseCaseExecutor, factory: UseCaseFactory
    ) -> None:
        with pytest.raises(LookupError, match="acme constructor failed"):
            await executor.run(AcmeBroken, factory=factory, identity=ALICE)

        assert JOURNAL.entries == []
        assert current_execution() is None
        assert current_identity() is ANONYMOUS

    async def test_a_constructor_failure_ends_the_scope_as_a_failure(
        self, make_executor: MakeExecutor, factory: UseCaseFactory, logger: CapturedLog
    ) -> None:
        with pytest.raises(LookupError):
            await make_executor().run(AcmeBroken, factory=factory)

        [(event, fields)] = logger.errors
        assert (event, fields[LogField.NAME]) == (
            ObservabilityLogEvent.LIFECYCLE_ENDED,
            "AcmeBroken",
        )
        assert fields[LogField.EXCEPTION_TYPE] == "LookupError"
        assert "acme constructor failed" not in repr(logger.calls)

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

        assert _failed_scopes(logger, "AcmeBrokenJob") == 1
        assert AcmeOnFailure.calls == (
            [] if on_failure is None else [exception_type(AcmeUnrecoverableError)]
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
    """Exactly one scope end per attempted run, built or not."""

    async def test_a_run_that_succeeds_ends_once_as_a_success(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        await make_executor().execute(AcmeNoMarkersUseCase(), params={"value": 1})

        assert _ends(logger, "AcmeNoMarkersUseCase") == ["success"]

    async def test_a_run_that_fails_ends_once_as_a_failure(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        with pytest.raises(ValueError, match="missing required parameter"):
            await make_executor().execute(AcmeNoMarkersUseCase(), params={})

        assert _ends(logger, "AcmeNoMarkersUseCase") == ["failure"]

    async def test_a_run_that_fails_to_build_ends_once_and_emits_no_event(
        self, make_executor: MakeExecutor, logger: CapturedLog, factory: UseCaseFactory
    ) -> None:
        with pytest.raises(LookupError):
            await make_executor().run(AcmeBroken, factory=factory)

        assert _ends(logger, "AcmeBroken") == ["failure"]
        assert JOURNAL.entries == []


class _RaisesOnStart(RecordingExecutionObserver):
    def on_event(self, event: ExecutionEvent) -> None:
        if event.kind is EventKind.EXEC_START:
            raise RuntimeError("acme observer down")
        super().on_event(event)


class _DoesNotCompile(UseCase[Any, None]):
    async def execute(self, user: AcmeUser = LoadById(AcmeUser, by="missing")) -> None:
        pass


class TestUnstartedRunsEndTheirScopeOnce:
    """A run refused before its unit of work opens still ends its scope once, as a failure."""

    async def test_a_plan_that_fails_to_compile_on_demand(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor()
        use_case = _DoesNotCompile()

        with pytest.raises(CompilationError):
            await executor.execute(use_case)

        assert _ends(logger, "_DoesNotCompile") == ["failure"]

    @pytest.mark.usefixtures("switches")
    async def test_a_caller_past_its_delegation(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        executor = make_executor()
        use_case = AcmeNoMarkersUseCase()

        with acting_as(ALICE, until=time.time() - 1), pytest.raises(DelegationExpired):
            await executor.execute(use_case, params={"value": 1})

        assert _ends(logger, "AcmeNoMarkersUseCase") == ["failure"]

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

        assert _ends(logger, "AcmeNoMarkersUseCase") == ["failure"]

    async def test_a_execution_observer_raising_on_start(
        self, make_executor: MakeExecutor, logger: CapturedLog
    ) -> None:
        observer = _RaisesOnStart()

        executor = make_executor(execution_observer=observer)
        use_case = AcmeNoMarkersUseCase()

        with pytest.raises(RuntimeError, match="acme observer down"):
            await executor.execute(use_case, params={"value": 1})

        assert _ends(logger, "AcmeNoMarkersUseCase") == ["failure"]
        assert observer.events == []


def _ends(logger: CapturedLog, use_case: str) -> list[str]:
    """The status of each scope end *use_case* recorded."""
    return [
        fields[LogField.STATUS]
        for fields in logger.named(ObservabilityLogEvent.LIFECYCLE_ENDED)
        if fields[LogField.NAME] == use_case
    ]


def _failed_scopes(logger: CapturedLog, use_case: str) -> int:
    """How many scope ends of *use_case* were failures."""
    return _ends(logger, use_case).count("failure")
