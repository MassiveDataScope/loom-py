"""Each attempted run reports one terminal outcome: events, metrics and logs.

``loom.core.engine.observation``: ``[EXEC]`` and ``EXEC_START`` once the use
case is built, then ``[DONE]``/``EXEC_DONE`` or ``[FAIL]``/``EXEC_ERROR``; a
constructor failure reports ``[FAIL]`` with ``error_kind="build"`` and no event.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.core.engine.events import EventKind
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import RuleViolation, RuleViolations
from loom.core.execution import current_execution
from loom.core.identity import ANONYMOUS, current_identity
from loom.core.job.context import flush_pending_dispatches
from loom.core.job.service import InlineJobService
from loom.core.tracing import reset_trace_id, set_trace_id
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.markers import Input
from loom.core.use_case.use_case import UseCase

from .._lifecycle_doubles import Metrics
from .conftest import MakeExecutor
from .doubles import (
    ALICE,
    JOURNAL,
    AcmeBroken,
    AcmeBrokenJob,
    AcmeCreateUserCommand,
    AcmeErrorRecorder,
    AcmeInputOnlyUseCase,
    AcmeNoMarkersUseCase,
    AcmeOnFailure,
    AcmeRecordingLogger,
    AcmeSlowStart,
    AcmeTracedUseCase,
    AcmeUser,
    AcmeWithLoadUseCase,
    fake_repo,
)


class TestLogs:
    """Every execution logs ``[EXEC]`` and its outcome; steps only in debug mode."""

    async def test_always_logs_exec(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger
    ) -> None:
        executor = make_executor(logger=logger)
        await executor.execute(AcmeNoMarkersUseCase(), params={"value": 1})
        assert any("[EXEC]" in m for m in logger.messages)

    async def test_always_logs_done_with_ms(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger
    ) -> None:
        executor = make_executor(logger=logger)
        await executor.execute(AcmeNoMarkersUseCase(), params={"value": 1})
        assert any("[DONE]" in m and "ms" in m for m in logger.messages)

    async def test_debug_false_no_step_logs(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger
    ) -> None:
        executor = make_executor(debug_execution=False, logger=logger)
        await executor.execute(
            AcmeInputOnlyUseCase(),
            payload={"email": "a@b.com", "name": "X"},
        )
        assert not any("[STEP]" in m for m in logger.messages)

    @pytest.mark.parametrize(
        "expected",
        [
            pytest.param("[STEP]", id="debug_true_emits_step_logs"),
            pytest.param("Bind Input", id="debug_true_emits_bind_input"),
        ],
    )
    async def test_debug_true_emits_each_log(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger, expected: str
    ) -> None:
        executor = make_executor(debug_execution=True, logger=logger)
        await executor.execute(
            AcmeInputOnlyUseCase(),
            payload={"email": "a@b.com", "name": "X"},
        )
        assert any(expected in m for m in logger.messages)

    async def test_debug_true_emits_load_step(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger
    ) -> None:
        executor = make_executor(debug_execution=True, logger=logger)
        user = AcmeUser(id=1, email="u@u.com")
        await executor.execute(
            AcmeWithLoadUseCase(),
            params={"user_id": 1},
            dependencies={AcmeUser: fake_repo(user)},
        )
        assert any("Load AcmeUser" in m for m in logger.messages)

    async def test_rule_failure_always_logged_as_warning(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger
    ) -> None:
        def bad_rule(cmd: Any, fs: Any) -> None:
            raise RuleViolation("email", "bad")

        class RuleUseCase(UseCase[Any, None]):
            rules = [bad_rule]

            async def execute(self, cmd: AcmeCreateUserCommand = Input()) -> None:
                pass

        executor = make_executor(debug_execution=False, logger=logger)

        use_case = RuleUseCase()
        with pytest.raises(RuleViolations):
            await executor.execute(
                use_case,
                payload={"email": "x@x.com", "name": "X"},
            )

        assert any("[RULE]" in m for m in logger.messages)


class TestTraceId:
    """The trace id in flight reaches every event and binds the logger."""

    async def test_exec_start_carries_trace_id_when_set(
        self, make_executor: MakeExecutor, metrics: Metrics
    ) -> None:
        executor = make_executor(metrics=metrics)

        token = set_trace_id("trace-abc")
        try:
            await executor.execute(AcmeTracedUseCase())
        finally:
            reset_trace_id(token)

        start_events = [e for e in metrics.events if e.kind == EventKind.EXEC_START]
        assert start_events[0].trace_id == "trace-abc"

    async def test_exec_done_carries_trace_id_when_set(
        self, make_executor: MakeExecutor, metrics: Metrics
    ) -> None:
        executor = make_executor(metrics=metrics)

        token = set_trace_id("trace-xyz")
        try:
            await executor.execute(AcmeTracedUseCase())
        finally:
            reset_trace_id(token)

        done_events = [e for e in metrics.events if e.kind == EventKind.EXEC_DONE]
        assert done_events[0].trace_id == "trace-xyz"

    async def test_trace_id_is_none_when_not_set(
        self, make_executor: MakeExecutor, metrics: Metrics
    ) -> None:
        executor = make_executor(metrics=metrics)

        await executor.execute(AcmeTracedUseCase())

        for ev in metrics.events:
            assert ev.trace_id is None

    async def test_logger_bind_called_with_trace_id(self, make_executor: MakeExecutor) -> None:
        mock_logger = MagicMock()
        bound_logger = MagicMock()
        mock_logger.bind.return_value = bound_logger

        executor = make_executor(logger=mock_logger)

        token = set_trace_id("bind-test")
        try:
            await executor.execute(AcmeTracedUseCase())
        finally:
            reset_trace_id(token)

        mock_logger.bind.assert_called_once_with(trace_id="bind-test")

    async def test_logger_not_bound_when_trace_id_absent(self, make_executor: MakeExecutor) -> None:
        mock_logger = MagicMock()
        executor = make_executor(logger=mock_logger)

        await executor.execute(AcmeTracedUseCase())

        mock_logger.bind.assert_not_called()


class TestBuildFailure:
    """A constructor failure raises as is, before ``EXEC_START``, logged with its kind.

    ``[FAIL]`` with ``error_kind="build"``, like an execution failing.
    """

    async def test_a_constructor_failure_raises_before_the_execution_starts(
        self, executor: RuntimeExecutor, factory: UseCaseFactory
    ) -> None:
        with pytest.raises(LookupError, match="acme constructor failed"):
            await executor.run(AcmeBroken, factory=factory, identity=ALICE)

        assert JOURNAL.entries == []
        assert current_execution() is None
        assert current_identity() is ANONYMOUS

    async def test_a_constructor_failure_is_logged_as_the_execution_failing(
        self, make_executor: MakeExecutor, factory: UseCaseFactory
    ) -> None:
        logger = AcmeErrorRecorder()

        with pytest.raises(LookupError):
            await make_executor(logger=logger).run(AcmeBroken, factory=factory)

        ((event, fields),) = logger.errors
        assert event == "[FAIL] AcmeBroken"
        assert fields["error_kind"] == "build"
        assert fields["error"] == "acme constructor failed"

    @pytest.mark.parametrize("on_failure", [None, AcmeOnFailure], ids=["no-callback", "callback"])
    async def test_an_inline_job_that_fails_to_build_is_reported_like_one_that_fails_to_run(
        self,
        make_executor: MakeExecutor,
        factory: UseCaseFactory,
        on_failure: type[AcmeOnFailure] | None,
    ) -> None:
        AcmeOnFailure.calls.clear()
        logger = AcmeErrorRecorder()
        factory.register(AcmeOnFailure)
        service = InlineJobService(factory, make_executor(logger=logger))

        service.dispatch(AcmeBrokenJob, on_failure=on_failure)
        await flush_pending_dispatches()

        assert [event for event, _ in logger.errors] == ["[FAIL] AcmeBrokenJob"]
        assert AcmeOnFailure.calls == (
            [] if on_failure is None else ["acme job constructor failed"]
        )


class TestDuration:
    """``duration_ms`` covers the whole execution, from before ``[EXEC]`` and ``EXEC_START``."""

    async def test_a_slow_exec_start_observer_is_included_in_the_duration(
        self, make_executor: MakeExecutor
    ) -> None:
        metrics = AcmeSlowStart()

        await make_executor(metrics=metrics).execute(AcmeTracedUseCase())

        assert metrics.done_ms is not None
        assert metrics.done_ms >= AcmeSlowStart.DELAY_S * 1000


class TestOneTerminalOutcome:
    """Exactly one ``[DONE]`` or ``[FAIL]`` line per attempted run, built or not."""

    async def test_a_run_that_succeeds_logs_one_done_and_no_fail(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger
    ) -> None:
        await make_executor(logger=logger).execute(AcmeNoMarkersUseCase(), params={"value": 1})

        assert [_outcome(m) for m in logger.messages if _outcome(m)] == ["[DONE]"]

    async def test_a_run_that_fails_logs_one_fail_and_no_done(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger
    ) -> None:
        with pytest.raises(ValueError, match="missing required parameter"):
            await make_executor(logger=logger).execute(AcmeNoMarkersUseCase(), params={})

        assert [_outcome(m) for m in logger.messages if _outcome(m)] == ["[FAIL]"]

    async def test_a_run_that_fails_to_build_logs_one_fail_and_emits_no_event(
        self, make_executor: MakeExecutor, logger: AcmeRecordingLogger, factory: UseCaseFactory
    ) -> None:
        with pytest.raises(LookupError):
            await make_executor(logger=logger).run(AcmeBroken, factory=factory)

        assert [_outcome(m) for m in logger.messages if _outcome(m)] == ["[FAIL]"]
        assert JOURNAL.entries == []


def _outcome(message: str) -> str | None:
    """``[DONE]`` or ``[FAIL]`` when *message* is a terminal outcome line."""
    return next((tag for tag in ("[DONE]", "[FAIL]") if message.startswith(tag)), None)
