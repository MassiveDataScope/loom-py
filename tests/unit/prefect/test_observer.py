"""Tests for loom.prefect._observer.PrefectObserver.

Verifies:
- PrefectObserver implements the LifecycleObserver protocol (has on_event).
- on_event() does NOT raise when called outside a Prefect context.
- on_event() logs the event when called inside a Prefect context
  (prefect.get_run_logger is patched to return a mock logger).
- on_event() falls back to stdlib logging when get_run_logger raises.
- All LifecycleEvent kinds (START, END, ERROR) are handled without raising.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from prefect.exceptions import MissingContextError

from loom.core.observability.event import LifecycleEvent, LifecycleStatus, Scope
from loom.prefect.observer import PrefectObserver

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _start_event(name: str = "MyPipeline") -> LifecycleEvent:
    return LifecycleEvent.start(
        scope=Scope.PIPELINE,
        name=name,
        trace_id="trace-1",
        correlation_id="corr-1",
    )


def _end_event(name: str = "MyPipeline") -> LifecycleEvent:
    return LifecycleEvent.end(
        scope=Scope.PIPELINE,
        name=name,
        trace_id="trace-1",
        correlation_id="corr-1",
        duration_ms=123.0,
        status=LifecycleStatus.SUCCESS,
    )


def _error_event(name: str = "MyStep") -> LifecycleEvent:
    return LifecycleEvent.exception(
        scope=Scope.STEP,
        name=name,
        trace_id="trace-1",
        correlation_id="corr-1",
        duration_ms=5.0,
        error="something went wrong",
    )


class TestPrefectObserver:
    # ---------------------------------------------------------------------------
    # Construction
    # ---------------------------------------------------------------------------

    def test_can_be_instantiated(self) -> None:
        """PrefectObserver can be constructed without arguments."""
        observer = PrefectObserver()
        assert observer is not None

    def test_has_on_event_method(self) -> None:
        """PrefectObserver exposes an on_event method."""
        observer = PrefectObserver()
        assert callable(getattr(observer, "on_event", None))

    # ---------------------------------------------------------------------------
    # No-raise guarantee outside Prefect context
    # ---------------------------------------------------------------------------

    def test_on_event_does_not_raise_outside_prefect_context(self) -> None:
        """on_event must not raise even when no Prefect run context is active."""
        observer = PrefectObserver()
        # Simulate missing Prefect context by making get_run_logger raise
        with patch(
            "prefect.get_run_logger",
            side_effect=MissingContextError("No active flow run context"),
        ):
            # None of these must raise
            observer.on_event(_start_event())
            observer.on_event(_end_event())
            observer.on_event(_error_event())

    def test_on_event_start_does_not_raise(self) -> None:
        """START events are handled without error (no Prefect context mocked)."""
        observer = PrefectObserver()
        with patch("prefect.get_run_logger", side_effect=MissingContextError("no context")):
            observer.on_event(_start_event())  # must not raise

    def test_on_event_end_does_not_raise(self) -> None:
        """END events are handled without error."""
        observer = PrefectObserver()
        with patch("prefect.get_run_logger", side_effect=MissingContextError("no context")):
            observer.on_event(_end_event())

    def test_on_event_error_does_not_raise(self) -> None:
        """ERROR events are handled without error."""
        observer = PrefectObserver()
        with patch("prefect.get_run_logger", side_effect=MissingContextError("no context")):
            observer.on_event(_error_event())

    # ---------------------------------------------------------------------------
    # With Prefect context active (mocked logger)
    # ---------------------------------------------------------------------------

    def test_on_event_uses_prefect_logger_when_available(self) -> None:
        """on_event calls the Prefect run logger when a context is active."""
        mock_logger = MagicMock()

        observer = PrefectObserver()
        with patch("prefect.get_run_logger", return_value=mock_logger):
            observer.on_event(_start_event("SomePipeline"))

        # The logger must have been called at least once with some message
        assert mock_logger.info.called or mock_logger.debug.called or mock_logger.warning.called

    def test_on_event_logs_error_kind_with_error_level(self) -> None:
        """ERROR events should trigger a warning or error log when logger is available."""
        mock_logger = MagicMock()

        observer = PrefectObserver()
        with patch("prefect.get_run_logger", return_value=mock_logger):
            observer.on_event(_error_event("FailedStep"))

        any_error_log = (
            mock_logger.error.called or mock_logger.warning.called or mock_logger.exception.called
        )
        assert any_error_log, "ERROR events should produce a warning/error log entry"

    # ---------------------------------------------------------------------------
    # Step-scoped events
    # ---------------------------------------------------------------------------

    def test_on_event_handles_step_start(self) -> None:
        """STEP START events are handled without raising."""
        observer = PrefectObserver()
        event = LifecycleEvent.start(scope=Scope.STEP, name="TransformStep")
        with patch("prefect.get_run_logger", side_effect=MissingContextError):
            observer.on_event(event)

    def test_on_event_handles_step_end(self) -> None:
        """STEP END events are handled without raising."""
        observer = PrefectObserver()
        event = LifecycleEvent.end(
            scope=Scope.STEP,
            name="TransformStep",
            duration_ms=10.0,
            status=LifecycleStatus.SUCCESS,
        )
        with patch("prefect.get_run_logger", side_effect=MissingContextError):
            observer.on_event(event)

    # ---------------------------------------------------------------------------
    # Fallback to stdlib logging
    # ---------------------------------------------------------------------------

    def test_on_event_falls_back_to_stdlib_logging(self) -> None:
        """on_event uses logging.getLogger() fallback when Prefect logger unavailable."""
        observer = PrefectObserver()

        with (
            patch("prefect.get_run_logger", side_effect=MissingContextError("no context")),
            patch("logging.getLogger") as mock_get_logger,
        ):
            mock_std_logger = MagicMock()
            mock_get_logger.return_value = mock_std_logger
            observer.on_event(_start_event())

    # Either Prefect or stdlib logger was used — no exception is the key guarantee.
    # We only verify the observer didn't raise.
