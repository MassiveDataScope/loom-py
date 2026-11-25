"""Contract tests for loom.core.runner."""

from __future__ import annotations

import logging

import pytest

import loom.core.runner as runner_module
from loom.core.runner import (
    RunnerEvent,
    SupportsFlush,
    SupportsShutdown,
    flush_runner,
    shutdown_runner,
)
from tests.helpers.loggers import CapturedLog


class _ShutdownCapable:
    def __init__(self) -> None:
        self.shutdown_calls = 0

    def shutdown(self) -> None:
        self.shutdown_calls += 1


class _FlushCapable:
    def __init__(self) -> None:
        self.flush_calls = 0

    def flush(self) -> None:
        self.flush_calls += 1


class _BrokenShutdown:
    def shutdown(self) -> None:
        raise RuntimeError("shutdown failed")


class _BrokenFlush:
    def flush(self) -> None:
        raise RuntimeError("flush failed")


# ---------------------------------------------------------------------------
# Public API contract
# ---------------------------------------------------------------------------


class TestRunnerModuleExports:
    def test_module_exports_only_lifecycle_capabilities(self) -> None:
        assert set(runner_module.__all__) == {
            "SupportsFlush",
            "SupportsShutdown",
            "flush_runner",
            "shutdown_runner",
        }


class TestRunnerCapabilityDispatch:
    def test_shutdown_runner_calls_structural_capability(self) -> None:
        runner = _ShutdownCapable()

        assert isinstance(runner, SupportsShutdown)

        shutdown_runner(runner)

        assert runner.shutdown_calls == 1

    def test_flush_runner_calls_structural_capability(self) -> None:
        runner = _FlushCapable()

        assert isinstance(runner, SupportsFlush)

        flush_runner(runner)

        assert runner.flush_calls == 1

    def test_lifecycle_helpers_are_noop_for_objects_without_capabilities(self) -> None:
        shutdown_runner(object())
        flush_runner(object())


class TestRunnerExceptionSafety:
    def test_shutdown_runner_suppresses_exception_and_logs_warning(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="loom.core.runner"):
            shutdown_runner(_BrokenShutdown())  # must not raise

        assert RunnerEvent.SHUTDOWN_FAILED in CapturedLog(caplog).rendered_text
        assert "_BrokenShutdown" in CapturedLog(caplog).rendered_text

    def test_flush_runner_suppresses_exception_and_logs_warning(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.WARNING, logger="loom.core.runner"):
            flush_runner(_BrokenFlush())  # must not raise

        assert RunnerEvent.FLUSH_FAILED in CapturedLog(caplog).rendered_text
        assert "_BrokenFlush" in CapturedLog(caplog).rendered_text

    def test_original_exception_is_preserved_when_shutdown_raises_in_finally(self) -> None:
        def _pipeline_fails_then_shuts_down() -> None:
            try:
                raise ValueError("pipeline error")
            finally:
                shutdown_runner(_BrokenShutdown())

        with pytest.raises(ValueError, match="pipeline error"):
            _pipeline_fails_then_shuts_down()

    def test_original_exception_is_preserved_when_flush_raises_in_finally(self) -> None:
        def _pipeline_fails_then_flushes() -> None:
            try:
                raise ValueError("pipeline error")
            finally:
                flush_runner(_BrokenFlush())

        with pytest.raises(ValueError, match="pipeline error"):
            _pipeline_fails_then_flushes()
