"""The Lambda edges: a failure reaches the runtime as a clean loom exception, logged once."""

from __future__ import annotations

import logging
import sys
import threading
from typing import Any

import pytest

from loom.core.errors import NotFound
from loom.core.errors.log_events import ErrorLogEvent, ErrorLogField
from loom.core.logger import LogIdentity
from loom.core.logger.record import LoomRecordFactory
from loom.serverless.aws.lambda_.boundary import LambdaBoundary, lambda_init
from tests.helpers.boundary import left_loom

_SECRET = "patient 0042"


class _Raising:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def __call__(self, event: dict[str, Any], context: Any) -> Any:
        raise self.error

    def close(self) -> None:
        """Nothing to close."""


@pytest.fixture
def boundary_log(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    LoomRecordFactory.install(LogIdentity(environment="test"))
    caplog.set_level(logging.DEBUG, logger="loom.core.errors.failure")
    return caplog


@pytest.fixture
def kept_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Lambda Init installs the process hooks for good: put pytest's back after the test."""
    monkeypatch.setattr(sys, "excepthook", sys.excepthook)
    monkeypatch.setattr(threading, "excepthook", threading.excepthook)
    monkeypatch.setattr(sys, "unraisablehook", sys.unraisablehook)


def _reports(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage() == ErrorLogEvent.FAILURE_LEFT_LOOM]


class TestLambdaBoundary:
    def test_the_runtime_gets_the_failure_without_its_message(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        with left_loom(ValueError) as left:
            LambdaBoundary(_Raising(ValueError(_SECRET)))({}, None)  # type: ignore[arg-type]

        assert _SECRET not in str(left.value) + repr(left.value)
        [record] = _reports(boundary_log)
        assert (record.levelno, record.__dict__[ErrorLogField.EXCEPTION_TYPE]) == (
            logging.ERROR,
            "ValueError",
        )
        assert _SECRET not in boundary_log.text

    def test_an_expected_failure_leaves_unlogged(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        with left_loom(NotFound):
            LambdaBoundary(_Raising(NotFound("Patient", id=42)))({}, None)  # type: ignore[arg-type]

        assert _reports(boundary_log) == []

    def test_a_failure_a_scope_already_logged_is_not_logged_again(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        error = ValueError(_SECRET)
        logging.getLogger("acme").error("acme.step_failed", exc_info=error)

        with left_loom(ValueError):
            LambdaBoundary(_Raising(error))({}, None)  # type: ignore[arg-type]

        assert _reports(boundary_log) == []


class TestLambdaInit:
    @pytest.mark.usefixtures("kept_hooks")
    def test_a_failing_init_is_logged_once_without_its_message(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        def build() -> None:
            raise RuntimeError(_SECRET)

        with left_loom(RuntimeError):
            lambda_init(build)

        [record] = _reports(boundary_log)
        assert record.__dict__[ErrorLogField.EXCEPTION_TYPE] == "RuntimeError"
        assert _SECRET not in boundary_log.text
