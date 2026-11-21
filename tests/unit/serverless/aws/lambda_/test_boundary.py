"""The Lambda edges: a failure reaches the runtime as a reference, and is logged once."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.core.errors import NotFound
from loom.core.logger import LogField, LogIdentity
from loom.core.logger.failure import FailureReference
from loom.core.logger.install import install_record_factory
from loom.core.logger.log_events import LoggerLogEvent
from loom.serverless.aws.lambda_.boundary import LambdaBoundary, lambda_init
from tests.helpers.handoffs import lambda_failure

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
    install_record_factory(LogIdentity(environment="test"))
    caplog.set_level(logging.DEBUG, logger="loom.core.logger.failure")
    return caplog


def _reports(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage() == LoggerLogEvent.FAILURE_HANDED_OFF]


class TestLambdaBoundary:
    def test_the_runtime_gets_a_reference_named_after_the_type_and_no_message(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        with pytest.raises(FailureReference) as raised:
            LambdaBoundary(_Raising(ValueError(_SECRET)))({}, None)  # type: ignore[arg-type]

        assert type(raised.value).__name__ == "ValueError"
        assert raised.value.__context__ is None
        assert _SECRET not in str(raised.value) + repr(raised.value)
        [record] = _reports(boundary_log)
        assert (record.levelno, record.__dict__[LogField.EXCEPTION_TYPE]) == (
            logging.ERROR,
            "ValueError",
        )
        assert _SECRET not in boundary_log.text

    def test_an_expected_failure_is_handed_off_unlogged(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        with lambda_failure(NotFound):
            LambdaBoundary(_Raising(NotFound("Patient", id=42)))({}, None)  # type: ignore[arg-type]

        assert _reports(boundary_log) == []

    def test_a_failure_a_scope_already_logged_is_not_logged_again(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        error = ValueError(_SECRET)
        logging.getLogger("acme").error("acme.step_failed", exc_info=error)

        with lambda_failure(ValueError):
            LambdaBoundary(_Raising(error))({}, None)  # type: ignore[arg-type]

        assert _reports(boundary_log) == []


class TestLambdaInit:
    def test_init_installs_the_factory_first_and_hands_off_a_failure(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        def build() -> None:
            raise RuntimeError(_SECRET)

        with lambda_failure(RuntimeError, match=_SECRET):
            lambda_init(build)

        [record] = _reports(boundary_log)
        assert record.__dict__[LogField.EXCEPTION_TYPE] == "RuntimeError"
        assert _SECRET not in boundary_log.text
