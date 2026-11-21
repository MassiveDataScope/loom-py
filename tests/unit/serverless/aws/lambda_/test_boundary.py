"""The Lambda boundary: a failure reaches the runtime with no message, and is logged once."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.core.errors import NotFound
from loom.core.logger import LogField, LogIdentity
from loom.core.logger.install import install_record_factory
from loom.serverless.aws.lambda_.boundary import LambdaBoundary
from loom.serverless.aws.lambda_.log_events import LambdaLogEvent
from tests.helpers.lambda_failures import lambda_failure

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
    caplog.set_level(logging.DEBUG, logger="loom.serverless.aws.lambda_.boundary")
    return caplog


def _reports(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage() == LambdaLogEvent.INVOCATION_FAILED]


class TestLambdaBoundary:
    def test_the_runtime_gets_the_type_and_code_and_no_message(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        with lambda_failure(ValueError) as replaced:
            LambdaBoundary(_Raising(ValueError(_SECRET)))({}, None)  # type: ignore[arg-type]

        assert isinstance(replaced.value, ValueError)
        [record] = _reports(boundary_log)
        assert (record.levelno, record.__dict__[LogField.EXCEPTION_TYPE]) == (
            logging.ERROR,
            "ValueError",
        )
        assert _SECRET not in boundary_log.text

    def test_an_expected_failure_is_reported_at_info_with_no_stack(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        with lambda_failure(NotFound):
            LambdaBoundary(_Raising(NotFound("Patient", id=42)))({}, None)  # type: ignore[arg-type]

        [record] = _reports(boundary_log)
        assert record.levelno == logging.INFO
        assert LogField.EXCEPTION_STACKTRACE not in record.__dict__

    def test_a_failure_a_scope_already_logged_is_not_logged_again(
        self, boundary_log: pytest.LogCaptureFixture
    ) -> None:
        error = ValueError(_SECRET)
        logging.getLogger("acme").error("acme.step_failed", exc_info=error)

        with lambda_failure(ValueError):
            LambdaBoundary(_Raising(error))({}, None)  # type: ignore[arg-type]

        assert _reports(boundary_log) == []
