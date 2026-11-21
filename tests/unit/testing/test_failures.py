"""``logged_failure`` finds the record that describes a failure a handoff replaced."""

from __future__ import annotations

import logging

import pytest

from loom.core.errors import NotFound
from loom.core.logger.failure import FailureReference, reference
from loom.core.logger.vocabulary import LogField
from loom.core.tracing import reset_trace_id, set_trace_id
from loom.testing.failures import logged_failure


def _handed_off(failure: Exception) -> FailureReference:
    try:
        raise failure
    except Exception as exc:
        handed_off = reference(exc)
    return handed_off


class TestLoggedFailure:
    def test_it_returns_the_record_with_the_trace_and_type_of_the_reference(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        token = set_trace_id("t-1")
        try:
            handed_off = _handed_off(ValueError("detail"))
        finally:
            reset_trace_id(token)

        record = logged_failure(caplog.records, handed_off)

        assert record.__dict__[LogField.EXCEPTION_TYPE] == "ValueError"
        assert LogField.EXCEPTION_STACKTRACE in record.__dict__

    def test_a_record_of_another_trace_does_not_describe_it(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        handed_off = _handed_off(ValueError("detail"))
        other = FailureReference("ValueError", None, "t-2", expected=False, retryable=True)

        assert logged_failure(caplog.records, handed_off).levelno == logging.ERROR
        with pytest.raises(LookupError, match="ValueError in trace t-2"):
            logged_failure(caplog.records, other)

    def test_an_expected_failure_that_nothing_logged_is_not_found(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        handed_off = _handed_off(NotFound("order", id=7))

        with pytest.raises(LookupError):
            logged_failure(caplog.records, handed_off)
