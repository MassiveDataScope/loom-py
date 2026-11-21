"""The shared assertion on what a call logged."""

from __future__ import annotations

import logging

import pytest

from loom.core.logger import LogEvent, LogField
from tests.helpers.log_standard import assert_logged


class _SampleLogEvent(LogEvent, prefix="sample"):
    DONE = "sample.thing_done"
    SKIPPED = "sample.thing_skipped"


_log = logging.getLogger("loom.sample")


class TestAssertLogged:
    def test_the_record_with_its_level_and_fields_passes(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        _log.warning(_SampleLogEvent.DONE, extra={LogField.JOB: "SendInvoice", LogField.ATTEMPT: 2})

        record = assert_logged(
            caplog,
            _SampleLogEvent.DONE,
            level=logging.WARNING,
            fields={LogField.JOB: "SendInvoice"},
        )

        assert record.levelno == logging.WARNING

    def test_another_level_fails(self, caplog: pytest.LogCaptureFixture) -> None:
        _log.info(_SampleLogEvent.DONE)

        with pytest.raises(pytest.fail.Exception, match="logged at INFO"):
            assert_logged(caplog, _SampleLogEvent.DONE, level=logging.WARNING)

    def test_a_different_field_value_fails(self, caplog: pytest.LogCaptureFixture) -> None:
        _log.info(_SampleLogEvent.DONE, extra={LogField.JOB: "SendInvoice"})

        with pytest.raises(pytest.fail.Exception, match="carries"):
            assert_logged(
                caplog, _SampleLogEvent.DONE, level=logging.INFO, fields={LogField.JOB: "Other"}
            )

    def test_an_event_never_logged_fails(self, caplog: pytest.LogCaptureFixture) -> None:
        _log.info(_SampleLogEvent.DONE)

        with pytest.raises(pytest.fail.Exception, match="got 0"):
            assert_logged(caplog, _SampleLogEvent.SKIPPED, level=logging.INFO)

    def test_an_event_logged_twice_fails(self, caplog: pytest.LogCaptureFixture) -> None:
        _log.info(_SampleLogEvent.DONE)
        _log.info(_SampleLogEvent.DONE)

        with pytest.raises(pytest.fail.Exception, match="got 2"):
            assert_logged(caplog, _SampleLogEvent.DONE, level=logging.INFO)
