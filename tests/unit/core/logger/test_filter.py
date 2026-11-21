"""Value rendering: what a field value becomes, and the filter that applies it on handlers."""

from __future__ import annotations

import logging

import pytest

from loom.core.environment import Environment
from loom.core.logger import LogField, LogIdentity, current_log_fields, log_context
from loom.core.logger.filter import LogFilter
from loom.core.logger.install import configure_logging
from loom.core.logger.record import LoomRecordFactory
from loom.core.logger.settings import LogSettings


class _Card:
    def __repr__(self) -> str:
        return "Card(4111-1111-1111-1111)"


class TestLogFilter:
    @pytest.fixture
    def record(self) -> logging.LogRecord:
        LoomRecordFactory.install(LogIdentity(environment="test", service_name="billing"))
        record = logging.getLogger("tests.filter").makeRecord(
            "tests.filter",
            logging.INFO,
            __file__,
            1,
            "billing.invoice_issued",
            (),
            None,
            extra={LogField.COUNT: _Card(), LogField.REASON: None},
        )
        return record

    def test_values_are_rendered_and_a_none_value_is_not_written(
        self, record: logging.LogRecord
    ) -> None:
        LogFilter().filter(record)

        assert record.__dict__[LogField.COUNT] == "_Card"
        assert LogField.REASON not in record.__dict__
        assert record.__dict__[LogField.SERVICE_NAME] == "billing"

    def test_a_second_filter_leaves_the_record_as_the_first_wrote_it(
        self, record: logging.LogRecord
    ) -> None:
        LogFilter().filter(record)
        written = dict(record.__dict__)

        assert LogFilter().filter(record)
        assert record.__dict__ == written

    def test_a_record_no_loom_factory_created_passes_as_it_is(self) -> None:
        record = logging.LogRecord("acme", logging.INFO, "", 0, "acme.thing_done", (), None)
        record.amount = _Card()

        assert LogFilter().filter(record)
        assert isinstance(record.__dict__["amount"], _Card)

    def test_logging_never_raises_once_configured(self) -> None:
        configure_logging(LogSettings(format="json"), environment=Environment.PRODUCTION)

        assert logging.raiseExceptions is False


class TestLogContextValues:
    def test_a_context_value_is_rendered_once_when_the_block_opens(self) -> None:
        with log_context({LogField.NAME: _Card(), LogField.REASON: None}):
            fields = current_log_fields()

        assert fields == {LogField.NAME.value: "_Card"}
