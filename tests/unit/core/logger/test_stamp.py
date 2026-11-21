from __future__ import annotations

import logging

import pytest

from loom.core.logger import (
    ContextFilter,
    LogField,
    LogIdentity,
    LogRuntime,
    log_context,
)
from loom.core.tracing.context import active_trace_id
from tests.helpers.loggers import CapturingHandler

_IDENTITY = LogIdentity(
    environment="production",
    runtime=LogRuntime.FASTAPI,
    service_name="billing",
    service_version="2.4.0",
)


@pytest.fixture
def stamped(handler: CapturingHandler) -> CapturingHandler:
    handler.addFilter(ContextFilter(_IDENTITY))
    return handler


def _only(handler: CapturingHandler) -> logging.LogRecord:
    assert len(handler.records) == 1
    return handler.records[0]


class Greeter:
    def greet(self) -> None:
        logging.getLogger("tests.stamp.greeter").info("greeter.greeting_sent")


class TestContextFilter:
    def test_a_record_carries_the_identity_of_the_emitter(self, stamped: CapturingHandler) -> None:
        logging.getLogger("tests.stamp").info("billing.invoice_issued")

        record = _only(stamped)

        assert record.__dict__[LogField.SERVICE_NAME] == "billing"
        assert record.__dict__[LogField.SERVICE_VERSION] == "2.4.0"
        assert record.__dict__[LogField.ENVIRONMENT] == "production"
        assert record.__dict__[LogField.RUNTIME] == "fastapi"

    def test_an_identity_without_a_version_stamps_no_version(
        self, handler: CapturingHandler
    ) -> None:
        handler.addFilter(ContextFilter(LogIdentity(environment="test")))

        logging.getLogger("tests.stamp").info("billing.invoice_issued")

        assert LogField.SERVICE_VERSION.value not in _only(handler).__dict__

    def test_the_active_trace_id_is_stamped(self, stamped: CapturingHandler) -> None:
        with active_trace_id("trace-42"):
            logging.getLogger("tests.stamp").info("billing.invoice_issued")

        assert _only(stamped).__dict__[LogField.TRACE_ID] == "trace-42"

    def test_the_fields_of_the_running_job_are_stamped(self, stamped: CapturingHandler) -> None:
        job = {LogField.JOB: "SendInvoice", LogField.TASK_ID: "t-9", LogField.ATTEMPT: 3}

        with log_context(job):
            logging.getLogger("tests.stamp").info("billing.invoice_issued")

        record = _only(stamped).__dict__
        assert (record[LogField.JOB], record[LogField.TASK_ID], record[LogField.ATTEMPT]) == (
            "SendInvoice",
            "t-9",
            3,
        )

    def test_a_record_of_a_library_logger_is_stamped_the_same(
        self, stamped: CapturingHandler
    ) -> None:
        with active_trace_id("trace-42"), log_context({LogField.USE_CASE: "CreateInvoice"}):
            logging.getLogger("botocore.credentials").info("Found credentials")

        record = _only(stamped).__dict__
        assert record[LogField.SERVICE_NAME] == "billing"
        assert record[LogField.TRACE_ID] == "trace-42"
        assert record[LogField.USE_CASE] == "CreateInvoice"

    def test_a_field_given_as_extra_is_kept(self, stamped: CapturingHandler) -> None:
        with active_trace_id("from-context"):
            logging.getLogger("tests.stamp").info(
                "billing.invoice_issued", extra={LogField.TRACE_ID: "from-extra"}
            )

        assert _only(stamped).__dict__[LogField.TRACE_ID] == "from-extra"

    def test_a_record_from_a_method_says_where_it_was_emitted(
        self, stamped: CapturingHandler
    ) -> None:
        Greeter().greet()

        record = _only(stamped).__dict__
        assert record[LogField.CODE_FUNCTION] == "greet"
        assert record[LogField.CODE_LINE] == Greeter.greet.__code__.co_firstlineno + 1
        assert record[LogField.CODE_FILE] == __file__

    def test_the_runtime_is_changed_after_the_filter_was_built(
        self, handler: CapturingHandler
    ) -> None:
        stamping = ContextFilter(_IDENTITY)
        handler.addFilter(stamping)

        stamping.use_runtime(LogRuntime.CELERY)
        logging.getLogger("tests.stamp").info("billing.invoice_issued")

        assert _only(handler).__dict__[LogField.RUNTIME] == "celery"
