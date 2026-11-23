from __future__ import annotations

import logging

import pytest

from loom.core.errors import NotFound
from loom.core.logger import (
    ContextFilter,
    LogField,
    LogIdentity,
    LogRuntime,
    log_context,
)
from loom.core.logger.stamp import MAX_STACK_LENGTH, STACK_CUT_MARKER
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

    def test_a_record_without_an_exception_gets_no_exception_type(
        self, stamped: CapturingHandler
    ) -> None:
        logging.getLogger("tests.stamp").info("billing.invoice_issued")

        assert LogField.EXCEPTION_TYPE.value not in _only(stamped).__dict__

    def test_the_trace_wins_over_the_context_and_the_context_over_the_identity(
        self, stamped: CapturingHandler
    ) -> None:
        shadowing = {LogField.TRACE_ID: "from-context", LogField.SERVICE_NAME: "from-context"}

        with active_trace_id("from-trace"), log_context(shadowing):
            logging.getLogger("tests.stamp").info("billing.invoice_issued")

        record = _only(stamped).__dict__
        assert record[LogField.TRACE_ID] == "from-trace"
        assert record[LogField.SERVICE_NAME] == "from-context"

    def test_the_runtime_is_changed_after_the_filter_was_built(
        self, handler: CapturingHandler
    ) -> None:
        stamping = ContextFilter(_IDENTITY)
        handler.addFilter(stamping)

        stamping.use_runtime(LogRuntime.CELERY)
        logging.getLogger("tests.stamp").info("billing.invoice_issued")

        assert _only(handler).__dict__[LogField.RUNTIME] == "celery"


def _log_failure(level: int, failure: Exception, **extra: object) -> None:
    try:
        raise failure
    except Exception:
        logging.getLogger("tests.stamp").log(
            level, "billing.invoice_failed", exc_info=True, extra=extra
        )


class TestFailureFields:
    """The filter is the one owner of what a record says about its exception."""

    def test_a_record_names_the_type_and_never_the_message(self, stamped: CapturingHandler) -> None:
        _log_failure(logging.WARNING, ValueError("secret row value"))

        record = _only(stamped).__dict__
        assert record[LogField.EXCEPTION_TYPE] == "ValueError"
        assert "secret row value" not in {str(value) for value in record.values()}
        assert LogField.EXCEPTION_STACKTRACE.value not in record

    def test_a_loom_error_adds_its_code(self, stamped: CapturingHandler) -> None:
        _log_failure(logging.WARNING, NotFound("Invoice", id=7))

        assert _only(stamped).__dict__[LogField.ERROR_CODE] == "not_found"

    def test_from_error_up_the_stack_is_written(self, stamped: CapturingHandler) -> None:
        _log_failure(logging.ERROR, ValueError("amount below zero"))

        stack = _only(stamped).__dict__[LogField.EXCEPTION_STACKTRACE]
        assert stack.startswith("Traceback (most recent call last):")
        assert stack.endswith("ValueError: amount below zero")

    def test_a_long_stack_keeps_its_end(self, stamped: CapturingHandler) -> None:
        _log_failure(logging.ERROR, ValueError("x" * MAX_STACK_LENGTH))

        stack = _only(stamped).__dict__[LogField.EXCEPTION_STACKTRACE]
        assert len(stack) == MAX_STACK_LENGTH
        assert stack.startswith(STACK_CUT_MARKER)
        assert stack.endswith("x" * 100)

    def test_a_chained_failure_is_one_stack_with_its_cause(self, stamped: CapturingHandler) -> None:
        try:
            try:
                raise KeyError("row")
            except KeyError as cause:
                raise ValueError("invoice") from cause
        except ValueError:
            logging.getLogger("tests.stamp").error("billing.invoice_failed", exc_info=True)

        stack = _only(stamped).__dict__[LogField.EXCEPTION_STACKTRACE]
        assert "KeyError" in stack
        assert stack.endswith("ValueError: invoice")

    def test_every_handler_of_a_record_shares_one_formatted_stack(
        self, stamped: CapturingHandler
    ) -> None:
        second = CapturingHandler()
        second.addFilter(ContextFilter(_IDENTITY))
        logger = logging.getLogger("tests.stamp")
        logger.addHandler(second)
        try:
            _log_failure(logging.ERROR, ValueError("boom"))
        finally:
            logger.removeHandler(second)

        record = _only(stamped)
        assert record is second.records[0]
        assert record.__dict__[LogField.EXCEPTION_STACKTRACE] == record.exc_text

    def test_exc_info_with_no_active_exception_writes_nothing(
        self, stamped: CapturingHandler
    ) -> None:
        logging.getLogger("tests.stamp").error("billing.invoice_failed", exc_info=True)

        record = _only(stamped).__dict__
        assert LogField.EXCEPTION_TYPE.value not in record
        assert LogField.EXCEPTION_STACKTRACE.value not in record

    @pytest.mark.parametrize("level", [logging.WARNING, logging.ERROR], ids=["warning", "error"])
    def test_extra_cannot_forge_the_failure_fields(
        self, stamped: CapturingHandler, level: int
    ) -> None:
        forged = {
            LogField.EXCEPTION_TYPE.value: "Forged",
            LogField.ERROR_CODE.value: "forged",
            LogField.EXCEPTION_STACKTRACE.value: "forged",
        }

        _log_failure(level, ValueError("boom"), **forged)

        record = _only(stamped).__dict__
        assert record[LogField.EXCEPTION_TYPE] == "ValueError"
        assert LogField.ERROR_CODE.value not in record
        assert record.get(LogField.EXCEPTION_STACKTRACE) != "forged"

    def test_a_record_without_an_exception_cannot_carry_a_stack(
        self, stamped: CapturingHandler
    ) -> None:
        stack = {LogField.EXCEPTION_STACKTRACE.value: "forged"}

        logging.getLogger("tests.stamp").info("billing.invoice_issued", extra=stack)

        assert LogField.EXCEPTION_STACKTRACE.value not in _only(stamped).__dict__
