"""The record factory: the one owner of the exception policy and of the record's stamp."""

from __future__ import annotations

import logging
from collections.abc import Iterator

import pytest

from loom.core.errors import NotFound, SystemError
from loom.core.logger import LogField, LogIdentity, LogRuntime, log_context
from loom.core.logger.filter import LogFilter
from loom.core.logger.install import install_record_factory
from loom.core.logger.record import LoomRecordFactory
from loom.core.tracing.context import active_trace_id
from tests.helpers.loggers import CapturingHandler

_SECRET = "iban ES91 2100 0418 4502 0005 1332"
_log = logging.getLogger("tests.record")


@pytest.fixture
def stamped(handler: CapturingHandler) -> Iterator[CapturingHandler]:
    """A root handler with loom's filter, behind loom's record factory."""
    install_record_factory(
        LogIdentity(environment="test", runtime=LogRuntime.CLI, service_name="billing")
    )
    handler.addFilter(LogFilter())
    yield handler


def _raised(exc: BaseException) -> BaseException:
    try:
        raise exc
    except BaseException as caught:
        return caught


def _chained() -> BaseException:
    try:
        try:
            raise KeyError(_SECRET)
        except KeyError as cause:
            raise ValueError(_SECRET) from cause
    except ValueError:
        try:
            raise RuntimeError(_SECRET)
        except RuntimeError as outer:
            outer.add_note(_SECRET)
            return outer


class TestStamp:
    def test_a_record_carries_the_emitter_the_trace_and_the_code_origin(
        self, stamped: CapturingHandler
    ) -> None:
        with active_trace_id("t" * 32), log_context({LogField.JOB: "SendInvoice"}):
            _log.info("billing.invoice_issued")

        (record,) = stamped.records
        fields = record.__dict__
        assert (fields[LogField.SERVICE_NAME], fields[LogField.RUNTIME]) == ("billing", "cli")
        assert (fields[LogField.TRACE_ID], fields[LogField.JOB]) == ("t" * 32, "SendInvoice")
        assert fields[LogField.CODE_FUNCTION] == (
            "test_a_record_carries_the_emitter_the_trace_and_the_code_origin"
        )

    def test_an_extra_key_named_like_a_stamped_field_does_not_raise_and_wins(
        self, stamped: CapturingHandler
    ) -> None:
        with active_trace_id("t" * 32):
            _log.info("billing.invoice_issued", extra={LogField.TRACE_ID: "given"})

        assert stamped.records[0].__dict__[LogField.TRACE_ID] == "given"

    def test_a_second_installation_replaces_the_first_instead_of_stacking(self) -> None:
        first = install_record_factory(LogIdentity(environment="test"))

        second = install_record_factory(LogIdentity(environment="test"))

        assert logging.getLogRecordFactory() is second
        assert second.previous is first.previous
        assert not isinstance(second.previous, LoomRecordFactory)

    def test_a_record_below_the_level_is_never_created(self, stamped: CapturingHandler) -> None:
        calls: list[str] = []
        factory = logging.getLogRecordFactory()

        def counting(*args: object, **kwargs: object) -> logging.LogRecord:
            calls.append("created")
            return factory(*args, **kwargs)

        logging.setLogRecordFactory(counting)
        _log.setLevel(logging.INFO)
        try:
            _log.debug("billing.invoice_drafted")
        finally:
            _log.setLevel(logging.NOTSET)

        assert calls == []


class TestExceptionPolicy:
    def test_an_error_carries_the_type_the_code_and_a_stack_and_no_exc_info(
        self, stamped: CapturingHandler
    ) -> None:
        error = _raised(SystemError(_SECRET))

        _log.error("billing.invoice_failed", exc_info=error)

        (record,) = stamped.records
        assert (record.exc_info, record.exc_text, record.stack_info) == (None, None, None)
        assert record.__dict__[LogField.EXCEPTION_TYPE] == "loom.core.errors.base.SystemError"
        assert record.__dict__[LogField.ERROR_CODE] == "system_error"
        assert "test_record.py" in record.__dict__[LogField.EXCEPTION_STACKTRACE]

    def test_an_expected_failure_writes_no_stack_even_at_error(
        self, stamped: CapturingHandler
    ) -> None:
        _log.error("billing.invoice_failed", exc_info=_raised(NotFound("Invoice", id=1)))

        assert LogField.EXCEPTION_STACKTRACE not in stamped.records[0].__dict__

    def test_below_error_the_exception_is_its_type_only(self, stamped: CapturingHandler) -> None:
        _log.warning("billing.invoice_retried", exc_info=_raised(ValueError(_SECRET)))

        fields = stamped.records[0].__dict__
        assert fields[LogField.EXCEPTION_TYPE] == "ValueError"
        assert LogField.EXCEPTION_STACKTRACE not in fields

    def test_stack_info_is_cleared(self, stamped: CapturingHandler) -> None:
        _log.info("billing.invoice_issued", stack_info=True)

        assert stamped.records[0].stack_info is None

    def test_extra_cannot_forge_the_failure_fields(self, stamped: CapturingHandler) -> None:
        _log.error(
            "billing.invoice_failed",
            exc_info=_raised(ValueError(_SECRET)),
            extra={LogField.EXCEPTION_TYPE: "Forged"},
        )

        assert stamped.records[0].__dict__[LogField.EXCEPTION_TYPE] == "ValueError"


class TestExceptionAsText:
    @pytest.mark.parametrize(
        ("message", "args"),
        [
            (ValueError(_SECRET), ()),
            ("billing.invoice_failed %r", (ValueError(_SECRET),)),
            ("billing.invoice_failed %(error)s", ({"error": ValueError(_SECRET)},)),
        ],
    )
    def test_an_exception_in_the_message_or_the_arguments_is_its_public_form(
        self, stamped: CapturingHandler, message: object, args: tuple[object, ...]
    ) -> None:
        _log.error(message, *args)

        text = stamped.records[0].getMessage()
        assert _SECRET not in text
        assert "ValueError" in text

    def test_a_captured_warning_keeps_only_its_category(self, stamped: CapturingHandler) -> None:
        logging.captureWarnings(True)

        import warnings

        warnings.warn(_SECRET, DeprecationWarning, stacklevel=1)

        (record,) = [r for r in stamped.records if r.name == "py.warnings"]
        assert record.getMessage() == "DeprecationWarning"
