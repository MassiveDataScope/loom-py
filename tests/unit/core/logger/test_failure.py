"""The failure policy: how a failure is described, when it is logged, what leaves the process."""

from __future__ import annotations

import logging
import pickle
from collections.abc import Iterator, Mapping

import pytest

from loom.core.errors import NotFound, RuleViolationError, SystemError
from loom.core.job.outcome import JobAbandonment
from loom.core.logger import LogField, LogIdentity, LogKey, Scalar
from loom.core.logger.failure import (
    FailureReference,
    exception_stacktrace,
    failure_fields,
    failure_level,
    is_stack_written,
    public_form,
    reference,
    should_report,
)
from loom.core.logger.filter import LogFilter
from loom.core.logger.install import install_record_factory
from loom.core.model.redaction import MASK, Secret
from loom.core.tracing.context import active_trace_id
from tests.helpers.loggers import CapturingHandler

_SECRET = "iban ES91 2100 0418 4502 0005 1332"
_log = logging.getLogger("tests.failure")


class _Declaring(SystemError):
    def log_fields(self) -> Mapping[LogKey, Scalar]:
        return {LogField.COUNT: 3, LogField.SETTING: Secret(_SECRET)}  # type: ignore[dict-item]


@pytest.fixture
def stamped(handler: CapturingHandler) -> Iterator[CapturingHandler]:
    install_record_factory(LogIdentity(environment="test"))
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


class TestPublicForm:
    @pytest.mark.parametrize(
        ("error", "form"),
        [
            (ValueError(_SECRET), "ValueError"),
            (NotFound("Invoice", id=_SECRET), "not_found"),
            (_Declaring(_SECRET), f"system_error loom.count=3 loom.setting={MASK}"),
        ],
    )
    def test_a_failure_is_its_code_and_declared_fields_or_its_type(
        self, error: BaseException, form: str
    ) -> None:
        assert public_form(error) == form

    def test_the_declared_fields_are_logged_beside_the_type_and_code(self) -> None:
        fields = failure_fields(_Declaring(_SECRET), stacktrace=False)

        assert fields[LogField.COUNT] == 3
        assert fields[LogField.SETTING] == MASK


class TestStacktrace:
    def test_every_link_of_the_chain_is_written_with_frames_and_types_only(self) -> None:
        stack = exception_stacktrace(_chained())

        assert _SECRET not in stack
        assert stack.index("KeyError") < stack.index("ValueError") < stack.index("RuntimeError")
        assert "direct cause" in stack
        assert "During handling" in stack
        assert stack.count("Traceback (most recent call last):") == 3

    def test_an_exception_group_writes_each_member_type_and_frames_and_no_message(self) -> None:
        group = _raised(
            BaseExceptionGroup(_SECRET, [_raised(ValueError(_SECRET)), KeyError(_SECRET)])
        )

        stack = exception_stacktrace(group)

        assert _SECRET not in stack
        assert "  | ValueError" in stack
        assert "  | KeyError" in stack
        assert stack.count("Traceback (most recent call last):") == 2

    def test_the_chain_stops_at_a_link_whose_stack_is_written(
        self, stamped: CapturingHandler
    ) -> None:
        cause = _raised(KeyError(_SECRET))
        _log.error("billing.lookup_failed", exc_info=cause)
        try:
            raise ValueError(_SECRET) from cause
        except ValueError as error:
            stack = exception_stacktrace(error)

        assert stack.startswith("KeyError\n")
        assert stack.count("Traceback (most recent call last):") == 1


class TestOnceStack:
    def test_the_stack_of_one_exception_is_written_once(self, stamped: CapturingHandler) -> None:
        error = _raised(ValueError(_SECRET))

        _log.error("billing.invoice_failed", exc_info=error)
        _log.error("billing.batch_failed", exc_info=error)

        first, second = (r.__dict__ for r in stamped.records)
        assert LogField.EXCEPTION_STACKTRACE in first
        assert LogField.EXCEPTION_STACKTRACE not in second
        assert second[LogField.EXCEPTION_TYPE] == "ValueError"
        assert is_stack_written(error)

    @pytest.mark.parametrize(
        ("error", "level"),
        [
            (NotFound("Invoice", id=1), logging.INFO),
            (RuleViolationError("amount", "negative"), logging.INFO),
            (JobAbandonment("gave up"), logging.INFO),
            (ValueError("x"), logging.ERROR),
        ],
    )
    def test_the_level_of_a_failure_follows_what_the_exception_declares(
        self, error: BaseException, level: int
    ) -> None:
        assert failure_level(error) == level

    def test_a_failure_whose_stack_is_written_is_reported_at_warning_after(
        self, stamped: CapturingHandler
    ) -> None:
        error = _raised(ValueError("x"))
        _log.error("billing.invoice_failed", exc_info=error)

        assert failure_level(error) == logging.WARNING
        assert not should_report(error)


class TestReference:
    def test_it_logs_an_unreported_failure_once_and_names_its_record(
        self, stamped: CapturingHandler
    ) -> None:
        error = _raised(ValueError(_SECRET))

        with active_trace_id("t-1"):
            handed_off = reference(error)
            reference(error)

        (record,) = stamped.records
        assert record.getMessage() == "logger.failure_handed_off"
        assert record.__dict__[LogField.TRACE_ID] == "t-1"
        assert (handed_off.exception_type, handed_off.trace_id) == ("ValueError", "t-1")
        assert _SECRET not in str(handed_off) + repr(handed_off)

    def test_an_expected_failure_is_not_logged(self, stamped: CapturingHandler) -> None:
        handed_off = reference(NotFound("Invoice", id=_SECRET))

        assert stamped.records == []
        assert (handed_off.code, handed_off.expected, handed_off.retryable) == (
            "not_found",
            True,
            False,
        )

    def test_the_reference_of_a_reference_is_itself(self) -> None:
        handed_off = reference(NotFound("Invoice", id=1))

        assert reference(handed_off) is handed_off

    def test_it_pickles_with_nothing_of_the_original(self) -> None:
        handed_off = reference(NotFound("Invoice", id=_SECRET))

        restored = pickle.loads(pickle.dumps(handed_off))

        assert isinstance(restored, FailureReference)
        assert str(restored) == str(handed_off)
        assert _SECRET.encode() not in pickle.dumps(handed_off)

    def test_raised_after_the_except_block_it_has_no_context(self) -> None:
        try:
            raise ValueError(_SECRET)
        except ValueError as error:
            handed_off = reference(error)

        with pytest.raises(FailureReference) as raised:
            raise handed_off

        assert raised.value.__context__ is None
        assert raised.value.__cause__ is None
