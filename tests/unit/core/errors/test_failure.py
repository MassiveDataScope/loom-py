"""A failure, as loom describes it: its fields, its stack, its level and what leaves loom."""

from __future__ import annotations

import logging
import pickle
from types import MappingProxyType

import pytest

from loom.core.errors import NotFound, RuleViolationError, SystemError, UnexpectedError
from loom.core.errors.failure import Failure
from loom.core.errors.log_events import ErrorLogEvent, ErrorLogField
from loom.core.fields import MASK, Secret
from loom.core.job.outcome import JobAbandonment
from loom.core.logger import LogField

_SECRET = "iban ES91 2100 0418 4502 0005 1332"
_log = logging.getLogger("tests.failure")


class _Declaring(SystemError):
    template = "{count} settings failed, {setting} first"
    field_keys = MappingProxyType({"count": LogField.COUNT, "setting": LogField.SETTING})


@pytest.fixture
def stamped(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Every record, made by loom's record factory and seen through its filter."""
    caplog.set_level(logging.DEBUG)
    return caplog


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


class TestFields:
    def test_the_declared_fields_are_logged_beside_the_type_and_code(self) -> None:
        fields = Failure(_Declaring(count=3, setting=Secret(_SECRET))).fields()

        assert fields[LogField.COUNT] == 3
        assert fields[LogField.SETTING] == MASK
        assert fields[ErrorLogField.ERROR_CODE] == "system_error"
        assert str(fields[ErrorLogField.EXCEPTION_TYPE]).endswith("_Declaring")

    def test_a_foreign_exception_is_its_type_only(self) -> None:
        fields = Failure(ValueError(_SECRET)).fields()

        assert fields == {ErrorLogField.EXCEPTION_TYPE: "ValueError"}

    @pytest.mark.parametrize(
        ("error", "text"),
        [
            (ValueError(_SECRET), "ValueError"),
            (_Declaring(count=2), "2 settings failed, <absent> first"),
        ],
        ids=["foreign", "declared"],
    )
    def test_the_text_is_a_loom_error_s_message_or_a_foreign_type(
        self, error: Exception, text: str
    ) -> None:
        assert Failure(error).text == text


class TestStacktrace:
    def test_every_link_of_the_chain_is_written_with_frames_and_types_only(self) -> None:
        stack = Failure(_chained()).stacktrace()

        assert _SECRET not in stack
        assert stack.index("KeyError") < stack.index("ValueError") < stack.index("RuntimeError")
        assert "direct cause" in stack
        assert "During handling" in stack
        assert stack.count("Traceback (most recent call last):") == 3

    def test_a_declared_loom_error_link_shows_its_safe_message(self) -> None:
        stack = Failure(_raised(_Declaring(count=3, setting=Secret(_SECRET)))).stacktrace()

        assert stack.endswith(f"_Declaring: 3 settings failed, {MASK} first")

    def test_an_exception_group_writes_each_member_type_and_frames_and_no_message(self) -> None:
        group = _raised(
            BaseExceptionGroup(_SECRET, [_raised(ValueError(_SECRET)), KeyError(_SECRET)])
        )

        stack = Failure(group).stacktrace()

        assert _SECRET not in stack
        assert "  | ValueError" in stack
        assert "  | KeyError" in stack
        assert stack.count("Traceback (most recent call last):") == 2

    def test_the_chain_stops_at_a_link_whose_stack_is_written(
        self, stamped: pytest.LogCaptureFixture
    ) -> None:
        cause = _raised(KeyError(_SECRET))
        _log.error("billing.lookup_failed", exc_info=cause)
        try:
            raise ValueError(_SECRET) from cause
        except ValueError as error:
            stack = Failure(error).stacktrace()

        assert stack.startswith("KeyError\n")
        assert stack.count("Traceback (most recent call last):") == 1


class TestOnceStack:
    def test_the_stack_of_one_exception_is_written_once(
        self, stamped: pytest.LogCaptureFixture
    ) -> None:
        error = _raised(ValueError(_SECRET))

        _log.error("billing.invoice_failed", exc_info=error)
        _log.error("billing.batch_failed", exc_info=error)

        first, second = (r.__dict__ for r in stamped.records)
        assert ErrorLogField.EXCEPTION_STACKTRACE in first
        assert ErrorLogField.EXCEPTION_STACKTRACE not in second
        assert second[ErrorLogField.EXCEPTION_TYPE] == "ValueError"
        assert Failure(error).is_stack_written

    def test_a_record_below_error_writes_no_stack(self, stamped: pytest.LogCaptureFixture) -> None:
        error = _raised(ValueError(_SECRET))

        _log.warning("billing.invoice_retried", exc_info=error)

        [record] = stamped.records
        assert ErrorLogField.EXCEPTION_STACKTRACE not in record.__dict__
        assert Failure(error).should_report

    @pytest.mark.parametrize(
        ("error", "level"),
        [
            (NotFound("Invoice", id=1), logging.INFO),
            (RuleViolationError("amount", "negative"), logging.INFO),
            (JobAbandonment("gave up"), logging.INFO),
            (ValueError("x"), logging.ERROR),
        ],
        ids=["not-found", "rule-violation", "job-signal", "foreign"],
    )
    def test_the_level_of_a_failure_follows_what_the_exception_declares(
        self, error: BaseException, level: int
    ) -> None:
        assert Failure(error).level == level

    def test_a_failure_whose_stack_is_written_is_reported_at_warning_after(
        self, stamped: pytest.LogCaptureFixture
    ) -> None:
        error = _raised(ValueError("x"))
        _log.error("billing.invoice_failed", exc_info=error)

        assert Failure(error).level == logging.WARNING
        assert not Failure(error).should_report


class TestTranslate:
    def test_an_unreported_foreign_failure_is_logged_once_and_leaves_as_unexpected(
        self, stamped: pytest.LogCaptureFixture
    ) -> None:
        error = _raised(ValueError(_SECRET))

        leaving = Failure(error).translate()
        Failure(error).translate()

        [record] = stamped.records
        assert record.getMessage() == ErrorLogEvent.FAILURE_LEFT_LOOM
        assert isinstance(leaving, UnexpectedError)
        assert leaving.original_type == "ValueError"
        assert _SECRET not in str(leaving) + repr(leaving)

    def test_the_unexpected_error_takes_the_stack_written_mark(
        self, stamped: pytest.LogCaptureFixture
    ) -> None:
        leaving = Failure(_raised(ValueError(_SECRET))).translate()

        assert Failure(leaving).is_stack_written

    def test_an_expected_failure_is_not_logged_and_leaves_as_itself(
        self, stamped: pytest.LogCaptureFixture
    ) -> None:
        error = NotFound("Invoice", id=1)

        assert Failure(error).translate() is error
        assert stamped.records == []

    def test_the_chain_and_the_notes_are_cleaned(self) -> None:
        leaving = Failure(_chained()).as_loom_error()

        assert (leaving.__cause__, leaving.__context__) == (None, None)
        assert leaving.__suppress_context__
        assert not hasattr(leaving, "__notes__")

    def test_a_loom_error_raised_from_a_foreign_one_leaves_without_it(self) -> None:
        error = _Declaring(count=1)
        try:
            raise error from ValueError(_SECRET)
        except _Declaring:
            error.add_note(_SECRET)
            leaving = Failure(error).as_loom_error()

        assert leaving is error
        assert _SECRET.encode() not in pickle.dumps(leaving)
        assert (leaving.__cause__, getattr(leaving, "__notes__", None)) == (None, None)

    @pytest.mark.parametrize(
        ("members", "leaves_as"),
        [
            ([NotFound("Invoice", id=1)], NotFound),
            ([NotFound("Invoice", id=1), ValueError(_SECRET)], UnexpectedError),
            ([ValueError(_SECRET)], UnexpectedError),
        ],
        ids=["one-loom-member", "mixed", "one-foreign-member"],
    )
    def test_a_group_leaves_as_its_single_loom_member_or_unexpected(
        self, members: list[Exception], leaves_as: type[Exception]
    ) -> None:
        leaving = Failure(ExceptionGroup("batch", members)).as_loom_error()

        assert type(leaving) is leaves_as

    def test_the_unexpected_error_rebuilds_from_its_code_and_pickles_without_the_original(
        self,
    ) -> None:
        leaving = Failure(ValueError(_SECRET)).as_loom_error()

        rebuilt = UnexpectedError(*leaving.args)
        restored = pickle.loads(pickle.dumps(leaving))

        assert rebuilt.code == leaving.code
        assert str(restored) == "unexpected ValueError"
        assert _SECRET.encode() not in pickle.dumps(leaving)
