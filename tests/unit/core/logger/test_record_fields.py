from __future__ import annotations

import logging

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.id_generator import IdGenerator
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from loom.core.environment import Environment
from loom.core.errors import NotFound
from loom.core.log_names import LogField
from loom.core.logger import LogIdentity, LogRuntime, log_context
from loom.core.logger.formats import MAX_STACK_LENGTH, TRUNCATION_MARKER, RecordFields
from loom.core.tracing.context import active_trace_id
from tests.helpers.loggers import CapturingHandler

_IDENTITY = LogIdentity(
    environment=Environment.PRODUCTION,
    runtime=LogRuntime.FASTAPI,
    service_name="billing",
    service_version="2.4.0",
)
_LOGGER = logging.getLogger("tests.record_fields")


class _ShortIds(IdGenerator):
    def generate_span_id(self) -> int:
        return 0xDEF

    def generate_trace_id(self) -> int:
        return 0xABC


class _Greeter:
    def greet(self) -> None:
        _LOGGER.info("greeter.greeting_sent")


@pytest.fixture
def identified(handler: CapturingHandler) -> CapturingHandler:
    handler.identity = _IDENTITY
    return handler


def _log_failure(level: int, failure: Exception, **extra: object) -> None:
    try:
        raise failure
    except Exception:
        _LOGGER.log(level, "billing.invoice_failed", exc_info=True, extra=extra)


class TestDerivedFields:
    def test_inside_a_span_of_a_caller_trace_the_ids_are_the_spans(
        self, identified: CapturingHandler
    ) -> None:
        caller_trace = "00000000000000000000000000000abc"
        incoming = {"traceparent": f"00-{caller_trace}-00f067aa0ba902b7-01"}
        parent = TraceContextTextMapPropagator().extract(incoming)
        tracer = TracerProvider(id_generator=_ShortIds()).get_tracer("tests.record_fields")

        with (
            active_trace_id("request-7"),
            tracer.start_as_current_span("request", context=parent),
        ):
            _LOGGER.info("billing.invoice_issued")
            _LOGGER.info("billing.invoice_issued", extra={LogField.TRACE_ID.value: "from-extra"})

        assert [fields[LogField.TRACE_ID] for fields in identified.fields] == [caller_trace] * 2
        assert identified.fields[0][LogField.SPAN_ID] == "0000000000000def"

    def test_outside_a_span_extra_wins_then_the_trace_then_the_context_then_the_identity(
        self, identified: CapturingHandler
    ) -> None:
        context = {LogField.TRACE_ID: "from-context", LogField.SERVICE_NAME: "from-context"}

        with active_trace_id("from-trace"), log_context(context):
            _LOGGER.info("a.b_c", extra={LogField.ATTEMPT.value: 2})
            _LOGGER.info("a.b_c", extra={LogField.TRACE_ID.value: "from-extra"})

        first, second = identified.fields
        assert (first[LogField.TRACE_ID], first[LogField.SERVICE_NAME]) == (
            "from-trace",
            "from-context",
        )
        assert second[LogField.TRACE_ID] == "from-extra"

    def test_a_record_from_a_method_says_where_it_was_emitted(
        self, identified: CapturingHandler
    ) -> None:
        _Greeter().greet()

        fields = identified.fields[0]
        assert fields[LogField.CODE_FUNCTION] == "greet"
        assert fields[LogField.CODE_LINE] == _Greeter.greet.__code__.co_firstlineno + 1
        assert fields[LogField.CODE_FILE] == __file__

    def test_the_record_is_left_as_it_was_logged(self, identified: CapturingHandler) -> None:
        record = _LOGGER.makeRecord("x", logging.INFO, __file__, 1, "a.b_c", (), None)

        with active_trace_id("trace-42"):
            CapturingHandler(_IDENTITY).handle(record)

        assert LogField.TRACE_ID not in record.__dict__
        assert LogField.SERVICE_NAME not in record.__dict__


@pytest.mark.parametrize(
    ("level", "standard"),
    [(0, 0), (5, 0), (10, 10), (25, 20), (30, 30), (45, 40), (60, 50)],
)
def test_a_level_is_the_standard_one_at_or_below_it(level: int, standard: int) -> None:
    record = _LOGGER.makeRecord("x", level, __file__, 1, "a.b_c", (), None)

    assert RecordFields(record).standard_level == standard


class TestFailureFields:
    def test_a_record_names_the_type_and_the_code_and_never_the_message(
        self, identified: CapturingHandler
    ) -> None:
        _log_failure(logging.WARNING, NotFound("Invoice", id=7))

        fields = identified.fields[0]
        assert (fields[LogField.EXCEPTION_TYPE], fields[LogField.ERROR_CODE]) == (
            "loom.core.errors.domain.NotFound",
            "not_found",
        )
        assert identified.stacks == [None]

    def test_from_error_up_the_stack_is_written_with_its_cause(
        self, identified: CapturingHandler
    ) -> None:
        try:
            try:
                raise KeyError("row")
            except KeyError as cause:
                raise ValueError("amount below zero") from cause
        except ValueError:
            _LOGGER.error("billing.invoice_failed", exc_info=True)

        (stack,) = identified.stacks
        assert stack is not None
        assert stack.startswith("Traceback (most recent call last):")
        assert "KeyError" in stack
        assert stack.endswith("ValueError: amount below zero")
        assert LogField.EXCEPTION_STACKTRACE not in identified.fields[0]

    def test_the_stack_info_asked_for_is_written_at_any_level_after_the_traceback(
        self, identified: CapturingHandler
    ) -> None:
        _LOGGER.info("billing.invoice_issued", stack_info=True)
        try:
            raise ValueError("boom")
        except ValueError:
            _LOGGER.error("billing.invoice_failed", exc_info=True, stack_info=True)

        issued, failed = identified.stacks
        assert issued is not None
        assert issued.startswith("Stack (most recent call last):")
        assert failed is not None
        assert failed.index("ValueError: boom") < failed.index("Stack (most recent call last):")

    def test_a_long_stack_keeps_its_end(self, identified: CapturingHandler) -> None:
        _log_failure(logging.ERROR, ValueError("x" * MAX_STACK_LENGTH))

        (stack,) = identified.stacks
        assert stack is not None
        assert len(stack) == MAX_STACK_LENGTH
        assert stack.startswith(TRUNCATION_MARKER + "...")
        assert stack.endswith("x" * 100)

    @pytest.mark.parametrize("level", [logging.WARNING, logging.ERROR], ids=["warning", "error"])
    def test_extra_cannot_forge_the_failure_fields(
        self, identified: CapturingHandler, level: int
    ) -> None:
        forged = {
            LogField.EXCEPTION_TYPE.value: "Forged",
            LogField.EXCEPTION_STACKTRACE.value: "forged",
        }

        _log_failure(level, ValueError("boom"), **forged)
        _LOGGER.info("billing.invoice_issued", extra=forged)

        failed, plain = identified.fields
        assert failed[LogField.EXCEPTION_TYPE] == "ValueError"
        assert plain[LogField.EXCEPTION_TYPE] == "Forged"
        assert LogField.EXCEPTION_STACKTRACE not in failed | plain
        assert identified.stacks[0] != "forged"
        assert identified.stacks[1] is None
