from __future__ import annotations

import logging
from collections.abc import Iterator

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

_IDENTITY = LogIdentity(
    environment=Environment.PRODUCTION,
    runtime=LogRuntime.FASTAPI,
    service_name="billing",
    service_version="2.4.0",
)
_LOGGER = logging.getLogger("tests.record_fields")


class _Rendering(logging.Handler):
    """Renders each record while it is emitted, as an output of loom does."""

    def __init__(self, identity: LogIdentity | None) -> None:
        super().__init__()
        self.identity = identity
        self.fields: list[dict[str, object]] = []
        self.stacks: list[str | None] = []

    def emit(self, record: logging.LogRecord) -> None:
        view = RecordFields(record, self.identity)
        self.fields.append(view.fields())
        self.stacks.append(view.stack)


class _ShortIds(IdGenerator):
    def generate_span_id(self) -> int:
        return 0xDEF

    def generate_trace_id(self) -> int:
        return 0xABC


class _Greeter:
    def greet(self) -> None:
        _LOGGER.info("greeter.greeting_sent")


@pytest.fixture
def rendering(pristine_root_logger: logging.Logger) -> Iterator[_Rendering]:
    handler = _Rendering(_IDENTITY)
    pristine_root_logger.addHandler(handler)
    pristine_root_logger.setLevel(logging.DEBUG)
    yield handler
    pristine_root_logger.removeHandler(handler)


def _log_failure(level: int, failure: Exception, **extra: object) -> None:
    try:
        raise failure
    except Exception:
        _LOGGER.log(level, "billing.invoice_failed", exc_info=True, extra=extra)


class TestDerivedFields:
    def test_inside_a_span_of_a_caller_trace_the_ids_are_the_spans(
        self, rendering: _Rendering
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

        assert rendering.fields[0][LogField.TRACE_ID] == caller_trace
        assert rendering.fields[0][LogField.SPAN_ID] == "0000000000000def"

    def test_extra_wins_then_the_trace_then_the_context_then_the_identity(
        self, rendering: _Rendering
    ) -> None:
        context = {LogField.TRACE_ID: "from-context", LogField.SERVICE_NAME: "from-context"}

        with active_trace_id("from-trace"), log_context(context):
            _LOGGER.info("a.b_c", extra={LogField.ATTEMPT.value: 2})
            _LOGGER.info("a.b_c", extra={LogField.TRACE_ID.value: "from-extra"})

        first, second = rendering.fields
        assert (first[LogField.TRACE_ID], first[LogField.SERVICE_NAME]) == (
            "from-trace",
            "from-context",
        )
        assert second[LogField.TRACE_ID] == "from-extra"

    def test_a_record_from_a_method_says_where_it_was_emitted(self, rendering: _Rendering) -> None:
        _Greeter().greet()

        fields = rendering.fields[0]
        assert fields[LogField.CODE_FUNCTION] == "greet"
        assert fields[LogField.CODE_LINE] == _Greeter.greet.__code__.co_firstlineno + 1
        assert fields[LogField.CODE_FILE] == __file__

    def test_the_record_is_left_as_it_was_logged(self, rendering: _Rendering) -> None:
        record = _LOGGER.makeRecord("x", logging.INFO, __file__, 1, "a.b_c", (), None)

        with active_trace_id("trace-42"):
            _Rendering(_IDENTITY).handle(record)

        assert LogField.TRACE_ID not in record.__dict__
        assert LogField.SERVICE_NAME not in record.__dict__


class TestFailureFields:
    def test_a_record_names_the_type_and_the_code_and_never_the_message(
        self, rendering: _Rendering
    ) -> None:
        _log_failure(logging.WARNING, NotFound("Invoice", id=7))

        fields = rendering.fields[0]
        assert (fields[LogField.EXCEPTION_TYPE], fields[LogField.ERROR_CODE]) == (
            "loom.core.errors.domain.NotFound",
            "not_found",
        )
        assert rendering.stacks == [None]

    def test_from_error_up_the_stack_is_written_with_its_cause(self, rendering: _Rendering) -> None:
        try:
            try:
                raise KeyError("row")
            except KeyError as cause:
                raise ValueError("amount below zero") from cause
        except ValueError:
            _LOGGER.error("billing.invoice_failed", exc_info=True)

        (stack,) = rendering.stacks
        assert stack is not None
        assert stack.startswith("Traceback (most recent call last):")
        assert "KeyError" in stack
        assert stack.endswith("ValueError: amount below zero")
        assert LogField.EXCEPTION_STACKTRACE not in rendering.fields[0]

    def test_a_long_stack_keeps_its_end(self, rendering: _Rendering) -> None:
        _log_failure(logging.ERROR, ValueError("x" * MAX_STACK_LENGTH))

        (stack,) = rendering.stacks
        assert stack is not None
        assert len(stack) == MAX_STACK_LENGTH
        assert stack.startswith(TRUNCATION_MARKER + "...")
        assert stack.endswith("x" * 100)

    @pytest.mark.parametrize("level", [logging.WARNING, logging.ERROR], ids=["warning", "error"])
    def test_extra_cannot_forge_the_failure_fields(self, rendering: _Rendering, level: int) -> None:
        forged = {
            LogField.EXCEPTION_TYPE.value: "Forged",
            LogField.EXCEPTION_STACKTRACE.value: "forged",
        }

        _log_failure(level, ValueError("boom"), **forged)
        _LOGGER.info("billing.invoice_issued", extra=forged)

        failed, plain = rendering.fields
        assert failed[LogField.EXCEPTION_TYPE] == "ValueError"
        assert plain[LogField.EXCEPTION_TYPE] == "Forged"
        assert LogField.EXCEPTION_STACKTRACE not in failed | plain
        assert rendering.stacks[0] != "forged"
        assert rendering.stacks[1] is None
