"""Acceptance table: every log output against the logging requirements.

Each test is one cell: an output loom writes to, crossed with one requirement,
driven through the real pipeline (``configure_logging``, the real formatters, the
OpenTelemetry Logs API handler and exporters).

The Lambda row uses a stand-in for the runtime's handler, because ``awslambdaric``
is not a dependency: its class name and text layout copy the runtime's Text format.
The runtime's JSON format is not reproduced, so it has no row yet.
"""

from __future__ import annotations

import io
import json
import logging
import shlex
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pytest
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from loom.core.config.observability import OtelConfig
from loom.core.environment import LAMBDA_LOG_FORMAT_VARIABLE, Environment
from loom.core.errors.base import DomainError
from loom.core.errors.codes import ErrorCode
from loom.core.log_names import LogField
from loom.core.logger import log_context
from loom.core.logger.formats import JsonFormatter
from loom.core.logger.identity import LogIdentity
from loom.core.logger.install import HANDLER_NAME
from loom.core.logger.settings import LogFormatName, LogSettings
from loom.core.model.redaction import MASK, Dsn, RedactedMap, Secret
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from loom.core.observability.observer.log import LogLifecycleObserver
from loom.core.observability.observer.otel import OtelLogHandler, build_log_export
from loom.core.tracing.context import active_trace_id
from loom.testing import make
from tests.helpers.loggers import LambdaLoggerHandler, configured

CANARY = "CANARY-ROW-VALUE-4711"
"""Data a driver puts in an exception message: it must not become a field loom writes."""

STACK_HEAD = "Traceback (most recent call last)"

_DOMAIN_ERROR = "loom.core.errors.base.DomainError"

_LOGGER = logging.getLogger("acceptance")

pytestmark = pytest.mark.filterwarnings("error::DeprecationWarning")

_APP = {"service_name": "billing", "service_version": "2.4.0"}


@dataclass(frozen=True, slots=True)
class _Card:
    number: str


def _failure() -> DomainError:
    """A loom error whose message carries data, raised so that it has a traceback."""
    try:
        raise DomainError(f"row {CANARY} rejected", code=ErrorCode.NOT_FOUND)
    except DomainError as exc:
        return exc


def _logfmt_fields(line: str) -> dict[str, object]:
    """The ``key=value`` tokens of a text line after its time, level, logger and message."""
    return dict(token.split("=", 1) for token in shlex.split(line)[4:])


@dataclass(frozen=True, slots=True)
class Output:
    """One place loom's records end up.

    Attributes:
        read: Everything written so far.
        read_fields: The same without the traceback, which is the standard
            one and may quote the messages of a chain.
        parse: Reads the fields back from one line of ``read_fields``.
    """

    read: Callable[[], str]
    read_fields: Callable[[], str]
    parse: Callable[[str], dict[str, object]] = json.loads

    def fields(self) -> dict[str, object]:
        """The fields of the last record, parsed back from the output."""
        return self.parse(self.read_fields().splitlines()[-1])


def _json_fields(text: str) -> str:
    entries = [json.loads(line) for line in text.splitlines() if line.strip()]
    return "\n".join(
        json.dumps({k: v for k, v in entry.items() if k != LogField.EXCEPTION_STACKTRACE})
        for entry in entries
    )


def _text_fields(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.startswith("  | "))


def _loom_stream() -> io.StringIO:
    """Point loom's stdout handler at a buffer the test reads."""
    stream = io.StringIO()
    (handler,) = [h for h in logging.getLogger().handlers if h.get_name() == HANDLER_NAME]
    assert isinstance(handler, logging.StreamHandler)
    handler.setStream(stream)
    return stream


@pytest.fixture
def stdout_json() -> Output:
    configured(LogSettings(format=LogFormatName.JSON), **_APP)
    stream = _loom_stream()
    return Output(stream.getvalue, lambda: _json_fields(stream.getvalue()))


@pytest.fixture
def stdout_text() -> Output:
    configured(LogSettings(format=LogFormatName.TEXT), environment=Environment.DEVELOPMENT, **_APP)
    stream = _loom_stream()
    return Output(stream.getvalue, lambda: _text_fields(stream.getvalue()), _logfmt_fields)


@pytest.fixture
def runtime() -> Iterator[LambdaLoggerHandler]:
    handler = LambdaLoggerHandler()
    root = logging.getLogger()
    root.addHandler(handler)
    yield handler
    root.removeHandler(handler)


def _lambda_output(runtime: LambdaLoggerHandler) -> Output:
    configured(LogSettings(format=LogFormatName.LAMBDA), **_APP)

    def read() -> str:
        return "".join(runtime.lines)

    return Output(read, lambda: _json_fields(read()))


@pytest.fixture
def lambda_text(runtime: LambdaLoggerHandler, monkeypatch: pytest.MonkeyPatch) -> Output:
    monkeypatch.setenv(LAMBDA_LOG_FORMAT_VARIABLE, "Text")
    return _lambda_output(runtime)


@pytest.fixture
def lambda_json(runtime: LambdaLoggerHandler, monkeypatch: pytest.MonkeyPatch) -> Output:
    monkeypatch.setenv(LAMBDA_LOG_FORMAT_VARIABLE, "JSON")
    return _lambda_output(runtime)


@dataclass(frozen=True, slots=True)
class OtelExport:
    """The export of the test: its handler and what it exported."""

    handler: OtelLogHandler
    exporter: InMemoryLogRecordExporter


@pytest.fixture
def otel_handler() -> Iterator[OtelExport]:
    exporter = InMemoryLogRecordExporter()
    identity = LogIdentity(environment=Environment.PRODUCTION, **_APP)
    handler = build_log_export(
        OtelConfig(endpoint=None), identity, service_name=_APP["service_name"], exporter=exporter
    )
    yield OtelExport(handler, exporter)
    logging.getLogger().removeHandler(handler)
    handler.close()


@pytest.fixture
def otel_export(otel_handler: OtelExport) -> Output:
    configured(
        LogSettings(format=LogFormatName.JSON), extra_handlers=[otel_handler.handler], **_APP
    )
    _loom_stream()
    exporter = otel_handler.exporter

    def attributes(*, with_stack: bool) -> str:
        otel_handler.handler.flush()
        return "\n".join(
            json.dumps(
                {
                    **item.resource.attributes,
                    **{
                        k: v
                        for k, v in dict(item.log_record.attributes or {}).items()
                        if with_stack or k != LogField.EXCEPTION_STACKTRACE
                    },
                },
                default=str,
            )
            for item in exporter.get_finished_logs()
        )

    return Output(lambda: attributes(with_stack=True), lambda: attributes(with_stack=False))


OUTPUTS = ("stdout_json", "stdout_text", "lambda_text", "lambda_json", "otel_export")


@pytest.fixture
def output(request: pytest.FixtureRequest) -> Output:
    return request.getfixturevalue(request.param)


class TestFieldsSurvive:
    """Requirement 1: a record's fields reach every output."""

    @pytest.mark.parametrize("output", OUTPUTS, indirect=True)
    def test_a_field_reaches_the_output(self, output: Output) -> None:
        _LOGGER.info("order_accepted", extra={LogField.ATTEMPT: 3})

        assert str(output.fields()[LogField.ATTEMPT]) == "3"


class TestContextDerivedAtWrite:
    """Requirement 4: every output derives the context, trace and identity itself."""

    @pytest.mark.parametrize("output", OUTPUTS, indirect=True)
    def test_a_record_carries_the_running_context_the_trace_and_the_identity(
        self, output: Output
    ) -> None:
        with log_context({LogField.JOB: "PlaceOrder"}), active_trace_id("trace-7"):
            _LOGGER.info("order_accepted")

        expected = {
            LogField.JOB: "PlaceOrder",
            LogField.TRACE_ID: "trace-7",
            LogField.SERVICE_NAME: "billing",
            LogField.SERVICE_VERSION: "2.4.0",
            LogField.RUNTIME: "cli",
        }
        assert expected.items() <= output.fields().items()

    def test_reconfiguring_twice_does_not_duplicate_the_export(
        self, otel_export: Output, otel_handler: OtelExport
    ) -> None:
        configured(
            LogSettings(format=LogFormatName.JSON), extra_handlers=[otel_handler.handler], **_APP
        )

        _LOGGER.info("order_accepted")

        otel_handler.handler.flush()
        assert len(otel_handler.exporter.get_finished_logs()) == 1


class TestValuesRendered:
    """Requirement 1: a value reaches every output rendered, a secret from any carrier none."""

    @pytest.mark.parametrize("output", OUTPUTS, indirect=True)
    @pytest.mark.parametrize(
        ("secret", "shown"),
        [
            (Secret(CANARY), MASK),
            (Dsn(f"postgresql://app:{CANARY}@db/orders"), f"postgresql://app:{MASK}@db/orders"),
            (RedactedMap({"password": CANARY}), MASK),
        ],
        ids=["secret", "dsn", "config"],
    )
    def test_a_secret_never_reaches_the_output(
        self, output: Output, secret: object, shown: str
    ) -> None:
        with log_context({LogField.USE_CASE: secret}):
            _LOGGER.info("order_accepted", extra={LogField.ATTEMPT: secret})

        assert output.fields()[LogField.ATTEMPT] == shown
        assert CANARY not in output.read()

    @pytest.mark.parametrize("output", OUTPUTS, indirect=True)
    def test_an_object_is_its_type_name_never_its_repr(self, output: Output) -> None:
        _LOGGER.info("order_accepted", extra={LogField.ATTEMPT: make(_Card, number=CANARY)})

        assert output.fields()[LogField.ATTEMPT] == "_Card"
        assert CANARY not in output.read_fields()

    def test_a_value_cannot_forge_a_text_field(self, stdout_text: Output) -> None:
        forged = "v w=1 loom.forged=evil"

        _LOGGER.info("order_accepted", extra={LogField.ATTEMPT: forged})

        fields = stdout_text.fields()
        assert fields[LogField.ATTEMPT] == forged
        assert {"w", "loom.forged"}.isdisjoint(fields)


class TestSpanCorrelation:
    """A record written inside a recording OpenTelemetry span names the span."""

    def test_the_export_carries_the_active_span_as_its_context_not_as_attributes(
        self, otel_export: Output, otel_handler: OtelExport
    ) -> None:
        with TracerProvider().get_tracer("acceptance").start_as_current_span("work") as span:
            _LOGGER.info("order_accepted")

        otel_handler.handler.flush()
        (item,) = otel_handler.exporter.get_finished_logs()
        assert item.log_record.span_id == span.get_span_context().span_id
        attributes = item.log_record.attributes or {}
        assert {LogField.TRACE_ID, LogField.SPAN_ID, LogField.SERVICE_NAME}.isdisjoint(attributes)
        assert item.resource.attributes["service.name"] == "billing"

    def test_a_stdout_line_carries_the_span_id_only_inside_a_span(
        self, stdout_json: Output
    ) -> None:
        _LOGGER.info("order_accepted")
        assert LogField.SPAN_ID not in stdout_json.fields()

        with TracerProvider().get_tracer("acceptance").start_as_current_span("work") as span:
            _LOGGER.info("order_accepted")

        assert stdout_json.fields()[LogField.SPAN_ID] == format(
            span.get_span_context().span_id, "016x"
        )


class TestDeveloperHandlers:
    """Any handler sees the raw record, before or after loom's."""

    def test_a_handler_before_or_after_loom_sees_the_value_as_passed(self) -> None:
        card = make(_Card, number=CANARY)
        attempt_repr = logging.Formatter(f"%({LogField.ATTEMPT.value})r", validate=False)
        before, after = logging.StreamHandler(io.StringIO()), logging.StreamHandler(io.StringIO())
        before.setFormatter(attempt_repr)
        after.setFormatter(attempt_repr)
        logging.getLogger().addHandler(before)
        configured(LogSettings(format=LogFormatName.JSON))
        _loom_stream()
        logging.getLogger().addHandler(after)

        _LOGGER.info("order_accepted", extra={LogField.ATTEMPT: card})

        assert before.stream.getvalue() == after.stream.getvalue() == f"{card!r}\n"


class TestJsonFormatterAlone:
    """A formatter on a handler loom did not install still writes the failure."""

    def test_a_plain_handler_keeps_the_type_and_the_stack(self) -> None:
        stream = io.StringIO()
        plain = logging.StreamHandler(stream)
        plain.setFormatter(JsonFormatter())
        logging.getLogger().addHandler(plain)

        _LOGGER.error("order_failed", exc_info=_failure())

        entry = json.loads(stream.getvalue())
        assert entry[LogField.EXCEPTION_TYPE] == _DOMAIN_ERROR
        assert STACK_HEAD in entry[LogField.EXCEPTION_STACKTRACE]


class TestFailureIdentity:
    """Requirement 2: a failure's type and code are written."""

    @pytest.mark.parametrize("output", OUTPUTS, indirect=True)
    def test_the_type_of_a_failure_is_written(self, output: Output) -> None:
        _LOGGER.error("order_failed", exc_info=_failure())

        assert output.fields()[LogField.EXCEPTION_TYPE] == _DOMAIN_ERROR

    @pytest.mark.parametrize("output", OUTPUTS, indirect=True)
    def test_the_code_of_a_loom_error_is_written(self, output: Output) -> None:
        _LOGGER.error("order_failed", exc_info=_failure())

        assert output.fields()[LogField.ERROR_CODE] == ErrorCode.NOT_FOUND.value


class TestLambdaFrames:
    """Under the JSON log format the runtime's handler frames each record as JSON."""

    def test_every_record_states_the_json_frame_of_its_level(
        self, runtime: LambdaLoggerHandler, lambda_json: Output
    ) -> None:
        _LOGGER.info("order_accepted")
        _LOGGER.log(25, "order_noticed")
        _LOGGER.error("order_failed", exc_info=_failure())

        assert runtime.frames == [
            bytes.fromhex("a55a000e"),
            bytes.fromhex("a55a000e"),
            bytes.fromhex("a55a0016"),
        ]
        assert all(line.endswith("}\n") for line in runtime.lines)

    def test_under_text_the_runtime_keeps_its_text_frames(
        self, runtime: LambdaLoggerHandler, lambda_text: Output
    ) -> None:
        _LOGGER.info("order_accepted")

        assert runtime.frames == [None]


class TestStackOnce:
    """Requirement 2: a record writes its failure's stack once.

    Once per record: loom passes ``exc_info`` only where it stops the exception,
    so one failure is one record.
    """

    @pytest.mark.parametrize("output", OUTPUTS, indirect=True)
    def test_a_failure_writes_one_stack(self, output: Output) -> None:
        _LOGGER.error("order_failed", exc_info=_failure())

        assert output.read().count(STACK_HEAD) == 1


class TestNoMessageField:
    """Requirement 3: loom never writes an exception's message as a field of its own."""

    @pytest.mark.parametrize(
        "output",
        OUTPUTS,
        indirect=True,
    )
    def test_the_message_of_a_failure_is_not_a_field(self, output: Output) -> None:
        _LOGGER.error("order_failed", exc_info=_failure())

        assert CANARY not in output.read_fields()


@pytest.fixture
def traced() -> Iterator[tuple[Observability, InMemorySpanExporter]]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer("acceptance")
    yield Observability([LogLifecycleObserver()], tracer=tracer), exporter
    provider.shutdown()


def _fail_a_scope(observability: Observability) -> None:
    with pytest.raises(DomainError), observability.span(Scope.USE_CASE, "PlaceOrder"):
        raise _failure()


class TestScopeFailure:
    """Requirements 2 and 3 for the record and the span a failing scope writes."""

    def test_the_scope_record_does_not_carry_the_message(
        self, traced: tuple[Observability, InMemorySpanExporter], stdout_json: Output
    ) -> None:
        _fail_a_scope(traced[0])

        assert CANARY not in stdout_json.read_fields()

    def test_the_scope_record_carries_type_and_code(
        self, traced: tuple[Observability, InMemorySpanExporter], stdout_json: Output
    ) -> None:
        _fail_a_scope(traced[0])

        fields = stdout_json.fields()
        assert fields[LogField.ERROR_TYPE] == _DOMAIN_ERROR
        assert fields[LogField.ERROR_CODE] == ErrorCode.NOT_FOUND.value

    def test_the_span_names_the_type_and_the_code(
        self, traced: tuple[Observability, InMemorySpanExporter]
    ) -> None:
        _fail_a_scope(traced[0])

        (span,) = traced[1].get_finished_spans()
        assert span.status.description == f"{_DOMAIN_ERROR} ({ErrorCode.NOT_FOUND.value})"
        assert [dict(e.attributes or {}) for e in span.events] == [
            {LogField.EXCEPTION_TYPE: _DOMAIN_ERROR, LogField.ERROR_CODE: ErrorCode.NOT_FOUND.value}
        ]

    def test_the_span_does_not_carry_the_message(
        self, traced: tuple[Observability, InMemorySpanExporter]
    ) -> None:
        _fail_a_scope(traced[0])

        (span,) = traced[1].get_finished_spans()
        exported = [span.status.description, *(dict(e.attributes or {}) for e in span.events)]
        assert CANARY not in json.dumps(exported, default=str)
