"""The behavioural canary: a value put where values hide never reaches any output loom emits.

The canary sits in an exception's message, in its chained cause, in a note, in
an object's ``repr``, past the cut of a long string and in a ``Secret``. One
failure is logged, ends a scope and crosses the Lambda boundary; every output
is then searched for it: the JSON and text formats, an OpenTelemetry log
export, the Lambda runtime's own JSON and Text formatters, a queue handler's
listener, the exported span, and the error the boundary hands the runtime.
"""

from __future__ import annotations

import io
import json
import logging
import queue
import traceback
from collections.abc import Iterator
from logging.handlers import QueueHandler, QueueListener
from typing import Any

import pytest
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from loom.core.config.observability import OtelConfig
from loom.core.environment import Environment
from loom.core.logger import LogField, log_context
from loom.core.logger.filter import MAX_VALUE_LENGTH, STANDARD_ATTRIBUTES
from loom.core.logger.formats import JsonFormatter, TextFormatter
from loom.core.logger.install import configure_logging
from loom.core.logger.settings import LogSettings
from loom.core.model.redaction import Secret
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from loom.core.observability.observer import otel as otel_module
from loom.core.observability.observer.log import LogLifecycleObserver
from loom.core.observability.observer.otel import install_otel_log_export
from loom.serverless.aws.lambda_.boundary import LambdaBoundary
from loom.serverless.aws.lambda_.errors import LambdaInvocationError

CANARY = "canary-7f3a9c"


class _Leaky:
    """An object whose text is the canary, as a driver's or a model's often is."""

    def __repr__(self) -> str:
        return CANARY

    __str__ = __repr__


class _LambdaRuntimeJsonFormatter(logging.Formatter):
    """What the Lambda runtime's JSON format writes: every extra key, and the exception."""

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, Any] = {
            key: value for key, value in record.__dict__.items() if key not in STANDARD_ATTRIBUTES
        }
        entry["message"] = record.getMessage()
        if record.exc_info:
            entry["errorMessage"] = str(record.exc_info[1])
            entry["stackTrace"] = self.formatException(record.exc_info)
        return json.dumps(entry, default=repr)


_LAMBDA_RUNTIME_TEXT = "[%(levelname)s]\t%(asctime)s\t%(message)s"


class _Outputs:
    """Every output under test, each read back as one text."""

    def __init__(self) -> None:
        self.streams: dict[str, io.StringIO] = {}
        self.handlers: list[logging.Handler] = []

    def stream(self, name: str, formatter: logging.Formatter) -> logging.Handler:
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        handler.setFormatter(formatter)
        self.streams[name] = stream
        self.handlers.append(handler)
        return handler

    def text(self, name: str) -> str:
        return self.streams[name].getvalue()


@pytest.fixture
def outputs(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[_Outputs, Any]]:
    """Loom's logging configured with every kind of handler a process can have."""
    monkeypatch.setattr(otel_module, "_LOG_EXPORT_STATE", None)
    root = logging.getLogger()
    found = _Outputs()
    for name, formatter in (
        ("json", JsonFormatter()),
        ("text", TextFormatter()),
        ("lambda-json", _LambdaRuntimeJsonFormatter()),
        ("lambda-text", logging.Formatter(_LAMBDA_RUNTIME_TEXT)),
    ):
        root.addHandler(found.stream(name, formatter))
    records: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
    root.addHandler(QueueHandler(records))
    listener = QueueListener(records, found.stream("queue", logging.Formatter("%(message)s")))
    exporter = InMemoryLogRecordExporter()
    otel_logs = install_otel_log_export(
        OtelConfig(endpoint=None, service_name="t"), exporter=exporter
    )
    configure_logging(
        LogSettings(format="text"),
        environment=Environment.DEVELOPMENT,
        extra_handlers=(otel_logs,),
    )
    root.setLevel(logging.DEBUG)
    listener.start()
    yield found, exporter
    listener.stop()
    root.removeHandler(otel_logs)


def _failure() -> RuntimeError:
    """A failure with the canary in its message, its cause and its note."""
    try:
        try:
            raise KeyError(CANARY)
        except KeyError as cause:
            raise RuntimeError(CANARY) from cause
    except RuntimeError as error:
        error.add_note(CANARY)
        return error


def _otel_text(exporter: InMemoryLogRecordExporter) -> str:
    state = otel_module._LOG_EXPORT_STATE  # pyright: ignore[reportPrivateUsage]
    assert state is not None
    state[0].force_flush()
    return repr(
        [
            (finished.log_record.body, dict(finished.log_record.attributes or {}))
            for finished in exporter.get_finished_logs()
        ]
    )


def _emit_everywhere(spans: InMemorySpanExporter) -> LambdaInvocationError:
    """Log the failure with leaky fields, end a failed scope with it, cross the boundary."""
    log = logging.getLogger("acme.billing")
    with log_context({LogField.NAME: _Leaky()}):
        log.error(
            "acme.charge_failed",
            exc_info=_failure(),
            extra={
                LogField.REASON: _Leaky(),
                LogField.STATUS: "x" * MAX_VALUE_LENGTH + CANARY,
                LogField.SETTING: Secret(CANARY),
            },
        )
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    runtime = Observability([LogLifecycleObserver()], tracer=provider.get_tracer("acme"))
    with pytest.raises(RuntimeError), runtime.span(Scope.JOB, "Charge"):
        raise _failure()

    def handler(event: dict[str, Any], context: Any) -> None:
        raise _failure()

    with pytest.raises(LambdaInvocationError) as raised:
        LambdaBoundary(handler)({}, None)  # type: ignore[arg-type]
    return raised.value


class TestCanary:
    @pytest.fixture
    def emitted(
        self, outputs: tuple[_Outputs, Any]
    ) -> tuple[_Outputs, Any, InMemorySpanExporter, LambdaInvocationError]:
        found, exporter = outputs
        spans = InMemorySpanExporter()
        boundary_error = _emit_everywhere(spans)
        return found, exporter, spans, boundary_error

    @pytest.mark.parametrize("name", ["json", "text", "lambda-json", "lambda-text", "queue"])
    def test_no_log_output_carries_the_canary(
        self, emitted: tuple[_Outputs, Any, InMemorySpanExporter, LambdaInvocationError], name: str
    ) -> None:
        found = emitted[0]

        assert found.text(name), "the output received the records"
        assert CANARY not in found.text(name)

    def test_the_otel_log_export_carries_no_canary(
        self, emitted: tuple[_Outputs, Any, InMemorySpanExporter, LambdaInvocationError]
    ) -> None:
        exported = _otel_text(emitted[1])

        assert "acme.charge_failed" in exported
        assert CANARY not in exported

    def test_the_exported_span_carries_no_canary(
        self, emitted: tuple[_Outputs, Any, InMemorySpanExporter, LambdaInvocationError]
    ) -> None:
        (span,) = emitted[2].get_finished_spans()

        assert span.events, "the span carries its exception event"
        assert CANARY not in span.to_json()

    def test_the_error_the_boundary_hands_the_runtime_carries_no_canary(
        self, emitted: tuple[_Outputs, Any, InMemorySpanExporter, LambdaInvocationError]
    ) -> None:
        error = emitted[3]
        reported = "".join(traceback.format_exception(error)) + repr(error) + repr(error.args)

        assert CANARY not in reported
        assert "RuntimeError" in str(error)
