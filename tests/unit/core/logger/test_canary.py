"""The behavioural canary of logging: no record carries a value put where values hide.

The canary sits in an exception's message, in its chained cause, in a note, in
an exception group's member, in an object's ``repr``, past the cut of a long
string and in a ``Secret``. One failure is logged with ``exc_info``, as the
message, as a ``%r`` argument and in a mapping argument, and ends a scope;
every output is then searched for it: loom's JSON and text formats, an
OpenTelemetry log export, the Lambda runtime's own JSON and Text formatters, a
queue handler's listener, a handler a framework attached to a child logger,
and the exported span. What leaves the process through a handoff is
``test_handoff_canary.py``'s.
"""

from __future__ import annotations

import io
import json
import logging
import queue
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
from loom.core.logger.filter import STANDARD_ATTRIBUTES
from loom.core.logger.formats import JsonFormatter
from loom.core.logger.install import configure_logging
from loom.core.logger.settings import LogSettings
from loom.core.logger.values import MAX_VALUE_LENGTH
from loom.core.model.redaction import Secret
from loom.core.observability.event import Scope
from loom.core.observability.observability import Observability
from loom.core.observability.observer import otel as otel_module
from loom.core.observability.observer.log import LogLifecycleObserver
from loom.core.observability.observer.otel import install_otel_log_export
from tests.helpers.canary import CANARY, Leaky, canary_failure


class LambdaLoggerHandler(logging.StreamHandler[io.StringIO]):
    """The Lambda runtime's handler, as loom recognises it: by its class name."""


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
_FRAMEWORK_TEXT = "%(levelname)s %(name)s %(message)s"


class _Outputs:
    """Every output under test, each read back as one text."""

    def __init__(self) -> None:
        self.streams: dict[str, io.StringIO] = {}

    def handler(
        self, name: str, formatter: logging.Formatter, kind: type[logging.StreamHandler[Any]]
    ) -> logging.Handler:
        stream = io.StringIO()
        handler = kind(stream)
        handler.setFormatter(formatter)
        self.streams[name] = stream
        return handler

    def text(self, name: str) -> str:
        return self.streams[name].getvalue()


@pytest.fixture
def outputs(monkeypatch: pytest.MonkeyPatch) -> Iterator[tuple[_Outputs, Any]]:
    """Loom's logging configured beside every kind of handler a process can have."""
    monkeypatch.setattr(otel_module, "_LOG_EXPORT_STATE", None)
    monkeypatch.setenv("AWS_LAMBDA_LOG_FORMAT", "JSON")
    root = logging.getLogger()
    found = _Outputs()
    plain = logging.StreamHandler
    root.addHandler(
        found.handler("lambda-json", _LambdaRuntimeJsonFormatter(), LambdaLoggerHandler)
    )
    root.addHandler(
        found.handler("lambda-text", logging.Formatter(_LAMBDA_RUNTIME_TEXT), LambdaLoggerHandler)
    )
    framework = logging.getLogger("acme.framework.error")
    framework.addHandler(found.handler("framework", logging.Formatter(_FRAMEWORK_TEXT), plain))
    records: queue.SimpleQueue[logging.LogRecord] = queue.SimpleQueue()
    root.addHandler(QueueHandler(records))
    listener = QueueListener(
        records, found.handler("queue", logging.Formatter("%(message)s"), plain)
    )
    exporter = InMemoryLogRecordExporter()
    otel_logs = install_otel_log_export(
        OtelConfig(endpoint=None, service_name="t"), exporter=exporter
    )
    configure_logging(
        LogSettings(format="text"),
        environment=Environment.DEVELOPMENT,
        extra_handlers=(otel_logs, found.handler("json", JsonFormatter(), plain)),
    )
    loom_handler = next(h for h in root.handlers if h.get_name() == "loom")
    assert isinstance(loom_handler, logging.StreamHandler)
    found.streams["text"] = io.StringIO()
    loom_handler.setStream(found.streams["text"])
    root.setLevel(logging.DEBUG)
    listener.start()
    yield found, exporter
    listener.stop()
    framework.handlers.clear()
    root.removeHandler(otel_logs)


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


def _emit_everywhere(spans: InMemorySpanExporter) -> None:
    """Log the failure every way a caller can, with leaky fields, and end a failed scope."""
    log = logging.getLogger("acme.billing")
    with log_context({LogField.NAME: Leaky()}):
        log.error(
            "acme.charge_failed",
            exc_info=canary_failure(),
            extra={
                LogField.REASON: Leaky(),
                LogField.STATUS: "x" * MAX_VALUE_LENGTH + CANARY,
                LogField.SETTING: Secret(CANARY),
            },
        )
    log.error(canary_failure())
    log.warning("acme.charge_retried %r", canary_failure())
    log.warning("acme.charge_retried %(error)s", {"error": canary_failure()})
    logging.getLogger("acme.framework.error").error(
        "Exception in lifespan", exc_info=canary_failure()
    )
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    runtime = Observability([LogLifecycleObserver()], tracer=provider.get_tracer("acme"))
    with pytest.raises(RuntimeError), runtime.span(Scope.JOB, "Charge"):
        raise canary_failure()


class TestCanary:
    @pytest.fixture
    def emitted(self, outputs: tuple[_Outputs, Any]) -> tuple[_Outputs, Any, InMemorySpanExporter]:
        found, exporter = outputs
        spans = InMemorySpanExporter()
        _emit_everywhere(spans)
        return found, exporter, spans

    @pytest.mark.parametrize(
        "name", ["json", "text", "lambda-json", "lambda-text", "queue", "framework"]
    )
    def test_no_log_output_carries_the_canary(
        self, emitted: tuple[_Outputs, Any, InMemorySpanExporter], name: str
    ) -> None:
        found = emitted[0]

        assert found.text(name), "the output received the records"
        assert CANARY not in found.text(name)

    def test_the_otel_log_export_carries_no_canary(
        self, emitted: tuple[_Outputs, Any, InMemorySpanExporter]
    ) -> None:
        exported = _otel_text(emitted[1])

        assert "acme.charge_failed" in exported
        assert CANARY not in exported

    def test_the_exported_span_carries_no_canary(
        self, emitted: tuple[_Outputs, Any, InMemorySpanExporter]
    ) -> None:
        (span,) = emitted[2].get_finished_spans()

        assert span.events, "the span carries its exception event"
        assert CANARY not in span.to_json()
