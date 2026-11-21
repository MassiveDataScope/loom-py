"""Fixtures of the observability tests."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import pytest
from opentelemetry.sdk._logs import LoggingHandler
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter

from loom.core.config.observability import OtelConfig
from loom.core.observability.observer.otel import OtelLogExport, install_otel_log_export


@dataclass(frozen=True)
class LogExport:
    """The OTEL log export of the test: its handler and what it exported."""

    handler: LoggingHandler
    exporter: InMemoryLogRecordExporter

    def exported(self) -> dict[object, dict[str, object]]:
        """The attributes of each record exported so far by its body, after flushing."""
        export = OtelLogExport.installed()
        assert export is not None
        export.flush()
        return {
            finished.log_record.body: dict(finished.log_record.attributes or {})
            for finished in self.exporter.get_finished_logs()
        }


@pytest.fixture
def log_export() -> Iterator[LogExport]:
    """A fresh OTEL log export into memory; ``install_otel_log_export`` returns it as given."""
    OtelLogExport.forget()
    exporter = InMemoryLogRecordExporter()
    handler = install_otel_log_export(
        OtelConfig(endpoint=None, service_name="loom-test"), exporter=exporter
    )
    yield LogExport(handler, exporter)
    OtelLogExport.forget()
