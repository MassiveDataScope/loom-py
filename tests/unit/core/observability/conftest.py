"""Fixtures of the observability tests."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import pytest
from opentelemetry.sdk._logs import LoggingHandler
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter

from loom.core.config.observability import OtelConfig
from loom.core.observability.observer import otel as otel_module
from loom.core.observability.observer.otel import install_otel_log_export


@dataclass(frozen=True)
class LogExport:
    """The OTEL log export of the test: its handler and what it exported."""

    handler: LoggingHandler
    exporter: InMemoryLogRecordExporter

    def exported(self) -> dict[object, dict[str, object]]:
        """The attributes of each record exported so far by its body, after flushing."""
        state = otel_module._LOG_EXPORT_STATE  # pyright: ignore[reportPrivateUsage]
        assert state is not None
        state[0].force_flush()
        return {
            finished.log_record.body: dict(finished.log_record.attributes or {})
            for finished in self.exporter.get_finished_logs()
        }


@pytest.fixture
def log_export(monkeypatch: pytest.MonkeyPatch) -> Iterator[LogExport]:
    """A fresh OTEL log export into memory; ``install_otel_log_export`` returns it as given."""
    monkeypatch.setattr(otel_module, "_LOG_EXPORT_STATE", None)
    exporter = InMemoryLogRecordExporter()
    handler = install_otel_log_export(
        OtelConfig(endpoint=None, service_name="loom-test"), exporter=exporter
    )
    yield LogExport(handler, exporter)
