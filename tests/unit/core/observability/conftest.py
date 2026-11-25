"""Fixtures of the observability tests."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass

import pytest
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter

from loom.core.config.observability import OtelConfig
from loom.core.environment import Environment
from loom.core.logger import LogIdentity
from loom.core.logger.install import EXPORT_HANDLER_NAME
from loom.core.observability.observer import otel as otel_module
from loom.core.observability.observer.otel import OtelLogHandler, build_log_export


@dataclass(frozen=True)
class LogExport:
    """The OTEL log export of the test: its handler and what it exported."""

    handler: OtelLogHandler
    exporter: InMemoryLogRecordExporter

    def exported(self) -> dict[object, dict[str, object]]:
        """The attributes of each record exported so far by its body, after flushing."""
        self.handler.flush()
        return {
            finished.log_record.body: dict(finished.log_record.attributes or {})
            for finished in self.exporter.get_finished_logs()
        }


@pytest.fixture
def log_export() -> Iterator[LogExport]:
    """An OTEL log export into memory, shut down when the test ends."""
    exporter = InMemoryLogRecordExporter()
    handler = build_log_export(
        OtelConfig(endpoint=None),
        LogIdentity(environment=Environment.TEST),
        service_name="loom-test",
        exporter=exporter,
    )
    yield LogExport(handler, exporter)
    handler.close()


@pytest.fixture
def configured_log_export(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[], LogExport]]:
    """The OTEL log export ``Observability.from_config`` installs, into memory.

    Calling it gives the handler now on the root logger with what its exporter got.
    """
    built: list[InMemoryLogRecordExporter] = []

    def build(_config: OtelConfig) -> InMemoryLogRecordExporter:
        built.append(InMemoryLogRecordExporter())
        return built[-1]

    monkeypatch.setattr(otel_module, "_build_log_exporter", build)

    def installed() -> LogExport:
        (handler,) = [
            h for h in logging.getLogger().handlers if h.get_name() == EXPORT_HANDLER_NAME
        ]
        assert isinstance(handler, OtelLogHandler)
        return LogExport(handler, built[-1])

    yield installed
    installed().handler.close()
