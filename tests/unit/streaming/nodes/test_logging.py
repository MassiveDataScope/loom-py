"""Tests for declarative logger bindings on streaming nodes."""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any

import pytest

from loom.core.logger import LoggerPort
from loom.streaming.nodes._step import RecordStep
from loom.streaming.nodes._with import ContextSupplier
from tests.unit.streaming.doubles import AcmePayload


class _FakeLogger:
    def __init__(self) -> None:
        self.bind_calls: list[dict[str, Any]] = []
        self.events: list[tuple[str, str, dict[str, Any]]] = []

    def bind(self, **fields: Any) -> LoggerPort:
        self.bind_calls.append(dict(fields))
        return self

    def debug(self, event: str, **fields: Any) -> None:
        self.events.append(("debug", event, dict(fields)))

    def info(self, event: str, **fields: Any) -> None:
        self.events.append(("info", event, dict(fields)))

    def warning(self, event: str, **fields: Any) -> None:
        self.events.append(("warning", event, dict(fields)))

    def error(self, event: str, **fields: Any) -> None:
        self.events.append(("error", event, dict(fields)))

    def exception(self, event: str, **fields: Any) -> None:
        self.events.append(("exception", event, dict(fields)))


class _LoggedStep(RecordStep[AcmePayload, AcmePayload]):
    pass


class TestNodeLogBindings:
    def test_step_log_binds_static_class_metadata(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake = _FakeLogger()
        step = _LoggedStep()
        monkeypatch.setattr("loom.streaming.nodes._step.get_logger", lambda name: fake)

        logger = step.log

        assert logger is fake
        assert fake.bind_calls == [
            {
                "component": "step",
                "class_name": "_LoggedStep",
                "module": __name__,
                "step_name": "_LoggedStep",
            }
        ]
        assert step.log is logger

    def test_context_supplier_log_binds_static_class_metadata(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeLogger()
        factory = ContextSupplier(lambda: nullcontext())
        monkeypatch.setattr("loom.streaming.nodes._with.get_logger", lambda name: fake)

        logger = factory.log

        assert logger is fake
        assert fake.bind_calls == [
            {
                "component": "context_supplier",
                "class_name": "ContextSupplier",
                "module": "loom.streaming.nodes._with",
                "factory_name": "ContextSupplier",
            }
        ]
        assert factory.log is logger
