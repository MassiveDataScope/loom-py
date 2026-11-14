"""Structlog observer tests for streaming observability."""

from __future__ import annotations

import pytest
import structlog

from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.observer.structlog import StructlogLifecycleObserver


class TestStructlogFlowObserver:
    @pytest.mark.parametrize(
        ("event", "expected_scope", "expected_kind"),
        [
            (
                LifecycleEvent(
                    scope=Scope.POLL_CYCLE,
                    name="orders",
                    kind=EventKind.START,
                    meta={"node_count": 3},
                ),
                Scope.POLL_CYCLE,
                EventKind.START,
            ),
            (
                LifecycleEvent(
                    scope=Scope.POLL_CYCLE,
                    name="orders",
                    kind=EventKind.END,
                    duration_ms=150,
                    status=LifecycleStatus.SUCCESS,
                ),
                Scope.POLL_CYCLE,
                EventKind.END,
            ),
            (
                LifecycleEvent(
                    scope=Scope.NODE,
                    name="orders:0",
                    kind=EventKind.ERROR,
                    error="ValueError('bad input')",
                    meta={"node_type": "FakeStep"},
                ),
                Scope.NODE,
                EventKind.ERROR,
            ),
        ],
    )
    def test_structlog_flow_observer_emits_core_events(
        self,
        event: LifecycleEvent,
        expected_scope: Scope,
        expected_kind: EventKind,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        calls: list[str] = []

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **_: object) -> None:
                calls.append(event)

            def info(self, event: str, **_: object) -> None:
                calls.append(event)

            def warning(self, event: str, **_: object) -> None:
                calls.append(event)

            def error(self, event: str, **_: object) -> None:
                calls.append(event)

        monkeypatch.setattr(structlog, "get_logger", lambda *_: _FakeLogger())
        observer = StructlogLifecycleObserver()

        observer.on_event(event)

        assert calls == [expected_kind.value]
        assert event.scope is expected_scope
