"""Protocol tests for core lifecycle observers used by streaming."""

from __future__ import annotations

from loom.core.observability.event import EventKind, LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.observability import Observability
from loom.core.observability.observer.noop import NoopObserver
from loom.core.observability.observer.structlog import StructlogLifecycleObserver
from loom.core.observability.protocol import LifecycleObserver
from tests.unit.streaming.observability.cases import FailingFlowObserver, RecordingFlowObserver


class TestFlowObserverProtocol:
    def test_noop_flow_observer_matches_protocol(self) -> None:
        observer: LifecycleObserver = NoopObserver()

        for kind in EventKind:
            observer.on_event(LifecycleEvent(scope=Scope.NODE, name="test", kind=kind))

    def test_structlog_flow_observer_matches_protocol(self) -> None:
        observer: LifecycleObserver = StructlogLifecycleObserver()
        assert type(observer) is StructlogLifecycleObserver
        observer.on_event(LifecycleEvent(scope=Scope.NODE, name="test", kind=EventKind.START))


class TestObservability:
    def test_runtime_fans_out_events(self) -> None:
        first = RecordingFlowObserver()
        second = RecordingFlowObserver()
        runtime = Observability([first, second])

        runtime.emit(LifecycleEvent(scope=Scope.POLL_CYCLE, name="my_flow", kind=EventKind.START))
        runtime.emit(
            LifecycleEvent(
                scope=Scope.NODE,
                name="my_flow:0",
                kind=EventKind.START,
                meta={"flow": "my_flow", "node_idx": 0, "node_type": "Step"},
            )
        )
        runtime.emit(
            LifecycleEvent(
                scope=Scope.NODE,
                name="my_flow:0",
                kind=EventKind.END,
                duration_ms=5,
                status=LifecycleStatus.SUCCESS,
                meta={"flow": "my_flow", "node_idx": 0, "node_type": "Step"},
            )
        )
        runtime.emit(LifecycleEvent(scope=Scope.POLL_CYCLE, name="my_flow", kind=EventKind.END))

        assert first.events == second.events
        assert len(first.events) == 4
        assert first.events[0].kind is EventKind.START
        assert first.events[1].scope is Scope.NODE

    def test_runtime_isolates_errors(self) -> None:
        good = RecordingFlowObserver()
        runtime = Observability([FailingFlowObserver(), good])

        runtime.emit(LifecycleEvent(scope=Scope.POLL_CYCLE, name="test", kind=EventKind.START))
        runtime.emit(LifecycleEvent(scope=Scope.NODE, name="test:0", kind=EventKind.START, meta={}))
        runtime.emit(
            LifecycleEvent(
                scope=Scope.NODE,
                name="test:0",
                kind=EventKind.ERROR,
                error="ValueError('bad')",
                meta={},
            )
        )
        runtime.emit(LifecycleEvent(scope=Scope.POLL_CYCLE, name="test", kind=EventKind.END))

        assert len(good.events) == 4

    def test_runtime_empty_is_noop(self) -> None:
        runtime = Observability([])
        runtime.emit(LifecycleEvent(scope=Scope.POLL_CYCLE, name="test", kind=EventKind.START))
        runtime.emit(LifecycleEvent(scope=Scope.POLL_CYCLE, name="test", kind=EventKind.END))
