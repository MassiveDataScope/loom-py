"""One fact, one record, one owner: the lifecycle observer owns every scope's failure."""

from __future__ import annotations

import logging
from typing import Any

import pytest
from opentelemetry.trace import StatusCode

from loom.core.bootstrap.kernel import create_kernel
from loom.core.errors import NotFound
from loom.core.logger import LogField, LogIdentity
from loom.core.logger.install import install_record_factory
from loom.core.observability.event import EventKind, Scope
from loom.core.observability.log_events import ObservabilityLogEvent
from loom.core.observability.observability import Observability
from loom.core.observability.observer.log import LogLifecycleObserver
from loom.core.use_case.use_case import UseCase
from tests.helpers.spans import EventCollector, build_recorder

_SECRET = "patient 0042 diagnosis"


class _Ping(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


@pytest.fixture
def lifecycle(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Lifecycle records captured as a loom handler sees them, behind loom's record factory."""
    install_record_factory(LogIdentity(environment="test"))
    caplog.set_level(logging.DEBUG, logger="loom.core.observability.observer.log")
    return caplog


def _ended(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.getMessage() == ObservabilityLogEvent.LIFECYCLE_ENDED]


def _fail_in(runtime: Observability, error: Exception) -> None:
    with (
        pytest.raises(type(error)),
        runtime.span(Scope.JOB, "SendInvoice"),
        runtime.span(Scope.USE_CASE, "IssueInvoice"),
    ):
        raise error


class TestNestedScopes:
    def test_each_scope_writes_its_end_and_the_stack_is_written_once(
        self, lifecycle: pytest.LogCaptureFixture
    ) -> None:
        _fail_in(Observability([LogLifecycleObserver()]), ValueError(_SECRET))

        inner, outer = _ended(lifecycle)
        assert (inner.levelname, outer.levelname) == ("ERROR", "WARNING")
        assert LogField.EXCEPTION_STACKTRACE in inner.__dict__
        assert LogField.EXCEPTION_STACKTRACE not in outer.__dict__
        assert outer.__dict__[LogField.EXCEPTION_TYPE] == "ValueError"
        assert outer.__dict__[LogField.STATUS] == "failure"
        assert _SECRET not in lifecycle.text

    def test_an_expected_failure_ends_its_scopes_at_info_with_no_stack(
        self, lifecycle: pytest.LogCaptureFixture
    ) -> None:
        _fail_in(Observability([LogLifecycleObserver()]), NotFound("Invoice", id=1))

        assert [r.levelname for r in _ended(lifecycle)] == ["INFO", "INFO"]
        assert all(LogField.EXCEPTION_STACKTRACE not in r.__dict__ for r in _ended(lifecycle))
        assert _ended(lifecycle)[0].__dict__[LogField.ERROR_CODE] == "not_found"


class TestSpanOutputs:
    def test_a_failed_span_carries_the_type_and_no_message_nor_stack(self) -> None:
        recorder = build_recorder()

        with pytest.raises(RuntimeError), recorder.runtime.span(Scope.JOB, "SendInvoice"):
            try:
                raise KeyError(_SECRET)
            except KeyError as cause:
                raise RuntimeError(_SECRET) from cause

        span = recorder.one("job:SendInvoice")
        (event,) = span.events
        attributes = dict(event.attributes or {})
        assert (span.status.status_code, span.status.description) == (
            StatusCode.ERROR,
            "RuntimeError",
        )
        assert (event.name, attributes[LogField.EXCEPTION_TYPE]) == ("exception", "RuntimeError")
        assert LogField.EXCEPTION_STACKTRACE not in attributes
        assert _SECRET not in repr(span.to_json())

    def test_an_expected_failure_leaves_the_span_status_unset(self) -> None:
        recorder = build_recorder()

        with pytest.raises(NotFound), recorder.runtime.span(Scope.JOB, "SendInvoice"):
            raise NotFound("Invoice", id=_SECRET)

        span = recorder.one("job:SendInvoice")
        assert span.status.status_code is StatusCode.UNSET
        assert dict(span.events[0].attributes or {})[LogField.ERROR_CODE] == "not_found"


class TestEngineScope:
    async def test_an_execution_outside_rest_runs_in_a_use_case_scope(self) -> None:
        collector = EventCollector()
        runtime = Observability([collector])
        kernel = create_kernel(
            config=object(),
            use_cases=[_Ping],
            modules=[lambda container: container.register_instance(Observability, runtime)],
        )

        assert await kernel.executor.run(_Ping, factory=kernel.factory) == "pong"

        assert [(e.scope, e.kind, e.name) for e in collector.events] == [
            (Scope.USE_CASE, EventKind.START, "_Ping"),
            (Scope.USE_CASE, EventKind.END, "_Ping"),
        ]
