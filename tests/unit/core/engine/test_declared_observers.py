"""``observability.execution_observers``: each entry is a ``module:Class`` observing executions."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.core.config import ConfigContext, ConfigError
from loom.core.engine.declared_observers import (
    build_execution_observers,
    check_written_execution_observers,
    resolve_execution_observer_class,
)
from loom.core.engine.events import EventKind, ExecutionEvent
from loom.core.engine.execution_observer import ExecutionObserverEvent, FanOutExecutionObserver
from tests.helpers.execution_observers import (
    NEEDS_ARGUMENTS,
    NOT_AN_OBSERVER,
    RAISING,
    RECORDING,
    JobOutcomeRecorder,
    RaisingObserver,
)

_EVENT = ExecutionEvent(EventKind.JOB_STARTED, "Acme")


def _written(entries: Any) -> ConfigContext:
    return ConfigContext.from_dict({"observability": {"execution_observers": entries}})


class TestEntryValidation:
    @pytest.mark.parametrize(
        ("entry", "problem"),
        [
            ("no_colon", "cannot be imported"),
            ("tests.helpers.execution_observers:", "cannot be imported"),
            ("tests.no_such_module:Observer", "cannot be imported"),
            ("tests.helpers.execution_observers:Missing", "cannot be imported"),
            (NOT_AN_OBSERVER, "is not an execution observer"),
            ("tests.helpers.execution_observers:RECORDING", "is not an execution observer"),
        ],
        ids=["no colon", "no class", "no module", "no attribute", "no on_event", "not a class"],
    )
    def test_a_bad_entry_is_refused_naming_it(self, entry: str, problem: str) -> None:
        with pytest.raises(ConfigError, match=problem) as refused:
            resolve_execution_observer_class(entry)

        assert repr(entry) in str(refused.value)

    def test_a_class_needing_arguments_is_refused_at_build_naming_it(self) -> None:
        with pytest.raises(ConfigError, match="cannot be built with no arguments") as refused:
            build_execution_observers([NEEDS_ARGUMENTS])

        assert repr(NEEDS_ARGUMENTS) in str(refused.value)

    def test_the_observers_are_built_in_declaration_order(self) -> None:
        built = build_execution_observers([RAISING, RECORDING])

        assert [type(observer) for observer in built] == [RaisingObserver, JobOutcomeRecorder]


class TestWrittenCheck:
    def test_a_resolvable_list_passes(self) -> None:
        check_written_execution_observers(_written([RECORDING, RAISING]))

    def test_an_absent_list_passes(self) -> None:
        check_written_execution_observers(ConfigContext.from_dict({}))

    def test_a_list_holding_a_placeholder_is_left_to_startup(self) -> None:
        check_written_execution_observers(_written(["${env:OBSERVER}"]))

    def test_a_list_of_the_wrong_type_is_refused(self) -> None:
        with pytest.raises(ConfigError, match="execution_observers"):
            check_written_execution_observers(_written({"acme": 1}))

    def test_the_first_bad_entry_is_named(self) -> None:
        with pytest.raises(ConfigError, match="tests.no_such_module"):
            check_written_execution_observers(_written([RECORDING, "tests.no_such_module:X"]))


class TestFanOut:
    def test_a_failing_observer_is_logged_and_the_others_still_receive_the_event(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        recorder = JobOutcomeRecorder()
        JobOutcomeRecorder.events.clear()
        fan_out = FanOutExecutionObserver([RaisingObserver(), recorder])

        with caplog.at_level(logging.WARNING):
            fan_out.on_event(_EVENT)

        assert JobOutcomeRecorder.events == [_EVENT]
        assert [record.getMessage() for record in caplog.records] == [ExecutionObserverEvent.FAILED]

    def test_nothing_to_fan_out_is_returned_as_it_is(self) -> None:
        recorder = JobOutcomeRecorder()

        assert FanOutExecutionObserver.combine(None, []) is None
        assert FanOutExecutionObserver.combine(recorder, []) is recorder

    def test_the_own_observer_comes_before_the_declared_ones(self) -> None:
        JobOutcomeRecorder.events.clear()
        combined = FanOutExecutionObserver.combine(RaisingObserver(), [JobOutcomeRecorder()])

        assert isinstance(combined, FanOutExecutionObserver)
        combined.on_event(_EVENT)

        assert JobOutcomeRecorder.events == [_EVENT]
