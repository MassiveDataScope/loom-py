"""Composition logs one structured event, with each phase's milliseconds, through one logger."""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest

from loom.composition import Application
from loom.composition.event import CompositionEvent, CompositionKey
from loom.core.di.container import LoomContainer
from loom.core.log_names import LogField
from loom.jobs.event import JobsKey
from tests.helpers.loggers import CapturedLog

_PHASES = ("scan", "plan", "build_stores", "register", "analysed", "wire_ai", "seal")


def _acme_module(container: LoomContainer) -> None:
    del container


class TestComposedEvent:
    def test_one_event_carries_the_counts_and_each_phase_duration(
        self, overridden: Callable[..., Application], caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.INFO, logger="loom.composition.project")

        overridden(modules=(_acme_module,))

        [fields] = CapturedLog(caplog).named(CompositionEvent.APPLICATION_COMPOSED)
        phases = [lap.split("=") for lap in fields.pop(CompositionKey.PHASES)]
        total = fields.pop(LogField.DURATION_MS)
        assert tuple(name for name, _ in phases) == _PHASES
        assert total >= sum(float(ms) for _, ms in phases) - 0.1 * len(phases)
        assert fields == {LogField.USE_CASE_COUNT: 1, JobsKey.COUNT: 1}
