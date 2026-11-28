"""Composition logs one structured event, with each phase's milliseconds, through one logger."""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest

from loom.composition import Application
from loom.composition.event import CompositionEvent, CompositionKey
from loom.core.di.container import LoomContainer
from loom.core.log_names import LogField
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
        phase_ms = {
            key.removeprefix(f"{CompositionKey.PHASE_MS}."): fields.pop(key)
            for key in [key for key in fields if key.startswith(CompositionKey.PHASE_MS)]
        }
        total = fields.pop(LogField.DURATION_MS)
        assert tuple(phase_ms) == _PHASES
        assert all(isinstance(ms, float) for ms in phase_ms.values())
        assert total >= sum(phase_ms.values())
        assert fields == {LogField.USE_CASE_COUNT: 1}
