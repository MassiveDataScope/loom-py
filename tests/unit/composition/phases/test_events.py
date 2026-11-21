"""Composition logs one structured event, with each phase's milliseconds, through one logger."""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest

from loom.composition import Application
from loom.core.di.container import LoomContainer
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

        [(event, fields)] = [
            call for call in CapturedLog(caplog).calls if call[0].startswith("loom.composition.")
        ]
        assert event == "loom.composition.composed"
        phase_ms = fields.pop("phase_ms")
        total = fields.pop("duration_ms")
        assert tuple(phase_ms) == _PHASES
        assert all(isinstance(ms, float) for ms in phase_ms.values())
        assert total >= sum(phase_ms.values())
        assert fields.pop("keys") > 10
        assert fields == {
            "runtime": "Serving",
            "use_cases": 1,
            "jobs": 1,
            "store_plugins": {"default": "sqlalchemy"},
            "installers": 0,
            "overrides": 1,
            "agents": 0,
            "mcp_servers": 0,
        }
