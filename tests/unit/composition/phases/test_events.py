"""Composition logs one structured event, with each phase's milliseconds, through one logger."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.composition import Application, project
from loom.core.di.container import LoomContainer
from tests.helpers.loggers import RecordingLogger

_PHASES = ("plan", "build_stores", "register", "analysed", "wire_ai", "seal")


def _acme_module(container: LoomContainer) -> None:
    del container


class TestComposedEvent:
    def test_one_event_carries_the_counts_and_each_phase_duration(
        self, overridden: Callable[..., Application], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        logger = RecordingLogger()
        monkeypatch.setattr(project, "get_logger", lambda name: logger)

        overridden(modules=(_acme_module,))

        [(event, fields)] = [
            call for call in logger.calls if call[0].startswith("loom.composition.")
        ]
        assert event == "loom.composition.composed"
        phase_ms = fields.pop("phase_ms")
        total = fields.pop("duration_ms")
        assert tuple(phase_ms) == _PHASES
        assert all(isinstance(ms, float) for ms in phase_ms.values())
        assert total >= sum(phase_ms.values())
        assert fields.pop("keys") > 10
        assert fields == {
            "adapter": "Serving",
            "use_cases": 1,
            "jobs": 1,
            "backends": {"default": "sqlalchemy"},
            "modules": 1,
            "overrides": 0,
            "agents": 0,
            "mcp_servers": 0,
        }
