"""Composition logs one structured event per phase through the ``LoggerPort`` it gets once."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.composition import Application, pipeline
from loom.core.di.container import LoomContainer
from tests.helpers.loggers import RecordingLogger

_PHASES = (
    "loom.composition.planned",
    "loom.composition.persisted",
    "loom.composition.registered",
    "loom.composition.ai_wired",
    "loom.composition.sealed",
    "loom.composition.composed",
)


def _acme_module(container: LoomContainer) -> None:
    del container


class TestPhaseEvents:
    def test_each_phase_logs_one_event_with_its_duration(
        self, overridden: Callable[..., Application], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        logger = RecordingLogger()
        monkeypatch.setattr(pipeline, "get_logger", lambda name: logger)

        overridden(modules=(_acme_module,))

        phases = [(event, fields) for event, fields in logger.calls if event in _PHASES]
        assert [event for event, _ in phases] == list(_PHASES)
        recorded = dict(phases)
        durations = [fields.pop("duration_ms") for fields in recorded.values()]
        assert all(isinstance(duration, float) for duration in durations)
        assert recorded["loom.composition.registered"].pop("keys") > 10
        assert recorded == {
            "loom.composition.planned": {"adapter": "Serving", "use_cases": 1, "jobs": 1},
            "loom.composition.persisted": {"backends": {"default": "sqlalchemy"}},
            "loom.composition.registered": {"modules": 1, "overrides": 0},
            "loom.composition.ai_wired": {"agents": 0, "mcp_servers": 0},
            "loom.composition.sealed": {},
            "loom.composition.composed": {},
        }
