"""Composition logs one structured event, with each phase's milliseconds, through one logger."""

from __future__ import annotations

import logging
from collections.abc import Callable

import pytest

from loom.composition import Application
from loom.core.di.container import LoomContainer
from loom.core.logger.install import UnfilteredHandlersWarning
from loom.core.logger.log_events import LoggerLogEvent
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
        assert fields.pop("phase_ms") == "dict", "not a scalar list: its type until L2c"
        assert fields.pop("duration_ms") > 0
        assert fields.pop("keys") > 10
        assert fields == {
            "runtime": "Serving",
            "use_cases": 1,
            "jobs": 1,
            "store_plugins": "dict",
            "installers": 0,
            "overrides": 1,
            "agents": 0,
            "mcp_servers": 0,
        }

    def test_a_root_handler_added_during_composition_is_reported_once_it_ends(
        self,
        overridden: Callable[..., Application],
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            "loom.composition.project.warn_unfiltered_handlers", UnfilteredHandlersWarning()
        )
        late = logging.NullHandler()

        def adds_a_handler(container: LoomContainer) -> None:
            del container
            logging.getLogger().addHandler(late)

        try:
            overridden(modules=(adds_a_handler,))
        finally:
            logging.getLogger().removeHandler(late)

        assert LoggerLogEvent.HANDLERS_UNFILTERED in caplog.messages
