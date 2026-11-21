"""``jobs.shutdown`` decides what stopping the application does with delayed inline jobs."""

from __future__ import annotations

from typing import Any

import pytest
from omegaconf import OmegaConf

from loom.core.config import ConfigContext, ConfigError
from loom.core.job.config import JobSettings, JobShutdown, ShutdownMode


def _shutdown(section: dict[str, Any] | None) -> JobShutdown:
    document = {} if section is None else {"jobs": {"shutdown": section}}
    return JobSettings.from_config(ConfigContext(OmegaConf.create(document)), []).shutdown


class TestJobShutdown:
    def test_an_absent_section_cancels(self) -> None:
        assert _shutdown(None) == JobShutdown(mode=ShutdownMode.CANCEL, timeout_s=None)

    @pytest.mark.parametrize(
        ("section", "expected"),
        [
            ({"mode": "cancel"}, JobShutdown()),
            ({"mode": "wait"}, JobShutdown(mode=ShutdownMode.WAIT)),
            (
                {"mode": "wait", "timeout_s": 600},
                JobShutdown(mode=ShutdownMode.WAIT, timeout_s=600),
            ),
        ],
    )
    def test_a_valid_section_is_read(self, section: dict[str, Any], expected: JobShutdown) -> None:
        assert _shutdown(section) == expected

    @pytest.mark.parametrize(
        "section",
        [
            {"mode": "linger"},
            {"mode": "cancel", "timeout_s": 5},
            {"timeout_s": 5},
            {"mode": "wait", "timeout_s": -1},
            {"mode": "wait", "timeout_s": 0},
            {"mode": "wait", "grace": 1},
        ],
        ids=[
            "unknown mode",
            "timeout with cancel",
            "timeout by default",
            "negative",
            "zero",
            "key",
        ],
    )
    def test_an_invalid_section_is_a_config_error(self, section: dict[str, Any]) -> None:
        with pytest.raises(ConfigError, match="jobs.shutdown"):
            _shutdown(section)

    def test_only_wait_is_waiting(self) -> None:
        assert JobShutdown(mode=ShutdownMode.WAIT).is_waiting
        assert not JobShutdown().is_waiting
