"""What building a kernel logs: one record for every module registered, not one per module."""

from __future__ import annotations

import logging
from typing import Any

import pytest

from loom.core.bootstrap import create_kernel
from loom.core.bootstrap.log_events import BootstrapLogEvent, BootstrapLogField
from loom.core.di.container import LoomContainer
from loom.core.logger import LogField
from loom.core.use_case.use_case import UseCase
from tests.helpers.loggers import CapturedLog


class _NoDepsUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


def _first(container: LoomContainer) -> None:
    del container


def _second(container: LoomContainer) -> None:
    del container


class TestKernelLogging:
    def test_the_modules_are_one_record_after_the_loop(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG, logger="loom.core.bootstrap.kernel")

        create_kernel(config=object(), use_cases=[_NoDepsUseCase], modules=[_first, _second])

        [fields] = CapturedLog(caplog).named(BootstrapLogEvent.MODULES_REGISTERED)
        assert (fields[BootstrapLogField.MODULES], fields[LogField.COUNT]) == (
            ["_first", "_second"],
            2,
        )
