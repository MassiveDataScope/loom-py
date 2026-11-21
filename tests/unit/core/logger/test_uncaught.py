"""Uncaught exceptions are one loom record each, under the record factory's policy."""

from __future__ import annotations

import asyncio
import logging
import sys
import threading

import pytest

from loom.core.logger import LogField, LogIdentity
from loom.core.logger.install import install_record_factory
from loom.core.logger.log_events import LoggerLogEvent
from loom.core.logger.uncaught import (
    install_uncaught_hooks,
    log_asyncio_exception,
    log_uncaught_exception,
)

_SECRET = "card 4111-1111-1111-1111"


@pytest.fixture
def uncaught(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    install_record_factory(LogIdentity(environment="test"))
    caplog.set_level(logging.ERROR, logger="loom.core.logger.uncaught")
    return caplog


def _the_one_record(caplog: pytest.LogCaptureFixture) -> logging.LogRecord:
    [record] = [r for r in caplog.records if r.getMessage() == LoggerLogEvent.UNCAUGHT_EXCEPTION]
    assert record.levelno == logging.ERROR
    assert record.__dict__[LogField.EXCEPTION_TYPE] == "ValueError"
    assert LogField.EXCEPTION_STACKTRACE in record.__dict__
    assert _SECRET not in caplog.text
    return record


class TestUncaughtHooks:
    def test_installing_replaces_the_interpreter_and_thread_hooks(self) -> None:
        install_uncaught_hooks()

        assert sys.excepthook is log_uncaught_exception
        assert threading.excepthook is not threading.__excepthook__

    def test_the_interpreter_hook_logs_one_record_with_no_message(
        self, uncaught: pytest.LogCaptureFixture
    ) -> None:
        error = ValueError(_SECRET)

        log_uncaught_exception(ValueError, error, error.__traceback__)

        _the_one_record(uncaught)

    def test_a_thread_failure_is_one_record_naming_the_thread(
        self, uncaught: pytest.LogCaptureFixture
    ) -> None:
        install_uncaught_hooks()

        def fail() -> None:
            raise ValueError(_SECRET)

        worker = threading.Thread(target=fail, name="acme-worker")
        worker.start()
        worker.join()

        record = _the_one_record(uncaught)
        assert "acme-worker" in repr(record.__dict__)

    async def test_an_unretrieved_task_failure_is_one_record_without_the_context_text(
        self, uncaught: pytest.LogCaptureFixture
    ) -> None:
        loop = asyncio.get_running_loop()

        log_asyncio_exception(
            loop,
            {
                "message": f"Task exception was never retrieved {_SECRET}",
                "exception": ValueError(_SECRET),
            },
        )

        _the_one_record(uncaught)
