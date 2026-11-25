"""Tests for the Prefect log bridge: loom's records through Prefect's API handler."""

from __future__ import annotations

import logging
import sys
import threading
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from prefect.logging.handlers import APILogWorker

from loom.core.log_names import LogField
from loom.core.logger import log_context
from loom.core.model.redaction import Secret
from loom.core.observability.diagnostic import ObservabilityEvent
from loom.core.tracing.context import active_trace_id
from loom.prefect.observer._logging_bridge import (
    PrefectLogBridgeHandler,
    active_task_run_id,
    log_bridge,
)


class _Sent(list[dict[str, Any]]):
    def send(self, payload: dict[str, Any]) -> None:
        self.append(payload)


@pytest.fixture
def sent(monkeypatch: pytest.MonkeyPatch) -> _Sent:
    """The payloads Prefect's handler hands to its API log worker."""
    payloads = _Sent()
    monkeypatch.setattr(APILogWorker, "instance", lambda *_: payloads)
    return payloads


@pytest.fixture
def task_run_id() -> Iterator[uuid.UUID]:
    """A step's TaskRun, bound for the test."""
    bound = uuid.uuid4()
    token = active_task_run_id.set(bound)
    yield bound
    active_task_run_id.reset(token)


def _record(
    message: str = "hello", level: int = logging.INFO, **extra: object
) -> logging.LogRecord:
    return logging.makeLogRecord(
        {"msg": message, "name": "loom.test", "levelno": level, "levelname": "X", **extra}
    )


class TestPrefectLogBridgeHandler:
    def test_a_step_record_reaches_the_api_with_its_task_run_and_its_flow_run(
        self, sent: _Sent, task_run_id: uuid.UUID
    ) -> None:
        flow_id = uuid.uuid4()

        PrefectLogBridgeHandler(flow_id).handle(_record("step log", level=logging.WARNING))

        (payload,) = sent
        assert (payload["flow_run_id"], payload["task_run_id"]) == (str(flow_id), str(task_run_id))
        assert (payload["message"], payload["level"]) == ("step log", logging.WARNING)

    def test_a_record_between_steps_carries_the_flow_run_only(self, sent: _Sent) -> None:
        flow_id = uuid.uuid4()

        PrefectLogBridgeHandler(flow_id).handle(_record())

        (payload,) = sent
        assert payload["flow_run_id"] == str(flow_id)
        assert payload["task_run_id"] is None

    def test_the_message_has_the_fields_as_loom_renders_them_and_no_code_origin(
        self, sent: _Sent
    ) -> None:
        record = _record(
            ObservabilityEvent.LIFECYCLE_FAILED,
            **{LogField.NAME: "Load step", "pw": Secret("x"), LogField.CODE_LINE: 3},
        )

        PrefectLogBridgeHandler(uuid.uuid4()).handle(record)

        assert sent[0]["message"] == (
            f'{ObservabilityEvent.LIFECYCLE_FAILED} loom.name="Load step" pw=***'
        )

    def test_the_message_has_the_running_context_and_the_trace(self, sent: _Sent) -> None:
        handler = PrefectLogBridgeHandler(uuid.uuid4())

        with log_context({LogField.JOB: "Load"}), active_trace_id("trace-1"):
            handler.handle(_record())

        assert sent[0]["message"] == "hello loom.job.name=Load trace_id=trace-1"

    def test_a_failure_sends_its_stack_after_the_line(self, sent: _Sent) -> None:
        try:
            raise ValueError("boom")
        except ValueError:
            exc_info = sys.exc_info()
        record = _record("failed", logging.ERROR, exc_info=exc_info)

        PrefectLogBridgeHandler(uuid.uuid4()).handle(record)

        first, *stack = sent[0]["message"].split("\n")
        assert first.startswith("failed ")
        assert stack[-1] == "ValueError: boom"

    def test_a_record_of_a_prefect_run_logger_is_left_to_prefect(self, sent: _Sent) -> None:
        PrefectLogBridgeHandler(uuid.uuid4()).handle(_record(flow_run_id=uuid.uuid4()))

        assert sent == []

    def test_the_shared_record_is_not_given_the_run_ids(
        self, sent: _Sent, task_run_id: uuid.UUID
    ) -> None:
        record = _record()

        PrefectLogBridgeHandler(uuid.uuid4()).handle(record)

        assert not {"flow_run_id", "task_run_id"} & set(vars(record))

    def test_a_thread_without_the_callers_context_still_carries_the_flow_run(
        self, sent: _Sent, task_run_id: uuid.UUID
    ) -> None:
        flow_id = uuid.uuid4()
        handler = PrefectLogBridgeHandler(flow_id)
        thread = threading.Thread(target=lambda: handler.handle(_record()))

        thread.start()
        thread.join()

        (payload,) = sent
        assert (payload["flow_run_id"], payload["task_run_id"]) == (str(flow_id), None)


class TestLogBridge:
    @staticmethod
    def _bridges() -> list[logging.Handler]:
        return [h for h in logging.getLogger().handlers if isinstance(h, PrefectLogBridgeHandler)]

    def test_ships_inside_the_block_and_flushes_and_detaches_on_exit(
        self, sent: _Sent, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        flushed: list[int] = []
        monkeypatch.setattr(PrefectLogBridgeHandler, "flush", lambda _self: flushed.append(1))
        flow_id = uuid.uuid4()

        with log_bridge(flow_id):
            (attached,) = self._bridges()
            attached.handle(_record())
            flushed_inside = list(flushed)

        assert (sent[0]["flow_run_id"], flushed_inside, flushed) == (str(flow_id), [], [1])
        assert self._bridges() == []

    def test_no_flow_run_attaches_nothing(self) -> None:
        with log_bridge(None):
            attached = self._bridges()

        assert attached == []
