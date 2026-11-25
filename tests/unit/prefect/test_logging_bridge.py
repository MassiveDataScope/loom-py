"""Tests for the Prefect log bridge: loom's records through Prefect's API handler."""

from __future__ import annotations

import inspect
import logging
import sys
import threading
import uuid
from collections.abc import Iterator
from typing import Any

import msgspec
import pytest
from prefect.logging.handlers import APILogWorker, set_api_log_sink

from loom.core.log_names import LogField
from loom.core.logger import log_context
from loom.core.model.redaction import Secret
from loom.core.observability.diagnostic import ObservabilityEvent
from loom.core.tracing.context import active_trace_id
from loom.prefect.flow import _assemble
from loom.prefect.flow._assemble import assemble_flow, flow_settings_from_mapping
from loom.prefect.observer._logging_bridge import (
    PrefectLogBridgeHandler,
    active_task_run_id,
    log_bridge,
)


@pytest.fixture
def sent() -> Iterator[list[dict[str, Any]]]:
    """The payloads Prefect's API handler produces."""
    payloads: list[dict[str, Any]] = []
    set_api_log_sink(payloads.append)
    yield payloads
    set_api_log_sink(None)


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
        self, sent: list[dict[str, Any]], task_run_id: uuid.UUID
    ) -> None:
        flow_id = uuid.uuid4()

        PrefectLogBridgeHandler(flow_id).handle(_record("step log", level=logging.WARNING))

        (payload,) = sent
        assert (payload["flow_run_id"], payload["task_run_id"]) == (str(flow_id), str(task_run_id))
        assert (payload["message"], payload["level"]) == ("step log", logging.WARNING)

    def test_a_record_between_steps_carries_the_flow_run_only(
        self, sent: list[dict[str, Any]]
    ) -> None:
        flow_id = uuid.uuid4()

        PrefectLogBridgeHandler(flow_id).handle(_record())

        (payload,) = sent
        assert payload["flow_run_id"] == str(flow_id)
        assert payload["task_run_id"] is None

    def test_the_message_has_the_fields_as_loom_renders_them_and_no_code_origin(
        self, sent: list[dict[str, Any]]
    ) -> None:
        record = _record(
            ObservabilityEvent.LIFECYCLE_FAILED,
            **{LogField.NAME: "Load step", "pw": Secret("x"), LogField.CODE_LINE: 3},
        )

        PrefectLogBridgeHandler(uuid.uuid4()).handle(record)

        assert sent[0]["message"] == (
            f'{ObservabilityEvent.LIFECYCLE_FAILED} loom.name="Load step" pw=***'
        )

    def test_the_message_has_the_running_context_and_the_trace(
        self, sent: list[dict[str, Any]]
    ) -> None:
        handler = PrefectLogBridgeHandler(uuid.uuid4())

        with log_context({LogField.JOB: "Load"}), active_trace_id("trace-1"):
            handler.handle(_record())

        assert sent[0]["message"] == "hello loom.job.name=Load trace_id=trace-1"

    def test_a_failure_sends_its_stack_after_the_line(self, sent: list[dict[str, Any]]) -> None:
        try:
            raise ValueError("boom")
        except ValueError:
            exc_info = sys.exc_info()
        record = _record("failed", logging.ERROR, exc_info=exc_info)

        PrefectLogBridgeHandler(uuid.uuid4()).handle(record)

        first, *stack = sent[0]["message"].split("\n")
        assert first.startswith("failed ")
        assert stack[-1] == "  | ValueError: boom"

    def test_the_shared_record_is_not_given_the_run_ids(
        self, sent: list[dict[str, Any]], task_run_id: uuid.UUID
    ) -> None:
        record = _record()

        PrefectLogBridgeHandler(uuid.uuid4()).handle(record)

        assert not {"flow_run_id", "task_run_id"} & set(vars(record))

    def test_a_thread_without_the_callers_context_still_carries_the_flow_run(
        self, sent: list[dict[str, Any]], task_run_id: uuid.UUID
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
        return [
            h for h in logging.getLogger("loom").handlers if isinstance(h, PrefectLogBridgeHandler)
        ]

    def test_ships_only_loom_records_and_drains_and_detaches_on_exit(
        self, sent: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        drained: list[int] = []
        monkeypatch.setattr(APILogWorker, "drain_all", lambda **_kw: drained.append(1))
        flow_id = uuid.uuid4()
        library_logger = logging.getLogger("a.library")

        with log_bridge(flow_id):
            logging.getLogger("loom.test").warning(ObservabilityEvent.LIFECYCLE_FAILED)
            library_logger.warning("theirs")
            drained_inside = list(drained)

        assert [p["message"] for p in sent] == [ObservabilityEvent.LIFECYCLE_FAILED]
        assert (drained_inside, drained, self._bridges()) == ([], [1], [])

    def test_no_flow_run_attaches_nothing(self) -> None:
        with log_bridge(None):
            attached = self._bridges()

        assert attached == []

    def test_a_flow_body_runs_inside_the_bridge(self, monkeypatch: pytest.MonkeyPatch) -> None:
        flow_id = uuid.uuid4()
        inside: list[list[logging.Handler]] = []
        monkeypatch.setattr(_assemble, "prefect_flow_run_id", lambda: flow_id)
        flow = assemble_flow(
            name="probe",
            body=lambda **_kw: inside.append(self._bridges()),
            signature=inspect.Signature(),
            params_type=msgspec.Struct,
            settings=flow_settings_from_mapping({}),
            config_path="probe.yaml",
            source_file=__file__,
        )

        flow.fn()

        assert len(inside[0]) == 1
        assert self._bridges() == []
