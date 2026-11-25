"""Tests for the Prefect log bridge: loom's records through Prefect's API handler."""

from __future__ import annotations

import logging
import sys
import uuid
from typing import Any

import pytest
from prefect.logging.handlers import APILogWorker

from loom.core.environment import Environment
from loom.core.log_names import LogField
from loom.core.logger import LogIdentity, log_context
from loom.core.model.redaction import Secret
from loom.core.observability.diagnostic import ObservabilityEvent
from loom.core.tracing.context import active_trace_id
from loom.etl.executor._dispatcher import ThreadDispatcher
from loom.prefect.observer._logging_bridge import (
    PrefectLogBridgeHandler,
    bind_task_run,
    current_task_run_id,
    install_log_bridge,
    uninstall_log_bridge,
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


def _record(
    message: str = "hello", level: int = logging.INFO, **extra: object
) -> logging.LogRecord:
    return logging.makeLogRecord(
        {"msg": message, "name": "loom.test", "levelno": level, "levelname": "X", **extra}
    )


class TestPrefectLogBridgeHandler:
    def test_a_step_record_reaches_the_api_with_its_task_run_and_its_flow_run(
        self, sent: _Sent
    ) -> None:
        flow_id, task_id = uuid.uuid4(), uuid.uuid4()
        handler = PrefectLogBridgeHandler(flow_id)

        with bind_task_run(task_id):
            handler.handle(_record("step log", level=logging.WARNING))

        (payload,) = sent
        assert (payload["flow_run_id"], payload["task_run_id"]) == (str(flow_id), str(task_id))
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

    def test_the_message_has_the_service_identity_when_one_is_given(self, sent: _Sent) -> None:
        identity = LogIdentity(environment=Environment.PRODUCTION, service_name="shop")

        PrefectLogBridgeHandler(uuid.uuid4(), identity).handle(_record())

        assert "service.name=shop" in sent[0]["message"]

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

    def test_the_shared_record_is_not_given_the_run_ids(self, sent: _Sent) -> None:
        record = _record()

        with bind_task_run(uuid.uuid4()):
            PrefectLogBridgeHandler(uuid.uuid4()).handle(record)

        assert not {"flow_run_id", "task_run_id"} & set(vars(record))

    def test_a_parallel_step_carries_the_flow_run_of_its_handler(self, sent: _Sent) -> None:
        flow_id, task_id = uuid.uuid4(), uuid.uuid4()
        handler = PrefectLogBridgeHandler(flow_id)

        def step() -> None:
            with bind_task_run(task_id):
                handler.handle(_record())

        ThreadDispatcher().run_all([step])

        (payload,) = sent
        assert (payload["flow_run_id"], payload["task_run_id"]) == (str(flow_id), str(task_id))


class TestBindTaskRun:
    def test_resets_on_exit(self) -> None:
        task_id = uuid.uuid4()

        with bind_task_run(task_id):
            inside = current_task_run_id()

        assert (inside, current_task_run_id()) == (task_id, None)


class TestInstall:
    @staticmethod
    def _bridges() -> list[logging.Handler]:
        return [h for h in logging.getLogger().handlers if isinstance(h, PrefectLogBridgeHandler)]

    def test_installing_again_replaces_the_handler_with_the_new_flow_run(self, sent: _Sent) -> None:
        first, second = uuid.uuid4(), uuid.uuid4()
        install_log_bridge(first)
        install_log_bridge(second)
        try:
            (installed,) = self._bridges()
            installed.handle(_record())
        finally:
            uninstall_log_bridge()

        assert sent[0]["flow_run_id"] == str(second)
        assert self._bridges() == []

    def test_no_flow_run_installs_nothing(self) -> None:
        install_log_bridge(None)

        assert self._bridges() == []
