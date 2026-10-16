"""Tests for ``loom.prefect.observer._logging_bridge``.

Verifies:
- The handler emits ``LogCreate``-compatible payloads tagged with the
  active ``flow_run_id`` and, inside a step, its ``task_run_id`` too, so a
  reader that scopes logs by flow run also gets the steps' lines.
- Records emitted with neither binding are silently dropped.
- ``install_log_bridge`` / ``uninstall_log_bridge`` are idempotent.
"""

from __future__ import annotations

import logging
import uuid

import pytest

from loom.prefect.observer._logging_bridge import (
    PrefectLogBridgeHandler,
    _current_flow_run_id,
    bind_task_run,
    install_log_bridge,
    uninstall_log_bridge,
)


def _make_record(message: str = "hello") -> logging.LogRecord:
    return logging.LogRecord(
        name="loom.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=None,
        exc_info=None,
    )


def test_emit_in_a_step_carries_its_task_run_and_its_flow_run() -> None:
    handler = PrefectLogBridgeHandler()
    flow_id = uuid.uuid4()
    task_id = uuid.uuid4()
    token = _current_flow_run_id.set(flow_id)
    try:
        with bind_task_run(task_id):
            payload = handler._record_to_payload(_make_record("step log"))
    finally:
        _current_flow_run_id.reset(token)
        handler.close()

    assert payload is not None
    assert payload["task_run_id"] == task_id
    assert payload["flow_run_id"] == flow_id
    assert payload["message"] == "step log"
    assert payload["level"] == logging.INFO


def test_emit_falls_back_to_flow_run_id_when_no_task_bound() -> None:
    handler = PrefectLogBridgeHandler()
    flow_id = uuid.uuid4()
    token = _current_flow_run_id.set(flow_id)
    try:
        payload = handler._record_to_payload(_make_record("between-steps log"))
    finally:
        _current_flow_run_id.reset(token)
        handler.close()

    assert payload is not None
    assert payload["flow_run_id"] == flow_id
    assert "task_run_id" not in payload


def test_emit_returns_none_when_neither_binding_is_active() -> None:
    handler = PrefectLogBridgeHandler()
    try:
        payload = handler._record_to_payload(_make_record("orphan log"))
    finally:
        handler.close()
    assert payload is None


def test_bind_task_run_resets_on_exit() -> None:
    task_id = uuid.uuid4()
    from loom.prefect.observer._logging_bridge import current_task_run_id

    assert current_task_run_id() is None
    with bind_task_run(task_id):
        assert current_task_run_id() == task_id
    assert current_task_run_id() is None


def test_install_log_bridge_is_idempotent() -> None:
    flow_id = uuid.uuid4()
    install_log_bridge(flow_id)
    try:
        first_handlers = [
            h for h in logging.getLogger().handlers if isinstance(h, PrefectLogBridgeHandler)
        ]
        install_log_bridge(flow_id)
        second_handlers = [
            h for h in logging.getLogger().handlers if isinstance(h, PrefectLogBridgeHandler)
        ]
        # Second call must not double-attach.
        assert len(first_handlers) == 1
        assert len(second_handlers) == 1
        assert first_handlers[0] is second_handlers[0]
    finally:
        uninstall_log_bridge()

    # After uninstall, no PrefectLogBridgeHandler on the root logger.
    assert not any(isinstance(h, PrefectLogBridgeHandler) for h in logging.getLogger().handlers)


def test_a_step_in_a_worker_thread_carries_the_installed_flow_run() -> None:
    from concurrent.futures import ThreadPoolExecutor

    flow_id = uuid.uuid4()
    task_id = uuid.uuid4()
    install_log_bridge(flow_id)
    try:
        (handler,) = [
            h for h in logging.getLogger().handlers if isinstance(h, PrefectLogBridgeHandler)
        ]

        def step() -> dict[str, object] | None:
            # A parallel step: a pool thread, which does not inherit the flow body's context.
            with bind_task_run(task_id):
                return handler._record_to_payload(_make_record("parallel step log"))

        with ThreadPoolExecutor(max_workers=1) as pool:
            payload = pool.submit(step).result()
    finally:
        uninstall_log_bridge()

    assert payload is not None
    assert payload["task_run_id"] == task_id
    assert payload["flow_run_id"] == flow_id


def test_install_log_bridge_none_flow_id_is_a_noop() -> None:
    install_log_bridge(None)
    assert not any(isinstance(h, PrefectLogBridgeHandler) for h in logging.getLogger().handlers)


def test_flush_retries_then_increments_dropped_counter() -> None:
    handler = PrefectLogBridgeHandler()
    attempts: list[int] = []

    def _always_fail(batch: list[dict[str, object]], run_sync_fn: object) -> None:
        attempts.append(1)
        raise RuntimeError("simulated outage")

    handler._post_batch = _always_fail  # type: ignore[method-assign]
    handler._sleep = lambda _s: None  # type: ignore[attr-defined]
    try:
        handler._flush_with_retry([{"name": "x", "level": 20, "message": "m"}], run_sync_fn=None)
    finally:
        handler.close()
    assert len(attempts) == handler._MAX_ATTEMPTS
    assert handler.dropped_logs == 1


def test_flush_succeeds_on_second_attempt_no_drop() -> None:
    handler = PrefectLogBridgeHandler()
    attempts: list[int] = []

    def _fail_then_succeed(batch: list[dict[str, object]], run_sync_fn: object) -> None:
        attempts.append(1)
        if len(attempts) < 2:
            raise RuntimeError("transient")

    handler._post_batch = _fail_then_succeed  # type: ignore[method-assign]
    handler._sleep = lambda _s: None  # type: ignore[attr-defined]
    try:
        handler._flush_with_retry([{"name": "x", "level": 20, "message": "m"}], run_sync_fn=None)
    finally:
        handler.close()
    assert len(attempts) == 2
    assert handler.dropped_logs == 0


@pytest.mark.parametrize("level", [logging.DEBUG, logging.WARNING, logging.ERROR])
def test_record_level_is_preserved(level: int) -> None:
    handler = PrefectLogBridgeHandler()
    task_id = uuid.uuid4()
    record = logging.LogRecord(
        name="loom.test",
        level=level,
        pathname=__file__,
        lineno=1,
        msg="msg",
        args=None,
        exc_info=None,
    )
    try:
        with bind_task_run(task_id):
            payload = handler._record_to_payload(record)
    finally:
        handler.close()
    assert payload is not None
    assert payload["level"] == level
