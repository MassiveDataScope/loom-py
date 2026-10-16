"""Route stdlib log records to the right Prefect TaskRun via a ContextVar."""

from __future__ import annotations

import contextlib
import logging
import queue
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

_current_task_run_id: ContextVar[uuid.UUID | None] = ContextVar(
    "loom_prefect_task_run_id",
    default=None,
)
_current_flow_run_id: ContextVar[uuid.UUID | None] = ContextVar(
    "loom_prefect_flow_run_id",
    default=None,
)


@contextmanager
def bind_task_run(task_run_id: uuid.UUID) -> Iterator[None]:
    """Bind *task_run_id* as the active step's TaskRun for the current context.

    Records emitted while this context manager is active are routed to
    the given TaskRun. Outside it (between steps, or in observer code
    that runs after STEP_END) records fall back to the flow run.

    Args:
        task_run_id: The Prefect TaskRun UUID returned by
            ``client.create_task_run(...)``.
    """
    token = _current_task_run_id.set(task_run_id)
    try:
        yield
    finally:
        _current_task_run_id.reset(token)


def current_task_run_id() -> uuid.UUID | None:
    """Return the active TaskRun id, or ``None`` if no step is running."""
    return _current_task_run_id.get()


def current_flow_run_id() -> uuid.UUID | None:
    """Return the active flow run id set by :func:`install_log_bridge`."""
    return _current_flow_run_id.get()


class PrefectLogBridgeHandler(logging.Handler):
    """logging.Handler that ships records to the Prefect logs API.

    Records carry the installed ``flow_run_id`` and, inside a step, the
    active ``task_run_id`` from the ContextVar too.
    A daemon worker drains the queue and retries with exponential
    backoff up to ``_MAX_ATTEMPTS`` before incrementing ``dropped_logs``.
    """

    _BATCH_SIZE = 100
    _FLUSH_SECONDS = 1.0
    _SENTINEL: Any = object()
    _MAX_ATTEMPTS = 3
    _BACKOFF_BASE_SECONDS = 1.0

    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self._queue: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._worker: threading.Thread | None = None
        self._closed = False
        self.dropped_logs = 0
        self._sleep = time.sleep
        # The flow run this handler was installed for; a step in a pool thread
        # does not inherit the flow body's context.
        self.flow_run_id: uuid.UUID | None = None

    def start(self) -> None:
        """Spawn the background drain worker. Idempotent."""
        if self._worker is not None:
            return
        self._worker = threading.Thread(
            target=self._drain_loop,
            name="loom-prefect-log-bridge",
            daemon=True,
        )
        self._worker.start()

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D401 - inherited
        if self._closed:
            return
        try:
            payload = self._record_to_payload(record)
        except Exception:  # noqa: BLE001
            self.handleError(record)
            return
        if payload is None:
            return
        self._queue.put_nowait(payload)

    def close(self) -> None:  # noqa: D401 - inherited
        """Drain pending logs and stop the worker."""
        if self._closed:
            return
        self._closed = True
        self._queue.put_nowait(self._SENTINEL)
        worker = self._worker
        if worker is not None:
            worker.join(timeout=5.0)
        super().close()

    def _record_to_payload(self, record: logging.LogRecord) -> dict[str, Any] | None:
        task_run_id = _current_task_run_id.get()
        flow_run_id = _current_flow_run_id.get() or self.flow_run_id
        if task_run_id is None and flow_run_id is None:
            return None
        payload: dict[str, Any] = {
            "name": record.name,
            "level": record.levelno,
            "message": record.getMessage(),
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC),
            "flow_run_id": flow_run_id,
        }
        if task_run_id is not None:
            payload["task_run_id"] = task_run_id
        return payload

    def _drain_loop(self) -> None:
        from loom.prefect._async import run_sync  # noqa: PLC0415

        batch: list[dict[str, Any]] = []
        while True:
            try:
                item = self._queue.get(timeout=self._FLUSH_SECONDS)
            except queue.Empty:
                batch = self._flush_batch(batch, run_sync)
                continue
            if item is self._SENTINEL:
                self._drain_remaining(batch, run_sync)
                return
            batch.append(item)
            if len(batch) >= self._BATCH_SIZE:
                batch = self._flush_batch(batch, run_sync)

    def _flush_batch(self, batch: list[dict[str, Any]], run_sync_fn: Any) -> list[dict[str, Any]]:
        if batch:
            self._flush_with_retry(batch, run_sync_fn=run_sync_fn)
        return []

    def _drain_remaining(self, batch: list[dict[str, Any]], run_sync_fn: Any) -> None:
        self._flush_batch(batch, run_sync_fn)
        while True:
            try:
                leftover = self._queue.get_nowait()
            except queue.Empty:
                return
            if leftover is self._SENTINEL:
                continue
            self._flush_with_retry([leftover], run_sync_fn=run_sync_fn)

    def _flush_with_retry(self, batch: list[dict[str, Any]], run_sync_fn: Any) -> None:
        for attempt in range(1, self._MAX_ATTEMPTS + 1):
            try:
                self._post_batch(batch, run_sync_fn)
                return
            except Exception:  # noqa: BLE001
                if attempt >= self._MAX_ATTEMPTS:
                    break
                self._sleep(self._BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
        self.dropped_logs += len(batch)
        with contextlib.suppress(Exception):
            self.handleError(logging.LogRecord(__name__, logging.ERROR, "", 0, "", None, None))

    def _post_batch(self, batch: list[dict[str, Any]], run_sync_fn: Any) -> None:
        from prefect.client.orchestration import get_client  # noqa: PLC0415
        from prefect.client.schemas.actions import LogCreate  # noqa: PLC0415

        logs = [LogCreate(**payload) for payload in batch]

        async def _send() -> None:
            async with get_client() as client:
                await client.create_logs(logs)

        run_sync_fn(_send())


class _BridgeRegistry:
    """Tracks the singleton handler attached to the root logger."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._handler: PrefectLogBridgeHandler | None = None
        self._token: Any = None

    def install(self, flow_run_id: uuid.UUID | None) -> None:
        if flow_run_id is None:
            return
        with self._lock:
            if self._handler is not None:
                return
            handler = PrefectLogBridgeHandler()
            handler.flow_run_id = flow_run_id
            handler.start()
            logging.getLogger().addHandler(handler)
            self._handler = handler
            self._token = _current_flow_run_id.set(flow_run_id)

    def uninstall(self) -> None:
        with self._lock:
            if self._handler is None:
                return
            logging.getLogger().removeHandler(self._handler)
            self._handler.close()
            self._handler = None
            if self._token is not None:
                with contextlib.suppress(ValueError):
                    _current_flow_run_id.reset(self._token)
                self._token = None


_registry = _BridgeRegistry()


def install_log_bridge(flow_run_id: uuid.UUID | None) -> None:
    """Attach the bridge handler to the root logger. Idempotent, ``None`` is a no-op."""
    _registry.install(flow_run_id)


def uninstall_log_bridge() -> None:
    """Detach the bridge handler and clear the flow_run_id binding."""
    _registry.uninstall()


__all__ = [
    "PrefectLogBridgeHandler",
    "bind_task_run",
    "current_flow_run_id",
    "current_task_run_id",
    "install_log_bridge",
    "uninstall_log_bridge",
]
