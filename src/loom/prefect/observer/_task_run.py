"""Synthesise per-step Prefect TaskRuns from loom lifecycle events."""

from __future__ import annotations

import contextlib
import logging
import uuid
from contextvars import Token
from typing import Any

from loom.core.errors.boundary import boundary
from loom.core.errors.failure import Failure
from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.prefect._async import run_sync
from loom.prefect.observer._logging_bridge import _current_task_run_id

_log = logging.getLogger(__name__)


class PrefectTaskRunObserver:
    """``LifecycleObserver`` that creates Prefect TaskRuns per loom step.

    Args:
        flow_run_id: UUID of the active Prefect flow run. Passed explicitly
            (instead of looked up from ``prefect.runtime``) so the observer
            can be unit-tested in isolation.

    Example::

        from loom.prefect import PrefectTaskRunObserver
        from prefect.runtime import flow_run

        observer = PrefectTaskRunObserver(flow_run_id=flow_run.id)
        runner = ETLRunner.from_yaml(path, extra_observers=[observer])
        runner.run(pipeline, params)
    """

    def __init__(self, flow_run_id: uuid.UUID | str) -> None:
        self._flow_run_id = (
            flow_run_id if isinstance(flow_run_id, uuid.UUID) else uuid.UUID(str(flow_run_id))
        )
        self._task_runs: dict[str, uuid.UUID] = {}
        self._tokens: dict[str, Token[uuid.UUID | None]] = {}
        self._task_markers: dict[str, Any] = {}

    def on_event(self, event: LifecycleEvent) -> None:
        """Dispatch one lifecycle event."""
        if event.scope is not Scope.STEP:
            return
        _log.debug(
            "PrefectTaskRunObserver event step=%s kind=%s step_run_id=%s",
            event.name,
            event.kind,
            event.id,
        )
        try:
            match event.kind:
                case EventKind.START:
                    self._on_start(event)
                case EventKind.END:
                    self._on_end(event)
                case EventKind.ERROR:
                    self._on_error(event)
        except Exception:  # noqa: BLE001
            _log.warning(
                "PrefectTaskRunObserver swallowed exception (step=%s, kind=%s)",
                event.name,
                event.kind,
                exc_info=True,
            )

    def _on_start(self, event: LifecycleEvent) -> None:
        step_run_id = event.id
        if step_run_id is None:
            return
        task_run_id = self._create_task_run(event)
        if task_run_id is not None:
            self._task_runs[step_run_id] = task_run_id
            self._tokens[step_run_id] = _current_task_run_id.set(task_run_id)

    def _on_end(self, event: LifecycleEvent) -> None:
        self._reset_log_binding(event.id)
        task_run_id = self._task_runs.pop(event.id, None) if event.id else None
        if task_run_id is None:
            return
        self._set_state(task_run_id, completed=True)

    def _on_error(self, event: LifecycleEvent) -> None:
        self._reset_log_binding(event.id)
        task_run_id = self._task_runs.pop(event.id, None) if event.id else None
        if task_run_id is None:
            return
        self._set_state(
            task_run_id,
            completed=False,
            message=None if event.failure is None else Failure(event.failure).type,
        )

    def _reset_log_binding(self, step_run_id: str | None) -> None:
        if step_run_id is None:
            return
        token = self._tokens.pop(step_run_id, None)
        if token is None:
            return
        with contextlib.suppress(ValueError):
            _current_task_run_id.reset(token)

    def _create_task_run(self, event: LifecycleEvent) -> uuid.UUID | None:
        from prefect.client.orchestration import get_client  # noqa: PLC0415
        from prefect.states import Running  # noqa: PLC0415

        flow_run_id = self._active_flow_run_id()
        marker = self._step_marker(event.name)

        async def _create() -> uuid.UUID:
            async with get_client() as client:
                created = await client.create_task_run(
                    task=marker,
                    flow_run_id=flow_run_id,
                    dynamic_key=str(event.id),
                    name=event.name,
                    state=Running(),
                    extra_tags=["loom-step"],
                )
                return uuid.UUID(str(created.id))

        result = run_sync(_create())
        return result if isinstance(result, uuid.UUID) else None

    def _active_flow_run_id(self) -> uuid.UUID:
        """Return the Prefect flow run ID for the currently active flow context.

        Inside a Prefect subflow the runtime context reflects the subflow's ID,
        so task runs are automatically attached to the correct process subflow.
        Falls back to the stored ``_flow_run_id`` when called outside a live
        Prefect context (unit tests, non-Prefect execution).
        """
        try:
            from prefect.runtime import flow_run as _fr  # noqa: PLC0415

            if _fr.id:
                return uuid.UUID(str(_fr.id))
        except (ImportError, AttributeError):
            pass
        return self._flow_run_id

    def _step_marker(self, name: str) -> Any:
        """Return a cached no-op ``@task`` for *name* used as orchestration marker."""
        marker = self._task_markers.get(name)
        if marker is not None:
            return marker
        from prefect import task  # noqa: PLC0415

        @task(name=name)
        @boundary
        def _step_marker() -> None:  # pragma: no cover
            return None

        self._task_markers[name] = _step_marker
        return _step_marker

    def _set_state(
        self, task_run_id: uuid.UUID, *, completed: bool, message: str | None = None
    ) -> None:
        from prefect.client.orchestration import get_client  # noqa: PLC0415
        from prefect.states import Completed, Failed  # noqa: PLC0415

        state: Any = Completed() if completed else Failed(message=message or "loom step failed")

        async def _set() -> None:
            async with get_client() as client:
                await client.set_task_run_state(task_run_id, state, force=True)

        run_sync(_set())


__all__ = ["PrefectTaskRunObserver"]
