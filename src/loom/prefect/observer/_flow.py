"""PrefectObserver — forwards loom lifecycle events to Prefect UI."""

from __future__ import annotations

import logging

import prefect
from prefect.exceptions import MissingContextError

from loom.core.logger.failure import exception_type
from loom.core.observability.event import EventKind, LifecycleEvent, Scope


class PrefectObserver:
    """``LifecycleObserver`` that forwards loom events to Prefect's run logger.

    Coexists with ``LineageObserver`` in the same ``Observability``:
    - ``PrefectObserver`` → Prefect UI logs and artifacts.
    - ``LineageObserver`` → loom lineage tables (permanent audit trail).

    Degrades gracefully to ``logging.getLogger()`` when no Prefect run context
    is active (unit tests, local dry-run).  Never raises from ``on_event``.

    Example::

        runtime = Observability([PrefectObserver(), LineageObserver(...)])
    """

    def on_event(self, event: LifecycleEvent) -> None:
        """Handle one lifecycle event from the ``Observability``.

        Args:
            event: Immutable lifecycle event emitted by the runtime.
        """
        try:
            logger = prefect.get_run_logger()
        except MissingContextError:
            logger = logging.getLogger(__name__)

        match event.kind:
            case EventKind.START:
                logger.info("[%s] START %s", event.scope, event.name)
            case EventKind.END:
                logger.info(
                    "[%s] END %s %.1fms",
                    event.scope,
                    event.name,
                    event.duration_ms if event.duration_ms is not None else 0.0,
                )
                if event.scope is Scope.PIPELINE:
                    _publish_summary_artifact(event)
            case EventKind.ERROR:
                logger.error(
                    "[%s] ERROR %s: %s",
                    event.scope,
                    event.name,
                    None if event.failure is None else exception_type(type(event.failure)),
                )


def _publish_summary_artifact(event: LifecycleEvent) -> None:
    from prefect.runtime import flow_run  # noqa: PLC0415

    if flow_run.id is None:
        return

    from prefect.artifacts import create_markdown_artifact  # noqa: PLC0415

    duration = f"{event.duration_ms:.1f}ms" if event.duration_ms is not None else "n/a"
    create_markdown_artifact(
        key="pipeline-summary",
        markdown=(f"## {event.name}\n\n- Status: {event.status}\n- Duration: {duration}\n"),
    )


__all__ = ["PrefectObserver"]
