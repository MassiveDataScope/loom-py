"""PrefectObserver — loom's lifecycle records, plus the pipeline summary artifact for Prefect UI."""

from __future__ import annotations

from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.core.observability.observer.log import LogLifecycleObserver

_LIFECYCLE_LOG = LogLifecycleObserver()


class PrefectObserver:
    """``LifecycleObserver`` that writes loom's lifecycle records and a pipeline summary.

    Each event is the record :class:`LogLifecycleObserver` writes on a ``loom.*``
    logger, inside a run or not. Prefect UI shows it through loom's log bridge in a
    loom flow, or through ``PREFECT_LOGGING_EXTRA_LOGGERS=loom`` in a flow of yours.
    At the end of a pipeline inside a flow run it also publishes the summary artifact.

    Coexists with ``LineageObserver`` in the same ``Observability``:
    - ``PrefectObserver`` → lifecycle records and the Prefect summary artifact.
    - ``LineageObserver`` → loom lineage tables (permanent audit trail).

    Example::

        runtime = Observability([PrefectObserver(), LineageObserver(...)])
    """

    def on_event(self, event: LifecycleEvent) -> None:
        """Handle one lifecycle event from the ``Observability``.

        Args:
            event: Immutable lifecycle event emitted by the runtime.
        """
        _LIFECYCLE_LOG.on_event(event)
        if event.kind is EventKind.END and event.scope is Scope.PIPELINE:
            _publish_summary_artifact(event)


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
