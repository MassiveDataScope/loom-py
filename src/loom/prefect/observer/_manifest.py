"""``LifecycleObserver`` that mirrors STEP events into a ``RunManifest``."""

from __future__ import annotations

import logging
from typing import Any

from loom.core.log_names import LogField
from loom.etl.lineage._records import RunStatus
from loom.prefect.event import PrefectEvent
from loom.prefect.manifest import ManifestStore, RunManifest, mark_step

_log = logging.getLogger(__name__)


class ManifestObserver:
    """Persist the manifest after every STEP end / error transition.

    Used by the flow body so a failure mid-run leaves a recoverable
    snapshot. On full success the flow body deletes the manifest
    entirely; on failure it stays and the next attempt loads it via
    :func:`loom.prefect.manifest.completed_steps` to skip already-SUCCESS
    steps.

    Args:
        store: Backend that persists each updated manifest.
        initial: The manifest at flow body entry. Subsequent
            ``on_event`` calls produce new manifests via
            :func:`mark_step` so this object is never mutated externally.
    """

    def __init__(self, store: ManifestStore, initial: RunManifest) -> None:
        self._store = store
        self._manifest = initial

    def on_event(self, event: Any) -> None:
        """Handle one lifecycle event from the ``Observability``.

        Only ``Scope.STEP`` ``END`` / ``ERROR`` events update the manifest.

        Args:
            event: Immutable lifecycle event.
        """
        from loom.core.observability.event import EventKind, Scope  # noqa: PLC0415

        if event.scope is not Scope.STEP:
            return
        if event.kind is EventKind.END:
            status = RunStatus.SUCCESS
        elif event.kind is EventKind.ERROR:
            status = RunStatus.FAILED
        else:
            return

        self._manifest = mark_step(self._manifest, event.name, status)
        try:
            self._store.save(self._manifest)
        except Exception:  # noqa: BLE001
            _log.warning(
                PrefectEvent.MANIFEST_SAVE_FAILED,
                extra={LogField.NAME: event.name, LogField.STATUS: status},
                exc_info=True,
            )


__all__ = ["ManifestObserver"]
