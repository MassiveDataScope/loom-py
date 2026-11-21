"""Run-time helpers shared by flow bodies: manifest lifecycle and observers."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime

from loom.core.observability.protocol import LifecycleObserver
from loom.prefect.manifest import ManifestStore, RunManifest
from loom.prefect.observer import ManifestObserver, PrefectTaskRunObserver


def load_or_init_manifest(store: ManifestStore | None, correlation_id: str) -> RunManifest:
    """Load the manifest for *correlation_id*, or start an empty one."""
    loaded = store.load(correlation_id) if store is not None else None
    if loaded is not None:
        return loaded
    return RunManifest(
        correlation_id=correlation_id,
        steps=(),
        updated_at=datetime.now(tz=UTC),
    )


def maybe_delete_manifest(store: ManifestStore | None, correlation_id: str) -> None:
    """Delete the manifest for *correlation_id* when a store is configured."""
    if store is None:
        return
    store.delete(correlation_id)


def build_observers(
    flow_run_id: uuid.UUID | None,
    manifest_store: ManifestStore | None,
    manifest: RunManifest | None,
) -> list[LifecycleObserver]:
    """Assemble the lifecycle observers for one runner invocation."""
    observers: list[LifecycleObserver] = []
    if flow_run_id is not None:
        observers.append(PrefectTaskRunObserver(flow_run_id=flow_run_id))
    if manifest_store is not None and manifest is not None:
        observers.append(ManifestObserver(manifest_store, manifest))
    prometheus = _maybe_build_prometheus_adapter()
    if prometheus is not None:
        observers.append(prometheus)
    return observers


def _maybe_build_prometheus_adapter() -> LifecycleObserver | None:
    pushgateway = os.environ.get("PROMETHEUS_PUSHGATEWAY_URL")
    if not pushgateway:
        return None
    try:
        from loom.prometheus.lifecycle import PrometheusLifecycleObserver  # noqa: PLC0415
    except ImportError:
        return None
    return PrometheusLifecycleObserver(pushgateway_url=pushgateway)


__all__ = ["build_observers", "load_or_init_manifest", "maybe_delete_manifest"]
