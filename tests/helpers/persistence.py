"""Build a persistence backend the way the composition root does."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from loom.core.config import ConfigContext
from loom.core.model import BaseModel
from loom.core.persistence import (
    SERVER_PROFILE,
    BackendRuntime,
    PersistenceBackend,
    PersistenceWiring,
    PoolMetricsSink,
    RuntimeProfile,
)
from loom.core.persistence.stores import DEFAULT_STORE, StoreCatalog, StoreKey, load_catalog


def build_backend(
    backend: PersistenceBackend,
    ctx: ConfigContext,
    models: Sequence[type[BaseModel]] = (),
    profile: RuntimeProfile = SERVER_PROFILE,
    *,
    pool_metrics: PoolMetricsSink | None = None,
    connection_only: bool = False,
    store: StoreKey = DEFAULT_STORE,
) -> PersistenceWiring:
    """Build *backend* for *store* with the catalog of *ctx*, loaded once, as a composition does."""
    runtime = BackendRuntime(
        profile,
        pool_metrics=pool_metrics,
        connection_only=connection_only,
        catalog=load_catalog(ctx),
        store=store,
    )
    return backend.build(ctx, models, runtime)


def record_catalogs(monkeypatch: pytest.MonkeyPatch) -> list[StoreCatalog]:
    """Record every :class:`StoreCatalog` built from now on, in order."""
    built: list[StoreCatalog] = []
    construct = StoreCatalog.__init__

    def recorded(self: StoreCatalog, *args: Any, **kwargs: Any) -> None:
        construct(self, *args, **kwargs)
        built.append(self)

    monkeypatch.setattr(StoreCatalog, "__init__", recorded)
    return built
