"""Build a store backend the way the composition root does."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from loom.connections.aws import AwsClients
from loom.core.config import ConfigContext
from loom.core.model import BaseModel
from loom.core.stores import (
    SERVER_PROFILE,
    PoolMetricsSink,
    RuntimeProfile,
    StorePlugin,
    StoreRuntime,
    StoreWiring,
)
from loom.core.stores.catalog import DEFAULT_STORE, StoreCatalog, StoreKey, load_catalog
from loom.core.stores.repository import CursorCodec


def build_backend(
    backend: StorePlugin,
    ctx: ConfigContext,
    models: Sequence[type[BaseModel]] = (),
    profile: RuntimeProfile = SERVER_PROFILE,
    *,
    pool_metrics: PoolMetricsSink | None = None,
    connection_only: bool = False,
    store: StoreKey = DEFAULT_STORE,
    aws: AwsClients | None = None,
) -> StoreWiring:
    """Build *backend* for *store* with the catalog of *ctx*, loaded once, as a composition does."""
    runtime = StoreRuntime(
        profile,
        pool_metrics=pool_metrics,
        connection_only=connection_only,
        catalog=load_catalog(ctx),
        store=store,
        cursor_codec=CursorCodec.from_context(ctx),
        aws=AwsClients() if aws is None else aws,
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
