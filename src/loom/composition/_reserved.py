"""The keys the framework owns, which a caller's DI module may not bind."""

from __future__ import annotations

import sys
from collections.abc import Iterable

from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap.kernel import KERNEL_KEYS
from loom.core.di.container import BindingKey
from loom.core.observability.observability import ObservabilityRuntime
from loom.core.stores.catalog import DEFAULT_STORE, StoreCatalog, StoreKey, store_binding
from loom.core.stores.repository import RepositoryCatalog
from loom.core.stores.sql import CallerBoundSql, SqlQueryService

_RESERVED: tuple[BindingKey, ...] = (
    SqlQueryService,
    CallerBoundSql,
    ObservabilityRuntime,
    StoreCatalog,
    RepositoryCatalog,
    ElevationSink,
    *KERNEL_KEYS,
)

_STORE_SESSIONS = (
    ("loom.stores.sqlalchemy.session_manager", "SessionManager"),
    ("loom.stores.mongo.uow", "MongoSessions"),
)


def reserved_keys(stores: Iterable[StoreKey]) -> tuple[BindingKey, ...]:
    """The framework-owned keys: its own services, the kernel's and each store's sessions."""
    sessions = [
        getattr(module, name)
        for module_name, name in _STORE_SESSIONS
        if (module := sys.modules.get(module_name)) is not None
    ]
    keys = dict.fromkeys((DEFAULT_STORE, *stores))
    return (*_RESERVED, *(store_binding(session, key) for session in sessions for key in keys))
