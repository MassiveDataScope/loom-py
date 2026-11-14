"""The keys the framework owns, which a caller's DI module may not bind."""

from __future__ import annotations

import functools
import sys
from collections.abc import Callable, Iterable, Sequence

from loom.composition.errors import ReservedBindingError
from loom.core.authz.elevation import ElevationSink
from loom.core.bootstrap.kernel import KERNEL_KEYS
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.persistence.stores import DEFAULT_STORE, StoreCatalog, StoreKey, store_binding
from loom.core.sql import CallerBoundSql, SqlQueryService

_RESERVED: tuple[BindingKey, ...] = (
    SqlQueryService,
    CallerBoundSql,
    ObservabilityRuntime,
    StoreCatalog,
    ElevationSink,
    *KERNEL_KEYS,
)

_STORE_SESSIONS = (
    ("loom.core.repository.sqlalchemy.session_manager", "SessionManager"),
    ("loom.core.repository.mongo.uow", "MongoSessions"),
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


def refusing_reserved(
    modules: Sequence[Callable[[LoomContainer], None]], stores: Sequence[StoreKey]
) -> list[Callable[[LoomContainer], None]]:
    """Wrap each caller module so binding or replacing a reserved key fails right after it runs.

    ``JobService``, repositories and ``DefaultRepositoryBuilder`` are not
    reserved: the framework registers them only where no module did.

    Raises:
        ReservedBindingError: From the wrapped module that bound a reserved key.
    """
    return [_refusing(module, stores) for module in modules]


def _refusing(
    module: Callable[[LoomContainer], None], stores: Sequence[StoreKey]
) -> Callable[[LoomContainer], None]:
    @functools.wraps(module)
    def register(container: LoomContainer) -> None:
        view = container.bindings()
        before = dict(view)
        module(container)
        for key in reserved_keys(stores):
            if view.get(key) is not before.get(key):
                raise ReservedBindingError.for_key(module, key)

    return register
