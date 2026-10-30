"""The keys the framework owns, which a caller's DI module may not bind."""

from __future__ import annotations

import functools
import sys
from collections.abc import Callable, Sequence

from loom.composition.errors import ReservedBindingError
from loom.core.authz.elevation import ElevationSink
from loom.core.di.container import BindingKey, LoomContainer
from loom.core.engine.executor import RuntimeExecutor
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.sql import CallerBoundSql, SqlQueryService
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.registry import UseCaseRegistry

_RESERVED: tuple[BindingKey, ...] = (
    SqlQueryService,
    CallerBoundSql,
    ObservabilityRuntime,
    ElevationSink,
    RuntimeExecutor,
    UseCaseRegistry,
    ApplicationInvoker,
)

# Looked up, not imported: importing it would load SQLAlchemy into every
# composition. A module cannot bind a class whose module was never imported.
_SESSION_MANAGER_MODULE = "loom.core.repository.sqlalchemy.session_manager"


def reserved_keys() -> tuple[BindingKey, ...]:
    """The framework-owned keys: its own services, the kernel's and the store's session manager."""
    session_module = sys.modules.get(_SESSION_MANAGER_MODULE)
    if session_module is None:
        return _RESERVED
    return (*_RESERVED, session_module.SessionManager)


def refusing_reserved(
    modules: Sequence[Callable[[LoomContainer], None]],
) -> list[Callable[[LoomContainer], None]]:
    """Wrap each caller module so binding a reserved key fails right after it runs.

    ``JobService``, repositories and ``DefaultRepositoryBuilder`` are not
    reserved: the framework registers them only where no module did.

    Raises:
        ReservedBindingError: From the wrapped module that bound a reserved key.
    """
    return [_refusing(module) for module in modules]


def _refusing(module: Callable[[LoomContainer], None]) -> Callable[[LoomContainer], None]:
    @functools.wraps(module)
    def register(container: LoomContainer) -> None:
        module(container)
        for key in reserved_keys():
            if container.is_registered(key):
                raise ReservedBindingError.for_key(module, key)

    return register
