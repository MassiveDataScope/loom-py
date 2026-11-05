"""Persistence backend plugin contract.

A backend is resolved by name from the ``loom.persistence.backends`` entry
point group and asked once, at bootstrap, for everything the host needs to
wire persistence: the unit-of-work factory, the DI module registering
repositories, the startup checks, the open/close lifespan, and a model
preparation step. The repository class the capability gate inspects is a
class fact of the backend, known before ``build``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.persistence.profile import BackendRuntime
from loom.core.uow.abc import UnitOfWorkFactory

if TYPE_CHECKING:
    from loom.core.persistence.elevation import StoreElevation
    from loom.core.persistence.stores import StoreCapabilities, StoreSpec


def no_model_preparation(models: Sequence[type[BaseModel]]) -> None:
    """Prepare nothing: the default for a backend whose models need no compilation.

    Args:
        models: Discovered models; ignored.
    """


@asynccontextmanager
async def no_lifespan() -> AsyncIterator[None]:
    """Hold no startup resource: the default lifespan of a backend without one."""
    yield


async def no_startup_checks() -> None:
    """Check nothing: the default for a backend that does not verify a schema."""


@dataclass(frozen=True)
class PersistenceWiring:
    """Everything a host needs from a persistence backend, resolved once at bootstrap.

    Args:
        uow_factory: Unit-of-work factory bound to the backend; ``None`` when
            the backend has no persistence, which the kernel executor accepts
            by running use cases without a unit of work.
        repo_registration_module: DI module registering model repositories.
        lifespan_init: Async context manager that opens the backend and
            releases it on the way out. It does not check the schema and it
            does not reset the compiled registry. Defaults to
            :func:`no_lifespan`.
        prepare_models: Step run on the discovered models after ``build``
            (schema compilation, identifier validation, ...); defaults to
            :func:`no_model_preparation`.
        startup_checks: Schema and role checks. When they finish, a queued
            pool holds no connection. Defaults to :func:`no_startup_checks`.
        readiness: Optional asynchronous readiness probe.
        elevation: How the store's open transaction gets elevation flags;
            ``None`` when its sessions carry none. The composition hands
            every store's to one
            :class:`~loom.core.persistence.elevation.ScopedElevationSink`.
    """

    uow_factory: UnitOfWorkFactory | None
    repo_registration_module: Callable[[LoomContainer], None]
    lifespan_init: Callable[[], AbstractAsyncContextManager[None]] = no_lifespan
    prepare_models: Callable[[Sequence[type[BaseModel]]], None] = no_model_preparation
    startup_checks: Callable[[], Awaitable[None]] = no_startup_checks
    readiness: Callable[[], Awaitable[bool]] | None = None
    elevation: StoreElevation | None = None


class PersistenceBackend(Protocol):
    """Factory a persistence backend exposes through its entry point.

    The entry point targets a class with a no-argument constructor; the
    registry instantiates it. Each backend serves the store the host names
    (``runtime.store``), taken from the catalog the host loaded once:
    ``runtime.catalog.served_by(name, runtime.store)``. The host builds one
    backend per store, so a backend registers nothing another store's
    instance would overwrite. It reads any other section from the context it
    is given.
    """

    name: ClassVar[str]
    repository_type: ClassVar[type[Any] | None]

    def capabilities(self, spec: StoreSpec) -> StoreCapabilities:
        """Describe the configured store without building its resources.

        Required: the catalog fails with a ``ConfigError`` naming a backend
        that does not implement it. Read only the settings a capability
        depends on; the catalog asks this before any connection's secrets
        need to resolve.
        """
        ...

    def build(
        self,
        ctx: ConfigContext,
        models: Sequence[type[BaseModel]],
        runtime: BackendRuntime,
    ) -> PersistenceWiring:
        """Build the persistence wiring for the discovered models.

        Args:
            ctx: Configuration context the backend reads its other sections from.
            models: Models discovered by the host.
            runtime: Defaults of the entry point building the backend, the
                optional pool-metrics receiver and the store catalog.

        Returns:
            The resolved wiring.
        """
        ...


__all__ = [
    "PersistenceBackend",
    "PersistenceWiring",
    "no_lifespan",
    "no_model_preparation",
    "no_startup_checks",
]
