"""Persistence backend plugin contract.

A backend is resolved by name from the ``loom.persistence.backends`` entry
point group and asked once, at bootstrap, for everything the host needs to
wire persistence: the unit-of-work factory, the DI module registering
repositories, the startup lifespan, the default repository type the
capability gate inspects, and a model preparation step.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, ClassVar, Protocol

from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.uow.abc import UnitOfWorkFactory


def no_model_preparation(models: Sequence[type[BaseModel]]) -> None:
    """Prepare nothing: the default for a backend whose models need no compilation.

    Args:
        models: Discovered models; ignored.
    """


@asynccontextmanager
async def no_lifespan() -> AsyncIterator[None]:
    """Hold no startup resource: the default lifespan of a backend without one."""
    yield


@dataclass(frozen=True)
class PersistenceWiring:
    """Everything a host needs from a persistence backend, resolved once at bootstrap.

    Args:
        uow_factory: Unit-of-work factory bound to the backend; ``None`` when
            the backend has no persistence, which the kernel executor accepts
            by running use cases without a unit of work.
        repo_registration_module: DI module registering model repositories.
        default_repository_type: Repository class whose capabilities decide
            which auto-CRUD operations a model supports; ``None`` when the
            backend serves no repositories.
        lifespan_init: Async context manager driving backend startup and
            shutdown (schema creation, resource disposal, ...); defaults to
            :func:`no_lifespan`.
        prepare_models: Step run on the discovered models after ``build``
            (schema compilation, identifier validation, ...); defaults to
            :func:`no_model_preparation`.
        readiness: Optional asynchronous readiness probe.
    """

    uow_factory: UnitOfWorkFactory | None
    repo_registration_module: Callable[[LoomContainer], None]
    default_repository_type: type[Any] | None
    lifespan_init: Callable[[], AbstractAsyncContextManager[None]] = no_lifespan
    prepare_models: Callable[[Sequence[type[BaseModel]]], None] = no_model_preparation
    readiness: Callable[[], Awaitable[bool]] | None = None


class PersistenceBackend(Protocol):
    """Factory a persistence backend exposes through its entry point.

    The entry point targets a class with a no-argument constructor; the
    registry instantiates it. Each backend reads its own configuration
    section from the context it is given.
    """

    name: ClassVar[str]

    def build(self, ctx: ConfigContext, models: Sequence[type[BaseModel]]) -> PersistenceWiring:
        """Build the persistence wiring for the discovered models.

        Args:
            ctx: Configuration context the backend reads its section from.
            models: Models discovered by the host.

        Returns:
            The resolved wiring.
        """
        ...


__all__ = ["PersistenceBackend", "PersistenceWiring", "no_lifespan", "no_model_preparation"]
