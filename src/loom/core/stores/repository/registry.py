from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from loom.core.model import LoomStruct

if TYPE_CHECKING:
    from loom.core.di.container import LoomContainer

_LOOM_REPOSITORY_ATTR = "__loom_repository__"


@dataclass(frozen=True)
class RepositoryBuildContext:
    """Context available to repository builders.

    Carries the model being built and, optionally, the DI container.  Prefer
    injecting infrastructure dependencies into your builder at construction
    time (e.g. as dataclass fields) rather than resolving them here.  Access
    ``container`` only when late resolution is unavoidable — for example,
    when the dependency itself is registered after the builder is decorated.

    Args:
        model: The Loom struct model whose repository is being built.
        container: The active DI container.  ``None`` in test harnesses that
            build repositories without a full container.
    """

    model: type[LoomStruct]
    container: LoomContainer | None = None


RepositoryBuilder = Callable[[RepositoryBuildContext], Any]


class DefaultRepositoryBuilder(Protocol):
    """Global fallback strategy for building repositories.

    Implement this protocol to swap the default persistence backend for all
    models that have no explicit ``repository_for`` registration.  The
    implementation receives infrastructure dependencies via its constructor
    (injected by the DI module), not via the context.
    """

    def __call__(self, context: RepositoryBuildContext) -> Any: ...


@dataclass(frozen=True)
class RepositoryRegistration:
    """Repository registration metadata for a logical Loom type.

    Stored directly on the model class as ``__loom_repository__`` so that
    no global mutable state is required.

    Capability DI keys (e.g. ``Readable[Product]``, custom Protocol classes)
    are auto-detected from the repository class's direct bases at registration
    time — no explicit ``contract`` parameter is needed.
    """

    model: type[LoomStruct]
    repository_type: type[Any]
    builder: RepositoryBuilder | None = None


@dataclass(frozen=True)
class RepositoryToken:
    """Internal DI token used for main-repository resolution."""

    model: type[LoomStruct]


def repository_for(
    model: type[LoomStruct],
    *,
    builder: RepositoryBuilder | None = None,
) -> Callable[[type[Any]], type[Any]]:
    """Register a repository implementation for a Loom logical type.

    Stores the registration as ``__loom_repository__`` on the model class —
    no global state is written.

    Capability DI keys are detected automatically from the repository's direct
    base classes.  Any Protocol class declared as a direct base is registered
    as an additional DI binding, alongside the standard ``-able`` capability
    generics (``Readable[Model]``, ``Creatable[Model]``, …) inferred from the
    MRO.

    Args:
        model: The Loom struct model this repository is bound to.
        builder: Optional custom factory called with a
            :class:`RepositoryBuildContext` to construct the repository.

    Raises:
        RuntimeError: If another repository is already registered for ``model``.

    Example — custom capability protocol::

        class IProductRepository(Readable[Product], Protocol):
            async def find_by_sku(self, sku: str) -> Product | None: ...

        @repository_for(Product)
        class ProductRepository(RepositorySQLAlchemy[Product, int], IProductRepository):
            async def find_by_sku(self, sku: str) -> Product | None:
                ...

        # Use case declares only the capability it needs:
        class GetProductUseCase(UseCase[Product, Product | None, IProductRepository]):
            async def execute(self, sku: str) -> Product | None:
                return await self.main_repo.find_by_sku(sku)

    Example — custom builder with DI dependencies::

        @repository_for(TaskView, builder=lambda ctx: MyRepo(ctx.container.resolve(Dep)))
        class MyTaskRepo(TaskViewRepo):
            ...
    """

    def decorator(repository_type: type[Any]) -> type[Any]:
        existing: RepositoryRegistration | None = getattr(model, _LOOM_REPOSITORY_ATTR, None)
        if existing is not None and existing.repository_type is not repository_type:
            raise RuntimeError(
                f"Repository already registered for model {model.__qualname__}: "
                f"{existing.repository_type.__qualname__}"
            )
        registration = RepositoryRegistration(
            model=model,
            repository_type=repository_type,
            builder=builder,
        )
        model.__loom_repository__ = registration  # type: ignore[attr-defined]
        return repository_type

    return decorator


def get_repository_registration(model: type[LoomStruct]) -> RepositoryRegistration | None:
    """Return the repository registration stored on ``model``, if any."""
    return getattr(model, _LOOM_REPOSITORY_ATTR, None)
