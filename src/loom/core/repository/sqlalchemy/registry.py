from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.repository import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
)
from loom.core.repository import (
    build_repository_registration_module as build_main_repository_module,
)
from loom.core.repository.abc import CursorCodec
from loom.core.repository.registry import (
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
    repository_for,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

__all__ = [
    "RepositoryRegistration",
    "RepositoryToken",
    "SQLAlchemyDefaultRepositoryBuilder",
    "build_sqlalchemy_repository_registration_module",
    "get_repository_registration",
    "repository_for",
]


@dataclass(frozen=True)
class SQLAlchemyDefaultRepositoryBuilder:
    """Default repository builder for SQLAlchemy-backed models.

    A frozen dataclass that receives a ``SessionManager`` and a
    ``CursorCodec`` at construction time — injected by the SQLAlchemy DI
    module.  The bootstrap and any other infrastructure layer must not
    construct this class directly; register it via the DI module so that
    both singletons are shared across all repositories.

    Args:
        session_manager: Shared SQLAlchemy session manager.
        cursor_codec: Shared codec that issues and verifies cursor tokens.
    """

    session_manager: SessionManager
    cursor_codec: CursorCodec

    def __call__(self, context: RepositoryBuildContext) -> Any:
        if not issubclass(context.model, BaseModel):
            raise RuntimeError(
                f"SQLAlchemyDefaultRepositoryBuilder cannot build a repository "
                f"for non-persistible type {context.model.__qualname__}"
            )
        return RepositorySQLAlchemy(
            session_manager=self.session_manager,
            model=context.model,
            cursor_codec=self.cursor_codec,
        )


def build_sqlalchemy_repository_registration_module(
    session_manager: SessionManager,
    models: Sequence[type[BaseModel]],
    *,
    cursor_codec: CursorCodec,
    logical_models: Sequence[type[Any]] = (),
) -> Callable[[LoomContainer], None]:
    """Build a DI module that registers model repositories and their capability bindings.

    The module self-declares its infrastructure dependencies: it registers
    ``SessionManager``, ``CursorCodec`` and ``DefaultRepositoryBuilder`` in the
    container so that the bootstrap does not need to know about SQLAlchemy
    internals, and a service paginating by hand resolves the codec the
    repositories sign with. A custom ``builder`` builds its repository with
    that codec too: resolve ``CursorCodec`` from ``context.container``.

    To swap the default builder, register your own ``DefaultRepositoryBuilder``
    in the container *before* loading this module — the module will not
    overwrite an existing registration.
    """

    def _registered_builder(
        context: RepositoryBuildContext,
        registration: RepositoryRegistration,
    ) -> Any:
        if registration.builder is not None:
            return registration.builder(context)
        repository_type = registration.repository_type
        if not isinstance(repository_type, type):
            raise RuntimeError(
                "Repository registration for "
                f"{registration.model.__qualname__} must use a class type."
            )
        if issubclass(repository_type, RepositorySQLAlchemy):
            if not issubclass(context.model, BaseModel):
                raise RuntimeError(
                    f"{repository_type.__qualname__} requires BaseModel-compatible registrations."
                )
            return repository_type(
                session_manager=session_manager,
                model=context.model,
                cursor_codec=cursor_codec,
            )
        return repository_type()

    register = build_main_repository_module(
        models=models,
        explicit_models=logical_models,
        build_registered_repository=_registered_builder,
        default_repository_type=RepositorySQLAlchemy,
    )

    def _register_with_session(container: LoomContainer) -> None:
        if not container.is_registered(SessionManager):
            container.register_instance(SessionManager, session_manager)
        if not container.is_registered(CursorCodec):
            container.register_instance(CursorCodec, cursor_codec)
        if not container.is_registered(DefaultRepositoryBuilder):
            container.register_instance(
                DefaultRepositoryBuilder,
                SQLAlchemyDefaultRepositoryBuilder(
                    session_manager=container.resolve(SessionManager),
                    cursor_codec=container.resolve(CursorCodec),
                ),
            )
        register(container)

    return _register_with_session
