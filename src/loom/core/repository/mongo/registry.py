"""DI module registering Mongo repositories for the discovered models.

Mirrors :mod:`loom.core.repository.dynamodb.registry`: every model is bound
to a collection of one shared database. The collection is named by the
model's ``__tablename__``, the same source the SQL backends use for the
table, unless ``persistence.mongo.collections`` maps the model's class name
to another one.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from loom.core.di.container import LoomContainer
from loom.core.model import BaseModel
from loom.core.model.introspection import get_table_name
from loom.core.repository import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
)
from loom.core.repository import (
    build_repository_registration_module as build_main_repository_module,
)
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.ids import IdPolicy
from loom.core.repository.mongo.repository import (
    MongoCollection,
    RepositoryMongo,
    SessionProvider,
)
from loom.core.repository.registry import RepositoryRegistration


class MongoDatabase(Protocol):
    """The subset of ``AsyncDatabase`` the registration module drives."""

    def __getitem__(self, name: str) -> MongoCollection: ...


def collection_name(model: type, collections: Mapping[str, str]) -> str:
    """Return the collection ``model`` is bound to.

    Args:
        model: Loom model bound to a collection.
        collections: Per-model overrides keyed by class name.

    Returns:
        The override for ``model.__name__`` when present, else its ``__tablename__``.
    """
    return collections.get(model.__name__, get_table_name(model))


@dataclass(frozen=True)
class MongoDefaultRepositoryBuilder:
    """Default repository builder for Mongo-backed models.

    Register it via the DI module rather than constructing it directly so
    every repository shares the database, id policy, session provider and
    cursor codec.

    Args:
        database: Shared database the collections are taken from.
        collections: Per-model collection overrides keyed by class name.
        id_policy: Strategy minting and converting primary keys.
        session_provider: Returns the active client session, or ``None``.
        cursor_codec: Shared codec that issues and verifies cursor tokens.
    """

    database: MongoDatabase
    collections: Mapping[str, str]
    id_policy: IdPolicy
    session_provider: SessionProvider
    cursor_codec: CursorCodec

    def __call__(self, context: RepositoryBuildContext) -> Any:
        if not issubclass(context.model, BaseModel):
            raise RuntimeError(
                "MongoDefaultRepositoryBuilder cannot build a repository "
                f"for non-persistible type {context.model.__qualname__}"
            )
        return self.build(RepositoryMongo, context.model)

    def build(self, repository_type: type[RepositoryMongo[Any, Any]], model: type) -> Any:
        """Instantiate ``repository_type`` over the collection bound to ``model``."""
        return repository_type(
            model,
            self.database[collection_name(model, self.collections)],
            id_policy=self.id_policy,
            session_provider=self.session_provider,
            cursor_codec=self.cursor_codec,
        )


def build_mongo_repository_registration_module(
    database: MongoDatabase,
    models: Sequence[type[BaseModel]],
    *,
    collections: Mapping[str, str],
    id_policy: IdPolicy,
    session_provider: SessionProvider,
    cursor_codec: CursorCodec,
    logical_models: Sequence[type[Any]] = (),
) -> Callable[[LoomContainer], None]:
    """Build a DI module that registers Mongo repositories and capability bindings.

    The module registers a ``DefaultRepositoryBuilder`` bound to the shared
    database unless one is already registered, so a deployment may swap the
    builder by registering its own before loading the module. It registers
    ``cursor_codec`` as ``CursorCodec`` too, unless one is already
    registered, so a custom builder or a service paginating by hand
    resolves the codec the repositories sign with.

    Args:
        database: Shared database the collections are taken from.
        models: Persistible models to register repositories for.
        collections: Per-model collection overrides keyed by class name.
        id_policy: Strategy minting and converting primary keys.
        session_provider: Returns the active client session, or ``None``.
        cursor_codec: Codec that issues and verifies cursor tokens.
        logical_models: Extra models with explicit ``repository_for`` registrations.
    """
    builder = MongoDefaultRepositoryBuilder(
        database=database,
        collections=collections,
        id_policy=id_policy,
        session_provider=session_provider,
        cursor_codec=cursor_codec,
    )

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
        if issubclass(repository_type, RepositoryMongo):
            return builder.build(repository_type, context.model)
        return repository_type()

    register = build_main_repository_module(
        models=models,
        explicit_models=logical_models,
        build_registered_repository=_registered_builder,
        default_repository_type=RepositoryMongo,
    )

    def _register_with_database(container: LoomContainer) -> None:
        if not container.is_registered(CursorCodec):
            container.register_instance(CursorCodec, cursor_codec)
        if not container.is_registered(DefaultRepositoryBuilder):
            container.register_instance(DefaultRepositoryBuilder, builder)
        register(container)

    return _register_with_database


__all__ = [
    "MongoDatabase",
    "MongoDefaultRepositoryBuilder",
    "build_mongo_repository_registration_module",
    "collection_name",
]
