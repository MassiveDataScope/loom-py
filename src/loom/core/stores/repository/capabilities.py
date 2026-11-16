"""Repository capability protocols for use-case dependency injection.

Each protocol represents an independent repository capability.  Use cases
declare only the capabilities they need as their third generic parameter
(``UseCase[TModel, TResult, TCapability]``).  Infrastructure repositories
declare which capabilities they implement via explicit inheritance so that
``@repository_for`` can auto-register them in the DI container.

``RepoFor`` remains as a full-surface composition for use cases that need
the complete standard CRUD interface.
"""

from __future__ import annotations

from collections.abc import Sequence
from enum import StrEnum
from typing import Any, Protocol, TypeVar

import msgspec

from loom.core.stores.repository.query import (
    CursorResult,
    FilterParams,
    PageParams,
    PageResult,
    QuerySpec,
)

ModelT = TypeVar("ModelT", bound=msgspec.Struct, covariant=True)


class Readable(Protocol[ModelT]):
    """Repository capability: fetch individual entities.

    Implement this to expose single-entity read operations.

    Example::

        class IProductRepository(Readable[Product], Protocol):
            async def find_by_sku(self, sku: str) -> Product | None: ...
    """

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> ModelT | None:
        """Fetch one entity by primary key."""
        ...

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> ModelT | None:
        """Fetch one entity by arbitrary field."""
        ...


class Creatable(Protocol[ModelT]):
    """Repository capability: persist new entities.

    Example::

        class CreateOrderUseCase(UseCase[Order, Order, Creatable[Order]]):
            async def execute(self, cmd: CreateOrderCommand = Input()) -> Order:
                return await self.main_repo.create(cmd)
    """

    async def create(self, data: msgspec.Struct) -> ModelT:
        """Persist one entity and return the persisted result."""
        ...


class OnConflict(StrEnum):
    """What ``create_many`` does with a row whose primary key already exists."""

    ERROR = "error"
    IGNORE = "ignore"


class BulkCreatable(Protocol[ModelT]):
    """Repository capability: persist several new entities in one call."""

    async def create_many(
        self, data: Sequence[msgspec.Struct], *, on_conflict: OnConflict = OnConflict.ERROR
    ) -> tuple[ModelT, ...]:
        """Persist the entities in *data*.

        The batch may run as several statements in the same transaction.

        Args:
            data: Command structs to persist.
            on_conflict: ``ERROR`` persists every row; a primary key that
                already exists raises ``Conflict`` and nothing is persisted,
                up to the backend's transaction limit (DynamoDB: 100 items
                per chunk, earlier chunks stay written). ``IGNORE`` skips the
                rows whose primary key already exists.

        Returns:
            With ``ERROR``, every persisted entity in input order. With
            ``IGNORE``, only the inserted entities, in input order; its length
            is the number of rows inserted.

        Raises:
            Conflict: With ``ERROR``, on a primary key that already exists;
                with either mode, on any other constraint violation.
        """
        ...


class Updatable(Protocol[ModelT]):
    """Repository capability: mutate existing entities."""

    async def update(self, obj_id: Any, data: msgspec.Struct) -> ModelT | None:
        """Update one entity by primary key."""
        ...


class Deletable(Protocol[ModelT]):
    """Repository capability: remove entities."""

    async def delete(self, obj_id: Any) -> bool:
        """Delete one entity by primary key.

        Returns:
            ``True`` if the entity existed and was deleted.
        """
        ...


class Listable(Protocol[ModelT]):
    """Repository capability: paginated and structured listing."""

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[ModelT]:
        """Fetch entities with offset pagination."""
        ...

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[ModelT] | CursorResult[ModelT]:
        """Fetch entities using a structured query (offset or cursor mode)."""
        ...


class Countable(Protocol[ModelT]):
    """Repository capability: existence checks and counting."""

    async def exists_by(self, field: str, value: Any) -> bool:
        """Return ``True`` if any entity matches ``field == value``."""
        ...

    async def count(self) -> int:
        """Return the total number of entities."""
        ...


class RepoFor(
    Readable[ModelT],
    Creatable[ModelT],
    BulkCreatable[ModelT],
    Updatable[ModelT],
    Deletable[ModelT],
    Listable[ModelT],
    Countable[ModelT],
    Protocol[ModelT],
):
    """Full-surface repository protocol for standard CRUD use cases.

    Composes all capability protocols.  Use this as the default ``RepoT``
    when a use case needs the complete CRUD surface.  For use cases that
    only need a subset of operations, prefer the specific capability protocol
    (``Readable``, ``Creatable``, etc.) to respect the Interface Segregation
    Principle.

    Example::

        # Full surface — no third param needed, RepoFor[Any] is the default
        class CreateProductUseCase(UseCase[Product, Product]):
            async def execute(self, cmd: CreateProductCommand = Input()) -> Product:
                return await self.main_repo.create(cmd)

        # Specific capability — only what the use case needs
        class GetProductUseCase(UseCase[Product, Product | None, Readable[Product]]):
            async def execute(self, product_id: int) -> Product | None:
                return await self.main_repo.get_by_id(product_id)
    """
