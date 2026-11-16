from __future__ import annotations

from typing import Any, Protocol, TypeVar, runtime_checkable

import msgspec

from loom.core.stores.repository.query import (
    CursorResult,
    FilterParams,
    PageParams,
    PageResult,
    QuerySpec,
)

IdT = TypeVar("IdT", contravariant=True)
OutputT = TypeVar("OutputT", bound=msgspec.Struct, covariant=True)
CreateT = TypeVar("CreateT", bound=msgspec.Struct, contravariant=True)
UpdateT = TypeVar("UpdateT", bound=msgspec.Struct, contravariant=True)


class RepositoryRead(Protocol[OutputT, IdT]):
    """Protocol for read-only repository operations (get by id and paginated listing)."""

    async def get_by_id(self, obj_id: IdT, profile: str = "default") -> OutputT | None:
        """Fetch a single entity by its primary key.

        Args:
            obj_id: Primary key of the entity.
            profile: Loading profile name for eager-load options.

        Returns:
            The entity output struct, or ``None`` if not found.
        """
        ...

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch a single entity by an arbitrary field.

        Args:
            field: Entity field name used in the equality lookup.
            value: Value to compare against.
            profile: Loading profile name for eager-load options.

        Returns:
            The entity output struct, or ``None`` if not found.
        """
        ...

    async def exists_by(self, field: str, value: Any) -> bool:
        """Check whether any entity exists matching ``field == value``."""
        ...

    async def count(self) -> int:
        """Return the total number of entities in the repository.

        Returns:
            Total row count as an integer.
        """
        ...

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        """Fetch a paginated list of entities.

        Args:
            page_params: Pagination parameters (page and limit).
            filter_params: Optional filter criteria.
            profile: Loading profile name for eager-load options.

        Returns:
            A ``PageResult`` with the matching items and pagination metadata.
        """
        ...

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[OutputT] | CursorResult[OutputT]:
        """Fetch entities using a structured
        :class:`~loom.core.stores.repository.query.QuerySpec`.

        Supports both offset and cursor pagination, structured filters, and
        explicit sort directives.  The concrete return type depends on
        ``query.pagination``:

        - ``PaginationMode.OFFSET`` → :class:`~loom.core.stores.repository.query.PageResult`
        - ``PaginationMode.CURSOR`` → :class:`~loom.core.stores.repository.query.CursorResult`

        Args:
            query: Structured query specification.
            profile: Loading profile name for eager-load options.

        Returns:
            A ``PageResult`` for offset queries or a ``CursorResult`` for
            cursor queries.
        """
        ...


class RepositoryWrite(Protocol[OutputT, CreateT, UpdateT, IdT]):
    """Protocol for write repository operations (create, update, delete)."""

    async def create(self, data: CreateT) -> OutputT:
        """Persist a new entity.

        Args:
            data: Creation payload struct.

        Returns:
            The newly created entity output struct.
        """
        ...

    async def update(self, obj_id: IdT, data: UpdateT) -> OutputT | None:
        """Apply a partial update to an existing entity.

        Args:
            obj_id: Primary key of the entity to update.
            data: Partial update payload struct.

        Returns:
            The updated entity output struct, or ``None`` if not found.
        """
        ...

    async def delete(self, obj_id: IdT) -> bool:
        """Delete an entity by its primary key.

        Args:
            obj_id: Primary key of the entity to delete.

        Returns:
            ``True`` if the entity was deleted, ``False`` if not found.
        """
        ...


@runtime_checkable
class Repository(
    RepositoryRead[OutputT, IdT],
    RepositoryWrite[OutputT, CreateT, UpdateT, IdT],
    Protocol[OutputT, CreateT, UpdateT, IdT],
):
    """Combined read-write repository protocol."""

    ...
