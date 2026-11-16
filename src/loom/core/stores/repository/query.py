from __future__ import annotations

from enum import StrEnum
from typing import Any, Generic, Literal, TypeVar

import msgspec

from loom.core.model import LoomFrozenStruct

OutputT = TypeVar("OutputT", bound=msgspec.Struct, covariant=True)


class PaginationMode(StrEnum):
    """Pagination strategy for list and query operations.

    Attributes:
        OFFSET: Classic page+limit pagination with a total count.  Convenient
            but optional: computing ``total_count`` and skipping to arbitrary
            offsets is not efficiently supported by every backend, so a backend
            may decline this mode.
        CURSOR: Keyset (cursor) pagination.  The portable mode — supported by
            every backend and performant at scale; requires a stable sort order
            with a tie-breaker.
    """

    OFFSET = "offset"
    CURSOR = "cursor"


class FilterOp(StrEnum):
    """Filter operator applied to a single field.

    Attributes:
        EQ: Exact equality.
        NE: Inequality.
        GT: Greater than.
        GTE: Greater than or equal.
        LT: Less than.
        LTE: Less than or equal.
        IN: Value is in a collection.
        LIKE: Text pattern match (case-sensitive).
        ILIKE: Text pattern match (case-insensitive).
        IS_NULL: Field has no value.
        EXISTS: Related collection is non-empty.
        NOT_EXISTS: Related collection is empty.
    """

    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    LIKE = "like"
    ILIKE = "ilike"
    IS_NULL = "is_null"
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"


class FilterSpec(LoomFrozenStruct, frozen=True):
    """A single field filter condition.

    Args:
        field: Dot-separated field path (e.g. ``"price"`` or
            ``"category.name"``).
        op: Comparison operator.
        value: Value to compare against.  Ignored for ``IS_NULL``,
            ``EXISTS``, and ``NOT_EXISTS``.

    Example::

        FilterSpec(field="price", op=FilterOp.GTE, value=10.0)
    """

    field: str
    op: FilterOp
    value: Any = None


class FilterGroup(LoomFrozenStruct, frozen=True):
    """A group of filter conditions combined with AND or OR logic.

    Args:
        filters: Filter conditions to combine.
        op: Logical operator: ``"AND"`` (default) or ``"OR"``.

    Example::

        FilterGroup(
            filters=(
                FilterSpec("price", FilterOp.GTE, 10.0),
                FilterSpec("price", FilterOp.LTE, 100.0),
            ),
            op="AND",
        )
    """

    filters: tuple[FilterSpec, ...]
    op: Literal["AND", "OR"] = "AND"


class SortSpec(LoomFrozenStruct, frozen=True):
    """A single sort directive applied to a query.

    Args:
        field: Field name to sort by.
        direction: ``"ASC"`` (default) or ``"DESC"``.

    Example::

        SortSpec(field="created_at", direction="DESC")
    """

    field: str
    direction: Literal["ASC", "DESC"] = "ASC"


class QuerySpec(LoomFrozenStruct, frozen=True):
    """Structured query contract for list operations.

    Replaces the flat ``FilterParams`` dict with an explicit, type-safe
    representation.  The repository implementation compiles this into
    backend-specific clauses at query time.

    Args:
        filters: Optional filter group applied to the query.
        sort: Ordered tuple of sort directives.
        pagination: Pagination strategy.  Defaults to ``OFFSET``.
        limit: Maximum number of items per page (1-1000).
        page: 1-based page number (only for ``OFFSET`` mode).
        cursor: Opaque cursor token (only for ``CURSOR`` mode).

    Example::

        QuerySpec(
            filters=FilterGroup(
                filters=(FilterSpec("price", FilterOp.GTE, 10.0),),
            ),
            sort=(SortSpec("name"),),
            pagination=PaginationMode.OFFSET,
            limit=20,
            page=1,
        )
    """

    filters: FilterGroup | None = None
    sort: tuple[SortSpec, ...] = msgspec.field(default_factory=tuple)
    pagination: PaginationMode = PaginationMode.OFFSET
    limit: int = 50
    page: int = 1
    cursor: str | None = None


class CursorResult(LoomFrozenStruct, Generic[OutputT], frozen=True, kw_only=True, rename="camel"):
    """Result of a cursor-paginated query.

    Attributes:
        items: Entities for the current page.
        next_cursor: Opaque token for the next page, or ``None`` if this
            is the last page.
        has_next: ``True`` if more items follow.
    """

    items: tuple[OutputT, ...]
    next_cursor: str | None
    has_next: bool


class PageParams(LoomFrozenStruct, frozen=True, kw_only=True):
    """Pagination parameters for list queries.

    Attributes:
        page: 1-based page number.
        limit: Maximum number of items per page (1-1000).
    """

    page: int = 1
    limit: int = 50

    def __post_init__(self) -> None:
        if self.page < 1:
            raise ValueError("page must be >= 1")
        if not (1 <= self.limit <= 1000):
            raise ValueError("limit must be in [1, 1000]")

    @property
    def offset(self) -> int:
        """Calculate the zero-based row offset for the current page."""
        return (self.page - 1) * self.limit


class FilterParams(LoomFrozenStruct, frozen=True, kw_only=True):
    """Generic filter container for list queries.

    Attributes:
        filters: Flat mapping of field name to expected value; each entry is an
            equality match combined with AND.  The backend translates it into
            its own query mechanism.
    """

    filters: dict[str, Any] = msgspec.field(default_factory=dict)


class PageResult(LoomFrozenStruct, Generic[OutputT], frozen=True, kw_only=True, rename="camel"):
    """Paginated result set returned by list queries.

    Attributes:
        items: Tuple of entity output structs for the current page.
        total_count: Total number of matching entities across all pages.
        page: Current page number (1-based).
        limit: Maximum items per page.
        has_next: ``True`` if more pages follow.
    """

    items: tuple[OutputT, ...]
    total_count: int
    page: int
    limit: int
    has_next: bool


def build_page_result(
    items: list[OutputT],
    total_count: int,
    page_params: PageParams,
) -> PageResult[OutputT]:
    """Construct a ``PageResult`` from a list of items and pagination metadata.

    Args:
        items: Entity output structs for the current page.
        total_count: Total number of matching entities across all pages.
        page_params: The pagination parameters used for this query.

    Returns:
        A populated ``PageResult`` instance.
    """
    return PageResult(
        items=tuple(items),
        total_count=total_count,
        page=page_params.page,
        limit=page_params.limit,
        has_next=(page_params.offset + len(items)) < total_count,
    )
