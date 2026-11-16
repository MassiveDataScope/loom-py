"""Backend-neutral contract for compiling :class:`QuerySpec` parts.

Each store backend compiles :class:`~loom.core.stores.repository.query.FilterGroup`
and :class:`~loom.core.stores.repository.query.SortSpec` into its own native
expression types; the protocol fixes the entry points and the error contract
so repositories can be written against a compiler instead of a backend.
"""

from __future__ import annotations

from typing import Protocol, TypeVar

from loom.core.stores.repository.query import FilterGroup, SortSpec

FilterT_co = TypeVar("FilterT_co", covariant=True)
SortT_co = TypeVar("SortT_co", covariant=True)


class QueryCompiler(Protocol[FilterT_co, SortT_co]):
    """Compiles query parts into a backend's native expressions.

    ``FilterT_co`` is the native filter expression type (a SQLAlchemy clause,
    a Mongo filter document, ...) and ``SortT_co`` the native sort expression
    type.
    """

    def compile_filter(self, group: FilterGroup) -> FilterT_co:
        """Compile a filter group into a native filter expression.

        Args:
            group: Flat AND/OR group of field conditions.

        Returns:
            The backend's filter expression.

        Raises:
            UnsupportedQuery: If a field or a :class:`FilterOp` cannot be
                served by the backend; the reason names both the operator
                and the backend.
        """
        ...

    def compile_sort(self, sort: tuple[SortSpec, ...]) -> SortT_co:
        """Compile sort directives into a native sort expression.

        Args:
            sort: Ordered sort directives.

        Returns:
            The backend's sort expression.

        Raises:
            UnsupportedQuery: If a sort field cannot be served by the backend.
        """
        ...
