"""QuerySpecCompiler — orchestrates filter, ordering, and pagination compilation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.repository.abc.cursor import CursorCodec
from loom.core.repository.abc.query import (
    FilterGroup,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.repository.sqlalchemy.query_compiler.cursor import (
    compile_cursor_predicate,
    extract_next_cursor,
)
from loom.core.repository.sqlalchemy.query_compiler.filters import compile_filter_group
from loom.core.repository.sqlalchemy.query_compiler.ordering import compile_order_by


class QuerySpecCompiler:
    """Compiles a :class:`~loom.core.repository.abc.query.QuerySpec` into
    SQLAlchemy statements and executes them.

    Designed to be instantiated once per repository mixin call — it holds
    no mutable state across invocations.

    Args:
        sa_model: SQLAlchemy mapped model class for the target entity.
        id_column: Primary-key column attribute, or the tuple of a composite
            key's column attributes in key order; appended to every ordering
            as the tie-breaker.
        allowed_fields: Permitted filter field names.  Empty frozenset
            allows all fields.
        backend: Backend name stamped on and required from cursor tokens.
        model_name: Qualified model name reported in query errors and bound
            into cursor tokens.
        cursor_codec: Codec that issues and verifies cursor tokens.
        scope: Caller scope cursor tokens are bound to; ``""`` deliberately
            binds them to no scope.
        columns: Annotation of each column a cursor may sort by, by name,
            the primary key included; a decoded cursor value must have its
            column's type.

    Example::

        compiler = QuerySpecCompiler(
            ProductSA,
            ProductSA.id,
            frozenset(),
            backend="sqlalchemy",
            model_name="Product",
            cursor_codec=codec,
            scope="",
            columns=scalar_column_annotations(Product),
        )
        result = await compiler.execute(session, query, profile_options)
    """

    def __init__(
        self,
        sa_model: type[Any],
        id_column: Any,
        allowed_fields: frozenset[str],
        *,
        backend: str,
        model_name: str,
        cursor_codec: CursorCodec,
        scope: str,
        columns: Mapping[str, Any],
    ) -> None:
        self._sa_model = sa_model
        self._id_columns: tuple[Any, ...] = (
            id_column if isinstance(id_column, tuple) else (id_column,)
        )
        self._allowed_fields = allowed_fields
        self._backend = backend
        self._model_name = model_name
        self._cursor_codec = cursor_codec
        self._scope = scope
        self._columns = columns

    def compile_filter(self, group: FilterGroup) -> Any:
        """Compile a filter group into a SQLAlchemy boolean clause.

        Args:
            group: Flat AND/OR group of field conditions.

        Returns:
            SQLAlchemy clause element honouring ``allowed_fields``.

        Raises:
            UnsafeFilterError: If a field is not in ``allowed_fields``.
            FilterPathError: If a field path cannot be resolved.
        """
        return compile_filter_group(self._sa_model, group, self._allowed_fields)

    def compile_sort(self, sort: tuple[SortSpec, ...]) -> list[Any]:
        """Compile sort directives into SQLAlchemy ORDER BY clauses.

        Args:
            sort: Ordered sort directives.

        Returns:
            Column expressions with direction applied; the tie-breaker is
            not appended here.

        Raises:
            FilterPathError: If a sort field cannot be resolved.
        """
        return compile_order_by(self._sa_model, sort)

    def compile_offset(self, query: QuerySpec, *, base_stmt: Any | None = None) -> Any:
        """Compile offset pagination statement."""
        sa_model = self._sa_model
        stmt = base_stmt if base_stmt is not None else select(sa_model)

        if query.filters:
            clause = compile_filter_group(sa_model, query.filters, self._allowed_fields)
            stmt = stmt.where(clause)

        order_clauses = compile_order_by(sa_model, query.sort)
        stmt = stmt.order_by(*order_clauses, *self._id_columns)

        offset = (query.page - 1) * query.limit
        stmt = stmt.offset(offset).limit(query.limit)

        return stmt

    def compile_count(self, query: QuerySpec) -> Any:
        """Compile a ``SELECT count(*)`` over the rows ``query.filters`` selects."""
        sa_model = self._sa_model
        stmt = select(func.count()).select_from(sa_model)
        if query.filters:
            clause = compile_filter_group(sa_model, query.filters, self._allowed_fields)
            stmt = stmt.where(clause)
        return stmt

    def compile_cursor(self, query: QuerySpec, *, base_stmt: Any | None = None) -> Any:
        """Compile the cursor pagination statement (N+1 applied).

        Rows are ordered by every sort key with every primary-key column
        appended as the tie-breaker, so the keyset predicate is total.

        Raises:
            UnsupportedQuery: If ``query.cursor`` is malformed or forged, was
                issued for another binding or backend, or does not match
                ``query.sort`` or the primary key in number or type of values.
        """
        sa_model = self._sa_model
        stmt = base_stmt if base_stmt is not None else select(sa_model)

        if query.filters:
            clause = compile_filter_group(sa_model, query.filters, self._allowed_fields)
            stmt = stmt.where(clause)

        if query.cursor:
            cursor = self._cursor_codec.decode(
                query.cursor,
                backend=self._backend,
                model=self._model_name,
                query=query,
                scope=self._scope,
                columns=self._columns,
                primary_key=[column.key for column in self._id_columns],
            )
            predicate = compile_cursor_predicate(sa_model, cursor, query.sort, self._id_columns)
            stmt = stmt.where(predicate)

        order_clauses = compile_order_by(sa_model, query.sort)
        stmt = stmt.order_by(*order_clauses, *self._id_columns)
        return stmt.limit(query.limit + 1)

    def paginate_cursor(
        self, items: list[Any], query: QuerySpec
    ) -> tuple[list[Any], str | None, bool]:
        """Truncate ``limit + 1`` rows to a page and issue the next token."""
        page_items, position, has_next = extract_next_cursor(
            items,
            query.sort,
            [column.key for column in self._id_columns],
            query.limit,
            self._backend,
        )
        if position is None:
            return page_items, None, has_next
        token = self._cursor_codec.encode(
            position, model=self._model_name, query=query, scope=self._scope
        )
        return page_items, token, has_next

    async def execute(
        self,
        session: AsyncSession,
        query: QuerySpec,
        profile_options: list[Any],
    ) -> tuple[list[Any], int | None, str | None, bool]:
        """Execute the query and return raw results for the mixin to convert.

        Args:
            session: Active async SQLAlchemy session.
            query: Structured query specification.
            profile_options: SQLAlchemy loader options for the active profile.

        Returns:
            Tuple of ``(orm_objects, total_count, next_cursor, has_next)``.
            ``total_count`` is ``None`` for cursor mode.
            ``next_cursor`` is ``None`` for offset mode.
        """
        if query.pagination == PaginationMode.CURSOR:
            return await self._execute_cursor(session, query, profile_options)
        return await self._execute_offset(session, query, profile_options)

    # ------------------------------------------------------------------
    # Offset pagination
    # ------------------------------------------------------------------

    async def _execute_offset(
        self,
        session: AsyncSession,
        query: QuerySpec,
        profile_options: list[Any],
    ) -> tuple[list[Any], int, None, bool]:
        stmt = self.compile_offset(query).add_columns(
            func.count().over().label("__loom_total_count")
        )
        if profile_options:
            stmt = stmt.options(*profile_options)

        result = await session.execute(stmt)
        rows = list(result.mappings().all())
        total_count = int(rows[0]["__loom_total_count"] or 0) if rows else 0
        items = [row[self._sa_model] for row in rows]
        offset = (query.page - 1) * query.limit
        has_next = (offset + len(items)) < total_count

        return items, total_count, None, has_next

    # ------------------------------------------------------------------
    # Cursor pagination
    # ------------------------------------------------------------------

    async def _execute_cursor(
        self,
        session: AsyncSession,
        query: QuerySpec,
        profile_options: list[Any],
    ) -> tuple[list[Any], None, str | None, bool]:
        stmt = self.compile_cursor(query)
        if profile_options:
            stmt = stmt.options(*profile_options)

        result = await session.execute(stmt)
        raw_items = list(result.scalars().all())

        page_items, next_cursor, has_next = self.paginate_cursor(raw_items, query)
        return page_items, None, next_cursor, has_next
