from __future__ import annotations

from abc import abstractmethod
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, ClassVar, Generic, cast
from uuid import UUID

import msgspec
from sqlalchemy import exists, func, insert, inspect, select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.backend.sqlalchemy import get_compiled_core
from loom.core.model.convert import to_struct
from loom.core.model.introspection import (
    get_column_fields,
    get_id_attribute,
    get_relations,
    get_table_name,
)
from loom.core.repository.abc import (
    CursorResult,
    FilterParams,
    IdT,
    OutputT,
    PageParams,
    PageResult,
    PaginationMode,
    QuerySpec,
    build_page_result,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.integrity import handle_integrity_errors
from loom.core.repository.sqlalchemy.query_compiler.compiler import QuerySpecCompiler
from loom.core.repository.sqlalchemy.transactional import record_mutation

_SENTINEL = object()
# Values SQLAlchemy binds natively; encoding them to strings breaks typed columns.
_NATIVE_COLUMN_TYPES = (bytes, datetime, date, time, Decimal, UUID)
_JSON_TYPE_NAMES = frozenset({"JSON", "Postgres.JSONB"})
_TOTAL_COUNT_ALIAS = "__loom_total_count"


class SQLAlchemyContextMixin(Generic[OutputT, IdT]):
    """Shared context and helper methods for all SQLAlchemy repository mixins."""

    backend_name: ClassVar[str]
    model: type[Any]
    _sa_model: type[Any] | None = None
    _id_attr: str | None = None
    _column_field_names: frozenset[str] | None = None
    _json_column_names: frozenset[str] = frozenset()
    # Pre-computed at init to avoid per-request/per-object reflection.
    _output_column_keys: tuple[str, ...] | None = None
    _all_sa_column_keys: tuple[str, ...] | None = None
    _output_sa_columns: tuple[Any, ...] | None = None
    _core_model: Any | None = None
    _relations_cache: dict[str, Any] | None = None
    _onupdate_columns: dict[str, Any] | None = None

    def _init_struct_model(self) -> None:
        """Resolve SA model and metadata from a Struct-based model definition."""
        from loom.core.backend.sqlalchemy import get_compiled

        sa = get_compiled(self.model)
        if sa is None:
            raise RuntimeError(
                f"Model {self.model.__name__} has not been compiled. "
                f"Call compile_model() or compile_all() before creating a repository."
            )
        self._sa_model = sa
        self._id_attr = get_id_attribute(self.model)
        column_fields = get_column_fields(self.model)
        column_field_names = frozenset(column_fields.keys())
        self._column_field_names = column_field_names
        self._json_column_names = frozenset(
            name
            for name, info in column_fields.items()
            if info.column_type.type_name in _JSON_TYPE_NAMES
        )
        self._relations_cache = get_relations(self.model)
        self._core_model = get_compiled_core(self.model)
        if self._core_model is None:
            raise RuntimeError(
                f"Model {self.model.__name__} core read artifact was not compiled. "
                "Call compile_all() before creating a repository."
            )

        sa_mapper: Any = inspect(sa).mapper
        self._output_column_keys = tuple(
            col.key for col in sa_mapper.column_attrs if col.key in column_field_names
        )
        self._all_sa_column_keys = tuple(col.key for col in sa_mapper.column_attrs)
        self._output_sa_columns = tuple(getattr(sa, key) for key in self._output_column_keys)
        self._onupdate_columns = {
            col_attr.key: col.onupdate.arg
            for col_attr in sa_mapper.column_attrs
            for col in col_attr.columns
            if col.onupdate is not None
        }

    @property
    def _effective_sa_model(self) -> type[Any]:
        if self._sa_model is not None:
            return self._sa_model
        return self.model

    @property
    def _effective_id_attribute(self) -> str:
        if self._id_attr is not None:
            return self._id_attr
        return "id"

    @property
    def _effective_core_model(self) -> Any:
        if self._core_model is not None:
            return self._core_model
        raise RuntimeError("Core model metadata is not initialized")

    def create_object(self, data: msgspec.Struct) -> Any:
        """Create a model instance from a struct without persisting it."""
        return self._effective_sa_model(**self._serialize_input(data))

    @property
    def entity_name(self) -> str:
        """Return normalized entity name for mutation tracking."""
        return get_table_name(self.model)

    def _id_column(self) -> Any:
        return getattr(self._effective_sa_model, self._effective_id_attribute)

    def _serialize_input(self, data: msgspec.Struct | dict[str, Any]) -> dict[str, Any]:
        if isinstance(data, msgspec.Struct):
            builtins = msgspec.to_builtins(data, builtin_types=_NATIVE_COLUMN_TYPES)
            if not isinstance(builtins, dict):
                raise TypeError("Struct payload must serialize to dict")
            return self._encode_json_columns(_to_internal_field_names(type(data), builtins))
        return data

    def _encode_json_columns(self, values: dict[str, Any]) -> dict[str, Any]:
        """Reduce JSON-column payloads to JSON-serialisable builtins."""
        for key in self._json_column_names.intersection(values):
            values[key] = msgspec.to_builtins(values[key])
        return values

    def _column_for_field(self, field: str) -> Any:
        """Resolve a model column attribute by field name.

        Raises:
            ValueError: If ``field`` does not exist on the SQLAlchemy model.
        """
        sa_model = self._effective_sa_model
        if not hasattr(sa_model, field):
            raise ValueError(f"Unknown field '{field}' for model '{self.model.__name__}'")
        return getattr(sa_model, field)

    def _apply_offset_page_filters(
        self,
        items_stmt: Any,
        filter_params: FilterParams | None,
    ) -> Any:
        """Apply page-mode filters to an offset items statement."""
        if not filter_params or not filter_params.filters:
            return items_stmt
        return items_stmt.filter_by(**filter_params.filters)

    def _to_output(
        self,
        obj: Any,
        profile: str = "default",
        projection_values: dict[str, Any] | None = None,
    ) -> Any:
        """Convert an SA ORM object to the Struct model."""
        output_keys = self._output_column_keys
        if output_keys is not None:
            kwargs: dict[str, Any] = {key: getattr(obj, key) for key in output_keys}
        else:
            mapper = inspect(obj).mapper
            column_names = self._column_field_names or frozenset()
            kwargs = {
                col.key: getattr(obj, col.key)
                for col in mapper.column_attrs
                if col.key in column_names
            }

        relations = (
            self._relations_cache
            if self._relations_cache is not None
            else get_relations(self.model)
        )
        for rel_name, rel in relations.items():
            if profile in rel.profiles and rel_name in obj.__dict__:
                kwargs[rel_name] = self._serialize_related(getattr(obj, rel_name))

        if projection_values:
            kwargs.update(projection_values)

        return to_struct(self.model, kwargs)

    def _serialize_related(self, value: Any) -> Any:
        """Build the related model structs for loaded ORM relationship values.

        The same shape the read path assembles, so the output field's
        annotation decides the final form on both paths.
        """
        if value is None:
            return None
        if isinstance(value, list):
            return [self._serialize_related(item) for item in value]
        if hasattr(value, "__mapper__"):
            rel_mapper = inspect(value).mapper
            columns = {col.key: getattr(value, col.key) for col in rel_mapper.column_attrs}
            struct_cls = getattr(type(value), "__struct_cls__", None)
            return columns if struct_cls is None else to_struct(struct_cls, columns)
        return value

    def to_output_from_payload(self, payload: dict[str, Any]) -> Any:
        """Build output from a cached dict payload."""
        return msgspec.convert(dict(payload), type=self.model)

    def _mutation_tags(self, obj: Any) -> frozenset[str]:
        table_name = get_table_name(self.model)
        obj_id = getattr(obj, self._effective_id_attribute, None)
        tags: set[str] = {f"{table_name}:list"}
        if obj_id is not None:
            tags.add(f"{table_name}:id:{obj_id}")

        column_keys = self._all_sa_column_keys
        if column_keys is None:
            column_keys = tuple(col.key for col in inspect(obj).mapper.column_attrs)
        for key in column_keys:
            value = getattr(obj, key, None)
            if value is not None:
                tags.add(f"{table_name}:{key}:{value}")
        return frozenset(tags)

    @abstractmethod
    def _session_scope(
        self, session: AsyncSession | None = None
    ) -> AbstractAsyncContextManager[AsyncSession]:
        """Open the session the operation runs in, reusing *session* when given."""


class SQLAlchemyCreateMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Mixin providing the ``create`` operation for SQLAlchemy repositories."""

    @handle_integrity_errors
    async def create(self, data: msgspec.Struct) -> OutputT:
        """Persist one entity and return its output struct."""
        serialized = self._serialize_input(data)
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            obj = sa_model(**serialized)
            scoped_session.add(obj)
            await scoped_session.flush()
            await scoped_session.refresh(obj)

            id_attr = self._effective_id_attribute
            obj_id = getattr(obj, id_attr, None)
            record_mutation(
                MutationEvent(
                    entity=self.entity_name,
                    op="create",
                    ids=(obj_id,),
                    changed_fields=frozenset(serialized.keys()),
                    tags=self._mutation_tags(obj),
                )
            )
            return cast(OutputT, self._to_output(obj))


class SQLAlchemyBulkCreateMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Mixin providing the ``create_many`` operation for SQLAlchemy repositories."""

    @handle_integrity_errors
    async def create_many(self, data: Sequence[msgspec.Struct]) -> tuple[OutputT, ...]:
        """Persist every entity in *data* and return them in input order.

        The batch is written with one multi-row INSERT. A row that omits a
        column supplied by another row gets that column's Python-side default
        when it has one and ``NULL`` otherwise; when such a column has only a
        server default, the batch is inserted row by row instead so the server
        default applies, then re-selected in one query. Dialects with
        ``INSERT ... RETURNING`` read the rows back from the same statement,
        relying on the database returning them in ``VALUES`` order (SQLite and
        PostgreSQL do); the others flush the rows and re-select them by
        primary key. SQLite caps the bound parameters of one statement, so
        very large batches must be split by the caller. An empty *data*
        returns ``()`` without touching the database.

        Args:
            data: Command structs to persist.

        Returns:
            The output structs, one per input and in the same order.
        """
        rows = [self._insert_values(item) for item in data]
        if not rows:
            return ()
        partial = self._partial_keys(rows)
        async with self._session_scope() as scoped_session:
            if self._single_statement_applies(scoped_session, partial):
                objs = await self._insert_returning(
                    scoped_session, self._normalize_rows(rows, partial)
                )
            else:
                objs = await self._insert_and_reselect(scoped_session, rows)
            self._record_bulk_create(objs, rows)
            return tuple(cast(OutputT, self._to_output(obj)) for obj in objs)

    def _insert_values(self, item: msgspec.Struct) -> dict[str, Any]:
        """Return the non-``None`` column values of *item* keyed by internal field name."""
        return {
            key: value for key, value in self._serialize_input(item).items() if value is not None
        }

    def _partial_keys(self, rows: list[dict[str, Any]]) -> set[str]:
        """Return the keys that some rows supply and others omit."""
        key_sets = [set(row) for row in rows]
        union: set[str] = set().union(*key_sets)
        common = key_sets[0].intersection(*key_sets[1:])
        return union - common

    def _single_statement_applies(self, session: AsyncSession, partial: set[str]) -> bool:
        """True when RETURNING is available and no partial column relies on a server default."""
        columns = self._effective_sa_model.__table__.c
        server_only = any(
            columns[key].default is None and columns[key].server_default is not None
            for key in partial
        )
        return bool(session.get_bind().dialect.insert_returning) and not server_only

    def _normalize_rows(
        self, rows: list[dict[str, Any]], partial: set[str]
    ) -> list[dict[str, Any]]:
        """Add ``None`` for omitted columns that have neither a Python-side nor a server default."""
        columns = self._effective_sa_model.__table__.c
        gaps = {
            key: None
            for key in partial
            if columns[key].default is None and columns[key].server_default is None
        }
        return [{**gaps, **row} for row in rows]

    async def _insert_returning(
        self, session: AsyncSession, rows: list[dict[str, Any]]
    ) -> list[Any]:
        sa_model = self._effective_sa_model
        stmt = insert(sa_model).values(rows).returning(sa_model)
        result = await session.execute(stmt)
        return list(result.scalars().all())

    async def _insert_and_reselect(
        self, session: AsyncSession, rows: list[dict[str, Any]]
    ) -> list[Any]:
        sa_model = self._effective_sa_model
        id_attr = self._effective_id_attribute
        objs = [sa_model(**row) for row in rows]
        session.add_all(objs)
        await session.flush()
        ids = [getattr(obj, id_attr) for obj in objs]
        result = await session.execute(select(sa_model).where(self._id_column().in_(ids)))
        by_id = {getattr(obj, id_attr): obj for obj in result.scalars()}
        return [by_id[obj_id] for obj_id in ids]

    def _record_bulk_create(self, objs: list[Any], rows: list[dict[str, Any]]) -> None:
        id_attr = self._effective_id_attribute
        record_mutation(
            MutationEvent(
                entity=self.entity_name,
                op="create",
                ids=tuple(getattr(obj, id_attr, None) for obj in objs),
                changed_fields=frozenset(key for row in rows for key in row),
                tags=frozenset().union(*(self._mutation_tags(obj) for obj in objs)),
            )
        )


class SQLAlchemyReadMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Read operations for SQLAlchemy repositories.

    Class attributes:
        allowed_filter_fields: Whitelist of field names permitted in
            :meth:`list_with_query` filters. An empty frozenset allows all.
    """

    allowed_filter_fields: frozenset[str] = frozenset()

    async def get_by_id(
        self,
        obj_id: IdT,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by id with optional profile-aware loading."""
        async with self._session_scope() as scoped_session:
            core_model = self._effective_core_model
            stmt = core_model.select(profile).where(self._id_column() == obj_id).limit(1)
            loaded = await core_model.fetch_one(scoped_session, stmt, profile=profile)
            return cast(OutputT | None, loaded)

    async def get_by(
        self,
        field: str,
        value: Any,
        profile: str = "default",
    ) -> OutputT | None:
        """Fetch one entity by equality over an arbitrary column field."""
        async with self._session_scope() as scoped_session:
            core_model = self._effective_core_model
            stmt = core_model.select(profile).where(self._column_for_field(field) == value).limit(1)
            loaded = await core_model.fetch_one(scoped_session, stmt, profile=profile)
            return cast(OutputT | None, loaded)

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        """Fetch a paginated entity list and total count."""
        async with self._session_scope() as scoped_session:
            core_model = self._effective_core_model
            stmt = core_model.select(profile).order_by(self._id_column())
            stmt = self._apply_offset_page_filters(stmt, filter_params)

            paged_stmt = stmt.offset(page_params.offset).limit(page_params.limit)
            paged_stmt = paged_stmt.add_columns(func.count().over().label(_TOTAL_COUNT_ALIAS))
            loaded_items, total_count = await core_model.fetch_all_with_total(
                scoped_session,
                paged_stmt,
                profile=profile,
                total_alias=_TOTAL_COUNT_ALIAS,
            )
            items = cast(list[OutputT], loaded_items)
            if not items and page_params.page > 1:
                count_stmt = select(func.count()).select_from(self._effective_sa_model)
                count_stmt = self._apply_offset_page_filters(count_stmt, filter_params)
                total_count = await self._count_matching(scoped_session, count_stmt)
            return build_page_result(items, total_count, page_params)

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[OutputT] | CursorResult[OutputT]:
        """Fetch entities using a structured
        :class:`~loom.core.repository.abc.query.QuerySpec`.

        Supports offset and cursor pagination, structured filters, and
        explicit sort directives.

        Args:
            query: Structured query specification.
            profile: Loading profile name for eager-load options.

        Returns:
            :class:`~loom.core.repository.abc.query.PageResult` for offset
            queries, :class:`~loom.core.repository.abc.query.CursorResult`
            for cursor queries.
        """
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            id_col = self._id_column()
            compiler = QuerySpecCompiler(
                sa_model,
                id_col,
                self.allowed_filter_fields,
                backend=self.backend_name,
                model_name=self.model.__qualname__,
            )
            core_model = self._effective_core_model

            if query.pagination == PaginationMode.CURSOR:
                return await self._list_with_query_cursor(
                    scoped_session,
                    query=query,
                    profile=profile,
                    compiler=compiler,
                    core_model=core_model,
                )

            return await self._list_with_query_offset(
                scoped_session,
                query=query,
                profile=profile,
                compiler=compiler,
                core_model=core_model,
            )

    async def _list_with_query_cursor(
        self,
        scoped_session: AsyncSession,
        *,
        query: QuerySpec,
        profile: str,
        compiler: QuerySpecCompiler,
        core_model: Any,
    ) -> CursorResult[OutputT]:
        cursor_stmt = compiler.compile_cursor(query, base_stmt=core_model.select(profile))
        loaded = await core_model.fetch_all(scoped_session, cursor_stmt, profile=profile)
        items, next_cursor, has_next = compiler.paginate_cursor(cast(list[Any], loaded), query)
        return CursorResult(
            items=tuple(cast(list[OutputT], items)),
            next_cursor=next_cursor,
            has_next=has_next,
        )

    async def _list_with_query_offset(
        self,
        scoped_session: AsyncSession,
        *,
        query: QuerySpec,
        profile: str,
        compiler: QuerySpecCompiler,
        core_model: Any,
    ) -> PageResult[OutputT]:
        offset_stmt = compiler.compile_offset(
            query,
            base_stmt=core_model.select(profile),
        )
        offset_stmt = offset_stmt.add_columns(func.count().over().label(_TOTAL_COUNT_ALIAS))
        loaded_items, total_count = await core_model.fetch_all_with_total(
            scoped_session,
            offset_stmt,
            profile=profile,
            total_alias=_TOTAL_COUNT_ALIAS,
        )
        items = cast(list[OutputT], loaded_items)
        if not items and query.page > 1:
            total_count = await self._count_matching(scoped_session, compiler.compile_count(query))
        page_params = PageParams(page=query.page, limit=query.limit)
        return build_page_result(items, total_count or 0, page_params)

    async def _count_matching(self, scoped_session: AsyncSession, count_stmt: Any) -> int:
        """Count the filtered rows when a page past the end yields no windowed total."""
        result = await scoped_session.execute(count_stmt)
        return int(result.scalar() or 0)

    async def exists(self, obj_id: IdT) -> bool:
        """Check whether an entity exists by id."""
        return await self.exists_by(self._effective_id_attribute, obj_id)

    async def exists_by(self, field: str, value: Any) -> bool:
        """Check whether an entity exists by an arbitrary column field."""
        async with self._session_scope() as scoped_session:
            stmt = select(exists().where(self._column_for_field(field) == value))
            result = await scoped_session.execute(stmt)
            return bool(result.scalar())

    async def count(self) -> int:
        """Return the total number of entities in the repository.

        Returns:
            Total row count as an integer.
        """
        async with self._session_scope() as scoped_session:
            stmt = select(func.count()).select_from(self._effective_sa_model)
            result = await scoped_session.execute(stmt)
            return int(result.scalar() or 0)


class SQLAlchemyUpdateMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Mixin providing the ``update`` operation for SQLAlchemy repositories."""

    @handle_integrity_errors
    async def update(self, obj_id: IdT, data: msgspec.Struct) -> OutputT | None:
        """Apply partial updates using UPDATE RETURNING and return the updated output struct.

        Executes a single round-trip ``UPDATE ... RETURNING`` statement instead of
        a ``SELECT`` followed by a ``flush``.  Server-side ``onupdate`` expressions
        (e.g. ``updated_at = func.now()``) are included automatically.

        Args:
            obj_id: Primary key of the entity to update.
            data: Partial update payload struct.

        Returns:
            The updated entity output struct, or ``None`` if not found.
        """
        serialized = self._serialize_input(data)
        id_attr = self._effective_id_attribute
        values = {key: val for key, val in serialized.items() if key != id_attr}
        full_values = {**values, **(self._onupdate_columns or {})}

        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            if not full_values:
                raw = await scoped_session.get(sa_model, obj_id)
                return cast(OutputT, self._to_output(raw)) if raw is not None else None

            stmt = (
                sa_update(sa_model)
                .where(getattr(sa_model, id_attr) == obj_id)
                .values(**full_values)
                .returning(sa_model)
                .execution_options(synchronize_session=False)
            )
            obj = (await scoped_session.execute(stmt)).scalars().one_or_none()
            if obj is None:
                return None
            record_mutation(
                MutationEvent(
                    entity=self.entity_name,
                    op="update",
                    ids=(obj_id,),
                    changed_fields=frozenset(values.keys()),
                    tags=self._mutation_tags(obj),
                )
            )
            return cast(OutputT, self._to_output(obj))


class SQLAlchemyDeleteMixin(SQLAlchemyContextMixin[OutputT, IdT], Generic[OutputT, IdT]):
    """Mixin providing the ``delete`` operation for SQLAlchemy repositories."""

    @handle_integrity_errors
    async def delete(self, obj_id: IdT) -> bool:
        """Delete one entity by id."""
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            obj = await scoped_session.get(sa_model, obj_id)
            if obj is None:
                return False
            tags = self._mutation_tags(obj)
            await scoped_session.delete(obj)

            record_mutation(
                MutationEvent(
                    entity=self.entity_name,
                    op="delete",
                    ids=(obj_id,),
                    tags=tags,
                )
            )
            return True


def _to_internal_field_names(
    struct_type: type[msgspec.Struct],
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Map external encoded names (camelCase) to internal field names (snake_case)."""
    encoded_to_internal = {
        (field.encode_name or field.name): field.name
        for field in msgspec.structs.fields(struct_type)
    }
    return {encoded_to_internal.get(key, key): value for key, value in payload.items()}
