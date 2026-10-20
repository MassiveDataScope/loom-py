from __future__ import annotations

from abc import abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any, ClassVar, Generic, cast

import msgspec
from sqlalchemy import and_, exists, func, insert, inspect, select, tuple_
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.backend.sqlalchemy import get_compiled_core
from loom.core.errors.codes import ErrorCode
from loom.core.model.convert import to_struct
from loom.core.model.introspection import (
    get_column_fields,
    get_id_attributes,
    get_relations,
    get_table_name,
    scalar_column_annotations,
)
from loom.core.repository.abc import (
    CursorCodec,
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
from loom.core.repository.sqlalchemy.rls.provider import SCOPE_PREFIX
from loom.core.repository.sqlalchemy.session_settings import applied_settings
from loom.core.repository.sqlalchemy.transactional import get_active_session, record_mutation
from loom.core.repository.sqlalchemy.values import (
    check_time_offsets,
    is_json_column,
    to_column_value,
    to_comparison_value,
)

if TYPE_CHECKING:
    from loom.core.repository.sqlalchemy.session_manager import SessionManager

_SENTINEL = object()
_TOTAL_COUNT_ALIAS = "__loom_total_count"


class SQLAlchemyContextMixin(Generic[OutputT, IdT]):
    """Shared context and helper methods for all SQLAlchemy repository mixins."""

    backend_name: ClassVar[str]
    model: type[Any]
    session_manager: SessionManager
    cursor_codec: CursorCodec
    _sa_model: type[Any] | None = None
    _id_attrs: tuple[str, ...] | None = None
    _column_field_names: frozenset[str] | None = None
    _json_column_names: frozenset[str] = frozenset()
    _cursor_columns: Mapping[str, Any]
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
        self._id_attrs = get_id_attributes(self.model)
        column_fields = get_column_fields(self.model)
        column_field_names = frozenset(column_fields.keys())
        self._column_field_names = column_field_names
        self._json_column_names = frozenset(
            name for name in column_fields if is_json_column(getattr(sa, name, None))
        )
        self._cursor_columns = scalar_column_annotations(self.model)
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
    def _effective_id_attributes(self) -> tuple[str, ...]:
        if self._id_attrs is not None:
            return self._id_attrs
        return ("id",)

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

    def _id_columns(self) -> tuple[Any, ...]:
        sa_model = self._effective_sa_model
        return tuple(getattr(sa_model, name) for name in self._effective_id_attributes)

    def _id_values(self, obj_id: Any) -> tuple[Any, ...]:
        """Return the primary-key values *obj_id* names, one per key column.

        A single-column key takes the value itself; a composite key takes a
        tuple holding every key value in key order.

        Raises:
            ValueError: If the key is composite and *obj_id* is not a tuple of
                one value per key column.
        """
        names = self._effective_id_attributes
        if len(names) == 1:
            return (obj_id,)
        if isinstance(obj_id, tuple) and len(obj_id) == len(names):
            return obj_id
        raise ValueError(
            f"{self.model.__name__} has the composite primary key ({', '.join(names)}); "
            f"pass a tuple of {len(names)} values in that order, not {obj_id!r}"
        )

    def _id_clause(self, obj_id: Any) -> Any:
        """Return the WHERE clause matching the row whose primary key is *obj_id*."""
        pairs = zip(self._id_columns(), self._id_values(obj_id), strict=True)
        return and_(*(column == value for column, value in pairs))

    def _key_of(self, obj: Any) -> Any:
        """Return the primary key of *obj* in the form :meth:`_id_values` accepts."""
        values = tuple(getattr(obj, name, None) for name in self._effective_id_attributes)
        return values[0] if len(values) == 1 else values

    def _serialize_input(self, data: msgspec.Struct | dict[str, Any]) -> dict[str, Any]:
        if isinstance(data, msgspec.Struct):
            builtins = to_column_value(data)
            if not isinstance(builtins, dict):
                raise TypeError("Struct payload must serialize to dict")
            values = self._encode_json_columns(_to_internal_field_names(type(data), builtins))
            sa_model = self._effective_sa_model
            names = (self._column_field_names or frozenset()).intersection(values)
            check_time_offsets(
                ((getattr(sa_model, name), values[name]) for name in names),
                code=ErrorCode.BOUNDARY_VALIDATION,
            )
            return values
        return data

    def _encode_json_columns(self, values: dict[str, Any]) -> dict[str, Any]:
        """Reduce JSON-column payloads to JSON-serialisable builtins."""
        for key in self._json_column_names.intersection(values):
            values[key] = to_column_value(values[key], json=True)
        return values

    def _column_for_field(self, field: str) -> Any:
        """Resolve a model column attribute by field name.

        Raises:
            ValueError: If ``field`` is not a mapped column of the SQLAlchemy
                model.
        """
        sa_model = self._effective_sa_model
        if field not in inspect(sa_model).column_attrs:
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
        return items_stmt.filter_by(
            **{
                name: to_comparison_value(self._column_for_field(name), value)
                for name, value in filter_params.filters.items()
            }
        )

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
        obj_id = self._key_of(obj)
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

            record_mutation(
                MutationEvent(
                    entity=self.entity_name,
                    op="create",
                    ids=(self._key_of(obj),),
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
        objs = [sa_model(**row) for row in rows]
        session.add_all(objs)
        await session.flush()
        ids = [self._key_of(obj) for obj in objs]
        columns = self._id_columns()
        key = columns[0] if len(columns) == 1 else tuple_(*columns)
        result = await session.execute(select(sa_model).where(key.in_(ids)))
        by_id = {self._key_of(obj): obj for obj in result.scalars()}
        return [by_id[obj_id] for obj_id in ids]

    def _record_bulk_create(self, objs: list[Any], rows: list[dict[str, Any]]) -> None:
        record_mutation(
            MutationEvent(
                entity=self.entity_name,
                op="create",
                ids=tuple(self._key_of(obj) for obj in objs),
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
        """Fetch one entity by id with optional profile-aware loading.

        A composite primary key is passed as a tuple of every key value, in
        key order.

        Raises:
            ValueError: If the key is composite and *obj_id* does not hold
                every key value.
        """
        clause = self._id_clause(obj_id)
        async with self._session_scope() as scoped_session:
            core_model = self._effective_core_model
            stmt = core_model.select(profile).where(clause).limit(1)
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
            column = self._column_for_field(field)
            stmt = core_model.select(profile).where(column == to_comparison_value(column, value))
            stmt = stmt.limit(1)
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
            stmt = core_model.select(profile).order_by(*self._id_columns())
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
            compiler = QuerySpecCompiler(
                sa_model,
                self._id_columns(),
                self.allowed_filter_fields,
                backend=self.backend_name,
                model=self.model,
                cursor_codec=self.cursor_codec,
                scope=self._cursor_scope(),
                columns=self._cursor_columns,
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

    def cache_scope(self) -> str:
        """Every session setting the caller's reads run under; ``""`` when none.

        Implements :class:`~loom.core.repository.abc.session_scope.SupportsCacheScope`.
        A row-level policy may read any setting, not only ``loom.scope.*``, so
        all of them partition the cache; the ``loom.scope.*`` ones are among
        them, so this tells apart every caller the cursor tokens do.
        """
        return _encode_settings(self._scope_settings())

    def _cursor_scope(self) -> str:
        """The row-level-security scope values cursors are bound to; ``""`` when unscoped."""
        settings = self._scope_settings()
        return _encode_settings({k: v for k, v in settings.items() if k.startswith(SCOPE_PREFIX)})

    def _scope_settings(self) -> Mapping[str, str]:
        """The settings the caller's reads run under.

        Inside a caller-owned transaction that has started, the ones it
        applied, which Postgres keeps until it ends; otherwise the ones the
        next transaction would start with.
        """
        active = get_active_session()
        applied = None if active is None else applied_settings(active.sync_session)
        return self.session_manager.current_settings() if applied is None else applied

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
        """Check whether an entity exists by id; a composite key as in :meth:`get_by_id`."""
        return await self._exists_where(self._id_clause(obj_id))

    async def exists_by(self, field: str, value: Any) -> bool:
        """Check whether an entity exists by an arbitrary column field."""
        column = self._column_for_field(field)
        return await self._exists_where(column == to_comparison_value(column, value))

    async def _exists_where(self, clause: Any) -> bool:
        async with self._session_scope() as scoped_session:
            result = await scoped_session.execute(select(exists().where(clause)))
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
            obj_id: Primary key of the entity to update; a tuple of every key
                value, in key order, when the key is composite.
            data: Partial update payload struct.

        Returns:
            The updated entity output struct, or ``None`` if not found.

        Raises:
            ValueError: If the key is composite and *obj_id* does not hold
                every key value.
        """
        serialized = self._serialize_input(data)
        id_attrs = self._effective_id_attributes
        values = {key: val for key, val in serialized.items() if key not in id_attrs}
        full_values = {**values, **(self._onupdate_columns or {})}
        identity = self._id_values(obj_id)

        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            if not full_values:
                raw = await scoped_session.get(sa_model, identity)
                return cast(OutputT, self._to_output(raw)) if raw is not None else None

            stmt = (
                sa_update(sa_model)
                .where(self._id_clause(obj_id))
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
        """Delete one entity by id; a composite key is a tuple as in :meth:`update`."""
        identity = self._id_values(obj_id)
        async with self._session_scope() as scoped_session:
            sa_model = self._effective_sa_model
            obj = await scoped_session.get(sa_model, identity)
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


def _encode_settings(settings: Mapping[str, str]) -> str:
    """Encode *settings* as a canonical string, sorted JSON pairs; ``""`` when empty."""
    return msgspec.json.encode(sorted(settings.items())).decode() if settings else ""
