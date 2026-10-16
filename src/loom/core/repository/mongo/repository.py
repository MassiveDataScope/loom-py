"""MongoDB implementation of the repository contract.

The repository speaks to one ``AsyncCollection`` through pymongo's native
async client, so every driver call is awaited directly. The model's primary
key is stored as ``_id`` and mapped back on read, so a model declaring
``slug`` as its key never sees ``_id`` (FR-013). The key is minted by an
:class:`~loom.core.repository.mongo.ids.IdPolicy` when the input carries
none.

Values cross the wire in their storage form
(:mod:`loom.core.repository.mongo.values`): ``datetime`` native as aware UTC
at millisecond precision, ``date``, ``time``, ``Decimal`` and ``UUID`` as
strings restored by the output struct's annotation on read. A
``DateTime(tz=False)`` column reads back naive (UTC), so the output of
``create`` equals a later read whatever the column declares. Only the model's
column fields are written.

``pymongo`` is an optional extra (``loom-kernel[mongo]``) imported at module
level: this module is only reached through the Mongo backend, and the
persistence registry turns an ``ImportError`` at entry-point load into a
``ConfigError`` naming the extra.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any, ClassVar, Generic, Protocol, cast

import msgspec
from pymongo import ReturnDocument
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import BulkWriteError, DuplicateKeyError, PyMongoError
from pymongo.results import DeleteResult, InsertManyResult, InsertOneResult

from loom.core.config import ConfigError
from loom.core.errors import Conflict
from loom.core.logger import get_logger
from loom.core.model.convert import to_struct
from loom.core.model.enums import ServerDefault, ServerOnUpdate
from loom.core.model.introspection import get_column_fields, get_id_attributes, get_table_name
from loom.core.repository.abc import (
    BulkCreatable,
    Countable,
    Creatable,
    Cursor,
    CursorResult,
    Deletable,
    FilterGroup,
    FilterOp,
    FilterParams,
    FilterSpec,
    IdT,
    Listable,
    OutputT,
    PageParams,
    PageResult,
    PaginationMode,
    QuerySpec,
    Readable,
    SortSpec,
    Updatable,
    build_page_result,
    decode_cursor,
    encode_cursor,
)
from loom.core.repository.mongo.ids import IdPolicy, Uuid4IdPolicy
from loom.core.repository.mongo.query_compiler import MongoFilter, MongoQueryCompiler, MongoSort
from loom.core.repository.mongo.values import to_storage_datetime, to_storage_value

Document = dict[str, Any]
SessionProvider = Callable[[], AsyncClientSession | None]

_log = get_logger(__name__).bind(component="repository")

_ID = "_id"
_ID_ASCENDING: tuple[str, int] = (_ID, 1)
_DUPLICATE_KEY = 11000
# BSON encodes datetime natively; every other rich type is stored as a string.
_NATIVE_TYPES = (datetime,)


def _no_session() -> AsyncClientSession | None:
    """Default ``session_provider``: every driver call runs outside a session."""
    return None


class MongoFindCursor(Protocol):
    """The subset of ``AsyncCursor`` the repository drives."""

    def sort(self, key_or_list: list[tuple[str, int]]) -> MongoFindCursor: ...

    def skip(self, skip: int) -> MongoFindCursor: ...

    def limit(self, limit: int) -> MongoFindCursor: ...

    async def to_list(self) -> list[Document]: ...


class MongoCollection(Protocol):
    """The subset of ``AsyncCollection`` the repository drives.

    ``session`` is keyword-only so pymongo's real signatures, which place
    other optional parameters before it, satisfy the protocol structurally.
    """

    async def insert_one(
        self, document: Document, *, session: AsyncClientSession | None = None
    ) -> InsertOneResult: ...

    async def insert_many(
        self,
        documents: Iterable[Document],
        *,
        ordered: bool = True,
        session: AsyncClientSession | None = None,
    ) -> InsertManyResult: ...

    async def find_one(
        self, filter: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> Document | None: ...

    def find(
        self, filter: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> MongoFindCursor: ...

    async def count_documents(
        self, filter: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> int: ...

    async def find_one_and_update(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        return_document: bool = False,
        session: AsyncClientSession | None = None,
    ) -> Document | None: ...

    async def delete_one(
        self, filter: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> DeleteResult: ...

    async def delete_many(
        self, filter: Mapping[str, Any], *, session: AsyncClientSession | None = None
    ) -> DeleteResult: ...


class RepositoryMongo(
    Readable[OutputT],
    Creatable[OutputT],
    BulkCreatable[OutputT],
    Updatable[OutputT],
    Deletable[OutputT],
    Listable[OutputT],
    Countable[OutputT],
    Generic[OutputT, IdT],
):
    """Document repository over one MongoDB collection.

    Id rule: a primary-key value present in the input struct wins and is
    written to ``_id``; when the input carries none (field unset or
    ``None``) ``id_policy`` generates it. ``ServerDefault.NOW`` fields left
    empty on create and ``ServerOnUpdate.NOW`` fields on every update receive
    the current UTC time. A duplicate key raises
    :class:`~loom.core.errors.Conflict`.

    Beyond refusing a composite key, the repository does not judge the
    model's key declaration: a model whose key Mongo cannot mint
    (``autoincrement=True``) is rejected at boot by
    :func:`~loom.core.repository.mongo.ids.validate_id_field`, which the
    backend runs over every discovered model.

    Args:
        model: Loom model bound to the collection; its ``primary_key`` field
            maps to ``_id``.
        collection: pymongo ``AsyncCollection`` (or any object with the same
            async shape) holding the documents.
        id_policy: Strategy minting and converting ids; defaults to
            :class:`~loom.core.repository.mongo.ids.Uuid4IdPolicy`.
        session_provider: Returns the active client session, or ``None``;
            every driver call runs in the session it returns. Defaults to
            no session.

    Raises:
        ValueError: If the model declares no primary key.
        ConfigError: If the primary key is composite: ``_id`` holds one value.

    Example::

        repository = RepositoryMongo(Article, database["articles"])
        article = await repository.create(ArticleCreate(slug="hello", title="Hi"))
    """

    backend_name: ClassVar[str] = "mongo"

    def __init__(
        self,
        model: type,
        collection: MongoCollection,
        id_policy: IdPolicy | None = None,
        session_provider: SessionProvider = _no_session,
    ) -> None:
        self._model = model
        self._collection = collection
        self._ids: IdPolicy = id_policy if id_policy is not None else Uuid4IdPolicy()
        self._session = session_provider
        self._id_attr = _single_id_attribute(model)
        column_fields = get_column_fields(model)
        self._column_fields = frozenset(column_fields)
        self._naive_datetime_fields = frozenset(
            name
            for name, info in column_fields.items()
            if info.column_type.type_name == "DateTime"
            and info.column_type.kwargs.get("timezone") is False
        )
        self._now_fields = frozenset(
            name
            for name, info in column_fields.items()
            if info.field.server_default is ServerDefault.NOW
        )
        self._onupdate_fields = frozenset(
            name
            for name, info in column_fields.items()
            if ServerOnUpdate.is_now(info.field.server_onupdate)
        )
        self._compiler = MongoQueryCompiler(model, self._id_attr, self._ids.to_storage)

    def has_caller_scoped_session(self) -> bool:
        """Whether a read would run inside a session bound to the caller's context.

        True while the ``session_provider`` yields a session, which
        :class:`~loom.core.repository.mongo.uow.MongoUnitOfWork` publishes for
        the duration of its transaction: it dies when that caller unwinds and
        holds writes only that caller can see.  False with the default
        provider, and with ``transactions: false``, where every driver call
        autocommits on its own.

        Implements
        :class:`~loom.core.repository.abc.session_scope.SupportsCallerScopedSession`.
        :func:`loom.core.transaction.in_atomic_transaction` is the neutral
        marker this capability used to be a placeholder for, but it answers a
        different question — whether *any* atomic transaction is open — not
        whether *this session* is scoped to the caller, which is what governs
        coalescing safely detaching a read into its own task.  Keep using this
        method for that; it is not superseded.

        Returns:
            ``True`` when a caller-scoped session is bound to the context.
        """
        return self._session() is not None

    @property
    def model(self) -> type:
        """Loom model bound to the collection."""
        return self._model

    @property
    def entity_name(self) -> str:
        """Model table name, the cache namespace shared with every backend."""
        return get_table_name(self._model)

    async def get_by_id(self, obj_id: IdT, profile: str = "default") -> OutputT | None:
        """Fetch one entity by primary key."""
        document = await self._collection.find_one(self._key(obj_id), session=self._session())
        return None if document is None else self._to_output(document)

    async def get_by(self, field: str, value: Any, profile: str = "default") -> OutputT | None:
        """Fetch the first entity whose ``field`` equals ``value``."""
        document = await self._collection.find_one(
            self._equals(field, value), session=self._session()
        )
        return None if document is None else self._to_output(document)

    async def exists_by(self, field: str, value: Any) -> bool:
        """Return whether any entity has ``field == value``."""
        document = await self._collection.find_one(
            self._equals(field, value), session=self._session()
        )
        return document is not None

    async def count(self) -> int:
        """Return the number of documents in the collection."""
        return await self._collection.count_documents({}, session=self._session())

    async def create(self, data: msgspec.Struct) -> OutputT:
        """Insert one document and return its output struct.

        Raises:
            Conflict: If a document with the same key already exists.
        """
        document = self._new_document(data)
        try:
            await self._collection.insert_one(document, session=self._session())
        except DuplicateKeyError as exc:
            raise self._conflict(document[_ID]) from exc
        return self._to_output(document)

    async def create_many(self, data: Sequence[msgspec.Struct]) -> tuple[OutputT, ...]:
        """Insert every struct with one ordered ``insert_many``.

        A duplicate key persists nothing: the documents the ordered write
        stored before the failing one are deleted again, unless a
        transaction is active, in which case its abort discards them. When
        that compensating delete itself fails, the driver error surfaces
        instead of ``Conflict`` (with a note naming it) and the store may
        hold the partial batch. An empty ``data`` returns ``()`` without a
        round trip.

        Raises:
            Conflict: If a document with the same key already exists.
        """
        documents = [self._new_document(item) for item in data]
        if not documents:
            return ()
        session = self._session()
        try:
            await self._collection.insert_many(documents, ordered=True, session=session)
        except BulkWriteError as exc:
            index = _duplicate_index(exc)
            if index is None:
                raise
            conflict = self._conflict(documents[index][_ID])
            if session is None:
                await self._undo_inserts(documents[:index], conflict)
            raise conflict from exc
        return tuple(self._to_output(document) for document in documents)

    async def _undo_inserts(self, documents: list[Document], conflict: Conflict) -> None:
        """Delete the documents an ordered ``insert_many`` stored before it failed.

        A driver failure here is logged and re-raised with a note naming the
        conflict it was compensating: the store may hold the partial batch.
        """
        if not documents:
            return
        keys = [document[_ID] for document in documents]
        try:
            await self._collection.delete_many({_ID: {"$in": keys}})
        except PyMongoError as exc:
            _log.error("MongoBulkUndoFailed", keys=keys, conflict=str(conflict))
            exc.add_note(f"while undoing a partial create_many after: {conflict}")
            raise

    async def update(self, obj_id: IdT, data: msgspec.Struct) -> OutputT | None:
        """``$set`` the non-``None`` fields of ``data`` on the document keyed ``obj_id``.

        ``ServerOnUpdate.NOW`` fields are stamped with the current UTC time.
        The primary key is never rewritten. Returns the updated output, or
        ``None`` when no document has that key.
        """
        key = self._key(obj_id)
        session = self._session()
        changes = {
            name: value
            for name, value in self._to_internal(data).items()
            if value is not None and name != self._id_attr
        }
        changes.update(_stamp(self._onupdate_fields))
        if changes:
            document = await self._collection.find_one_and_update(
                key, {"$set": changes}, return_document=ReturnDocument.AFTER, session=session
            )
        else:
            document = await self._collection.find_one(key, session=session)
        return None if document is None else self._to_output(document)

    async def delete(self, obj_id: IdT) -> bool:
        """Delete the document keyed ``obj_id``, returning whether it existed."""
        result = await self._collection.delete_one(self._key(obj_id), session=self._session())
        return result.deleted_count > 0

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        """Fetch one offset page ordered by primary key, with the total count."""
        mongo_filter = self._compiler.compile_filter(_equality_group(filter_params))
        return await self._offset_page(mongo_filter, [_ID_ASCENDING], page_params)

    async def list_with_query(
        self, query: QuerySpec, profile: str = "default"
    ) -> PageResult[OutputT] | CursorResult[OutputT]:
        """Fetch entities matching a :class:`QuerySpec` in offset or cursor mode.

        Both modes append ``_id`` ascending to the requested sort so pages
        are deterministic, unless the sort already names the primary key.

        Raises:
            UnsupportedQuery: On an unknown field, an unsupported operator or
                a cursor token that is invalid, foreign or does not match
                the sort.
        """
        if query.pagination == PaginationMode.CURSOR:
            return await self._list_cursor(query)
        return await self._list_offset(query)

    async def _list_offset(self, query: QuerySpec) -> PageResult[OutputT]:
        return await self._offset_page(
            self._query_filter(query),
            self._sort(query.sort),
            PageParams(page=query.page, limit=query.limit),
        )

    async def _offset_page(
        self, mongo_filter: MongoFilter, sort: MongoSort, page_params: PageParams
    ) -> PageResult[OutputT]:
        session = self._session()
        documents = await (
            self._collection.find(mongo_filter, session=session)
            .sort(sort)
            .skip(page_params.offset)
            .limit(page_params.limit)
            .to_list()
        )
        total = await self._collection.count_documents(mongo_filter, session=session)
        items = [self._to_output(document) for document in documents]
        return build_page_result(items, total, page_params)

    async def _list_cursor(self, query: QuerySpec) -> CursorResult[OutputT]:
        mongo_filter = self._query_filter(query)
        if query.cursor is not None:
            cursor = self._decode(query.cursor, query.sort)
            keyset = self._compiler.compile_cursor_filter(query.sort, cursor)
            mongo_filter = {"$and": [mongo_filter, keyset]} if mongo_filter else keyset
        documents = await (
            self._collection.find(mongo_filter, session=self._session())
            .sort(self._sort(query.sort))
            .limit(query.limit + 1)
            .to_list()
        )
        has_next = len(documents) > query.limit
        page = documents[: query.limit]
        next_cursor = self._encode(page[-1], query.sort) if has_next else None
        return CursorResult(
            items=tuple(self._to_output(document) for document in page),
            next_cursor=next_cursor,
            has_next=has_next,
        )

    def _query_filter(self, query: QuerySpec) -> MongoFilter:
        if query.filters is None:
            return {}
        return self._compiler.compile_filter(query.filters)

    def _sort(self, sort: tuple[SortSpec, ...]) -> MongoSort:
        """Compiled sort plus ``_id`` ascending, unless the sort already names the key.

        pymongo folds the pairs into one document, so a second ``_id`` pair
        would silently override the requested direction.
        """
        pairs = self._compiler.compile_sort(sort)
        if any(column == _ID for column, _ in pairs):
            return pairs
        return [*pairs, _ID_ASCENDING]

    def _decode(self, token: str, sort: tuple[SortSpec, ...]) -> Cursor:
        """Decode the token and put its keys in storage form."""
        cursor = decode_cursor(
            token, self.backend_name, self._model.__qualname__, key_count=len(sort)
        )
        keys = tuple(
            self._compiler.storage_value(spec.field, key)
            for spec, key in zip(sort, cursor.keys, strict=True)
        )
        return Cursor(
            backend=cursor.backend,
            keys=keys,
            tie_breaker=self._compiler.storage_value(self._id_attr, cursor.tie_breaker),
        )

    def _encode(self, document: Document, sort: tuple[SortSpec, ...]) -> str:
        """Issue the token for the page after ``document``; ``_id`` keys use the model form."""
        keys = [self._model_value(document, spec.field) for spec in sort]
        return encode_cursor(self.backend_name, keys, self._ids.from_storage(document[_ID]))

    def _model_value(self, document: Document, field: str) -> object:
        if field == self._id_attr:
            return self._ids.from_storage(document[_ID])
        return document.get(field)

    def _key(self, obj_id: object) -> MongoFilter:
        return {_ID: {"$eq": self._compiler.storage_value(self._id_attr, obj_id)}}

    def _equals(self, field: str, value: Any) -> MongoFilter:
        return self._compiler.compile_filter(
            FilterGroup(filters=(FilterSpec(field, FilterOp.EQ, value),))
        )

    def _conflict(self, key: object) -> Conflict:
        return Conflict(
            f"{self._model.__qualname__} with {self._id_attr}="
            f"{self._ids.from_storage(key)!r} already exists."
        )

    def _new_document(self, data: msgspec.Struct) -> Document:
        """Build the document to insert: ``_id`` resolved and server defaults applied."""
        values = self._to_internal(data)
        supplied = values.pop(self._id_attr, None)
        key = self._ids.generate() if supplied is None else self._ids.to_storage(supplied)
        missing = frozenset(name for name in self._now_fields if values.get(name) is None)
        return {_ID: key, **values, **_stamp(missing)}

    def _to_internal(self, data: msgspec.Struct) -> Document:
        """Serialize a struct to its column fields, keyed by internal name, in storage form."""
        builtins = msgspec.to_builtins(data, builtin_types=_NATIVE_TYPES)
        if not isinstance(builtins, dict):
            raise TypeError("Struct payload must serialize to a dict")
        encoded_to_internal = {
            field.encode_name: field.name for field in msgspec.structs.fields(type(data))
        }
        payload = cast(dict[str, Any], builtins)
        internal = {encoded_to_internal.get(key, key): value for key, value in payload.items()}
        # ``to_builtins`` already stringified every other rich type; only ``datetime`` converts.
        return {
            name: to_storage_value(value)
            for name, value in internal.items()
            if name in self._column_fields
        }

    def _to_output(self, document: Document) -> OutputT:
        """Build the output struct: ``_id`` back to the key field, unknown keys dropped."""
        kwargs = {name: value for name, value in document.items() if name in self._column_fields}
        kwargs[self._id_attr] = self._ids.from_storage(document[_ID])
        for name in self._naive_datetime_fields:
            value = kwargs.get(name)
            if isinstance(value, datetime):
                kwargs[name] = value.replace(tzinfo=None)
        return cast(OutputT, to_struct(self._model, kwargs))


def _single_id_attribute(model: type) -> str:
    key = get_id_attributes(model)
    if len(key) == 1:
        return key[0]
    raise ConfigError(
        f"{model.__qualname__} has the composite primary key ({', '.join(key)}); "
        "Mongo stores one key value in _id, so declare a single primary key"
    )


def _equality_group(filter_params: FilterParams | None) -> FilterGroup:
    """Translate page-mode equality filters into a flat AND group."""
    if filter_params is None:
        return FilterGroup(filters=())
    return FilterGroup(
        filters=tuple(
            FilterSpec(field, FilterOp.EQ, value) for field, value in filter_params.filters.items()
        )
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _stamp(fields: frozenset[str]) -> Document:
    """Return ``fields`` mapped to one shared current UTC timestamp."""
    if not fields:
        return {}
    now = to_storage_datetime(_utc_now())
    return dict.fromkeys(fields, now)


def _duplicate_index(exc: BulkWriteError) -> int | None:
    """Return the index of the first duplicate-key write error, if any."""
    for error in exc.details.get("writeErrors", []):
        if error.get("code") == _DUPLICATE_KEY:
            return int(error["index"])
    return None


__all__ = ["MongoCollection", "MongoFindCursor", "RepositoryMongo", "SessionProvider"]
