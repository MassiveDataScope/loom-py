from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, cast
from uuid import UUID

import pytest
from bson import ObjectId
from pymongo.errors import AutoReconnect, BulkWriteError
from pymongo.results import DeleteResult, InsertManyResult

from loom.core.errors import Conflict
from loom.core.model import BaseModel, ColumnField, DateTime, TimestampedModel
from loom.core.model.enums import ServerDefault
from loom.core.repository.abc import (
    CursorResult,
    FilterGroup,
    FilterOp,
    FilterParams,
    FilterSpec,
    PageParams,
    PageResult,
    PaginationMode,
    QuerySpec,
    SortSpec,
    UnsupportedQuery,
    encode_cursor,
)
from loom.core.repository.mongo import repository as repository_module
from loom.core.repository.mongo.ids import ObjectIdPolicy, Uuid4IdPolicy
from loom.core.repository.mongo.repository import RepositoryMongo, SessionProvider

from ._fake import FakeCollection, FakeMongoClient
from .conftest import Article

pytestmark = pytest.mark.asyncio


class ArticleCreate(BaseModel):
    slug: str
    title: str
    views: int = 0


class ArticleUpdate(BaseModel):
    title: str | None = None
    views: int | None = None
    slug: str | None = None


class ArticlePublish(BaseModel):
    slug: str
    title: str
    published_at: datetime


class ArticleWithExtra(BaseModel):
    """Input carrying a field the model has no column for."""

    slug: str
    title: str
    extra: str


class Event(BaseModel):
    """Aware datetime column (the ``DateTime`` default) plus a NOW column."""

    __tablename__ = "events"

    id: str = ColumnField(primary_key=True, server_default=ServerDefault.UUID4)
    at: datetime = ColumnField(DateTime(tz=True))
    created_at: datetime | None = ColumnField(
        server_default=ServerDefault.NOW, nullable=True, default=None
    )


class EventCreate(BaseModel):
    at: datetime


class Invoice(BaseModel):
    __tablename__ = "invoices"

    id: UUID = ColumnField(primary_key=True)
    amount: Decimal = ColumnField()
    issued_on: date = ColumnField()


class InvoiceCreate(BaseModel):
    id: UUID
    amount: Decimal
    issued_on: date


class Note(BaseModel):
    """Model whose key is minted by loom: ``ServerDefault.UUID4`` plus a NOW column."""

    __tablename__ = "notes"

    id: str = ColumnField(primary_key=True, server_default=ServerDefault.UUID4)
    body: str = ColumnField(length=64)
    created_at: datetime | None = ColumnField(
        server_default=ServerDefault.NOW, nullable=True, default=None
    )


class NoteCreate(BaseModel):
    body: str
    id: str | None = None


class Stamped(TimestampedModel):
    __tablename__ = "stamped"

    id: str = ColumnField(primary_key=True, server_default=ServerDefault.UUID4)
    body: str = ColumnField(length=64)


class StampedUpdate(BaseModel):
    body: str


class Audited(BaseModel):
    """``server_onupdate`` in its string form, as the SQLAlchemy backend also accepts."""

    __tablename__ = "audited"

    id: str = ColumnField(primary_key=True, server_default=ServerDefault.UUID4)
    body: str = ColumnField(length=64)
    touched_at: datetime | None = ColumnField(server_onupdate="NOW", nullable=True, default=None)


class Counter(BaseModel):
    __tablename__ = "counters"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    value: int = ColumnField()


class CounterCreate(BaseModel):
    value: int
    id: int | None = None


class Shade(Enum):
    DARK = "dark"


class Swatch(BaseModel):
    """A plain enum field with no declared column type: a SQL-only concern."""

    __tablename__ = "swatches"

    shade: Shade
    id: str = ColumnField(primary_key=True, server_default=ServerDefault.UUID4)


class SwatchCreate(BaseModel):
    shade: Shade


@pytest.fixture
def collection() -> FakeCollection:
    return FakeCollection()


def _articles(collection: FakeCollection) -> RepositoryMongo[Article, str]:
    return RepositoryMongo(Article, collection)


def _notes(
    collection: FakeCollection, policy: Uuid4IdPolicy | ObjectIdPolicy | None = None
) -> RepositoryMongo[Note, str]:
    return RepositoryMongo(Note, collection, id_policy=policy)


async def _seed(repo: RepositoryMongo[Article, str], rows: list[tuple[str, int]]) -> None:
    for slug, views in rows:
        await repo.create(ArticleCreate(slug=slug, title=slug.upper(), views=views))


class TestPrimaryKeyMapping:
    async def test_crud_round_trip_with_non_id_primary_key(
        self, collection: FakeCollection
    ) -> None:
        repo = _articles(collection)

        created = await repo.create(ArticleCreate(slug="hello", title="Hello", views=1))

        assert created == Article(slug="hello", title="Hello", views=1)
        assert set(collection.documents["hello"]) == {"_id", "title", "views"}
        assert await repo.get_by_id("hello") == created
        assert await repo.get_by("slug", "hello") == created
        assert await repo.get_by("title", "Hello") == created
        assert await repo.exists_by("slug", "hello") is True
        assert await repo.exists_by("views", 99) is False
        assert await repo.count() == 1

        updated = await repo.update("hello", ArticleUpdate(views=2))

        assert updated == Article(slug="hello", title="Hello", views=2)
        assert collection.documents["hello"]["_id"] == "hello"
        assert await repo.delete("hello") is True
        assert await repo.delete("hello") is False
        assert await repo.get_by_id("hello") is None

    async def test_update_absent_returns_none(self, collection: FakeCollection) -> None:
        repo = _articles(collection)

        assert await repo.update("missing", ArticleUpdate(title="x")) is None
        assert await repo.update("missing", ArticleUpdate()) is None

    async def test_update_payload_never_rewrites_the_key(self, collection: FakeCollection) -> None:
        repo = _articles(collection)
        await repo.create(ArticleCreate(slug="a", title="A", views=1))

        updated = await repo.update("a", ArticleUpdate(slug="other", title="B"))

        assert updated == Article(slug="a", title="B", views=1)
        assert set(collection.documents) == {"a"}
        assert "slug" not in collection.documents["a"]

    async def test_update_without_changes_returns_current(self, collection: FakeCollection) -> None:
        repo = _articles(collection)
        await repo.create(ArticleCreate(slug="a", title="A", views=1))

        assert await repo.update("a", ArticleUpdate()) == Article(slug="a", title="A", views=1)


class TestStorageForm:
    async def test_a_plain_enum_field_needs_no_declared_column_type(
        self, collection: FakeCollection
    ) -> None:
        repo: RepositoryMongo[Swatch, str] = RepositoryMongo(Swatch, collection)

        created = await repo.create(SwatchCreate(shade=Shade.DARK))

        assert collection.documents[created.id]["shade"] == "dark"
        assert await repo.get_by_id(created.id) == created

    async def test_only_model_columns_are_persisted(self, collection: FakeCollection) -> None:
        await _articles(collection).create(ArticleWithExtra(slug="a", title="A", extra="x"))

        assert set(collection.documents["a"]) == {"_id", "title"}

    async def test_update_persists_only_model_columns(self, collection: FakeCollection) -> None:
        repo = _articles(collection)
        await repo.create(ArticleCreate(slug="a", title="A"))

        await repo.update("a", ArticleWithExtra(slug="a", title="B", extra="x"))

        assert set(collection.documents["a"]) == {"_id", "title"}

    async def test_create_output_equals_a_later_read(
        self, collection: FakeCollection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """BSON keeps milliseconds and the client is tz-aware: the output takes that form."""
        monkeypatch.setattr(
            repository_module,
            "_utc_now",
            lambda: datetime(2026, 1, 1, 0, 0, 0, 999_999, tzinfo=UTC),
        )
        repo: RepositoryMongo[Event, str] = RepositoryMongo(Event, collection)

        created = await repo.create(
            EventCreate(at=datetime(2026, 1, 3, 12, 0, 0, 123_456, tzinfo=UTC))
        )

        assert created.at == datetime(2026, 1, 3, 12, 0, 0, 123_000, tzinfo=UTC)
        assert created.created_at == datetime(2026, 1, 1, 0, 0, 0, 999_000, tzinfo=UTC)
        assert await repo.get_by_id(created.id) == created

    async def test_naive_datetime_column_reads_back_naive_utc(
        self, collection: FakeCollection
    ) -> None:
        """``DateTime(tz=False)`` is stored as UTC and restored without an offset."""
        repo = _articles(collection)

        created = await repo.create(
            ArticlePublish(
                slug="a", title="A", published_at=datetime(2026, 1, 3, 12, 0, 0, 123_456)
            )
        )

        assert collection.documents["a"]["published_at"] == datetime(
            2026, 1, 3, 12, 0, 0, 123_000, tzinfo=UTC
        )
        assert created.published_at == datetime(2026, 1, 3, 12, 0, 0, 123_000)
        assert await repo.get_by_id("a") == created

    async def test_string_stored_types_round_trip_and_filter(
        self, collection: FakeCollection
    ) -> None:
        repo: RepositoryMongo[Invoice, UUID] = RepositoryMongo(Invoice, collection)
        ident = UUID("b7cdfa6d-0805-4c13-8071-af9317c08254")
        created = await repo.create(
            InvoiceCreate(id=ident, amount=Decimal("19.90"), issued_on=date(2026, 1, 3))
        )

        assert collection.documents[str(ident)] == {
            "_id": str(ident),
            "amount": "19.90",
            "issued_on": "2026-01-03",
        }
        assert created == Invoice(id=ident, amount=Decimal("19.90"), issued_on=date(2026, 1, 3))
        assert await repo.get_by("issued_on", date(2026, 1, 3)) == created
        assert await repo.get_by("amount", Decimal("19.90")) == created
        assert await repo.exists_by("issued_on", date(2026, 1, 4)) is False

    async def test_a_mapping_value_never_becomes_an_operator(
        self, collection: FakeCollection
    ) -> None:
        repo = _articles(collection)
        await repo.create(ArticleCreate(slug="a", title="A"))

        assert await repo.get_by_id(cast(Any, {"$gt": ""})) is None
        assert await repo.exists_by("title", {"$gt": ""}) is False


class TestIdGeneration:
    async def test_generates_uuid4_when_id_absent(self, collection: FakeCollection) -> None:
        repo = _notes(collection)

        created = await repo.create(NoteCreate(body="first"))

        assert UUID(created.id).version == 4
        assert collection.documents[created.id]["_id"] == created.id
        assert await repo.get_by_id(created.id) == created

    async def test_client_supplied_id_wins(self, collection: FakeCollection) -> None:
        repo = _notes(collection)

        created = await repo.create(NoteCreate(body="first", id="client-id"))

        assert created.id == "client-id"
        assert "client-id" in collection.documents

    async def test_server_default_now_is_applied_on_create(
        self, collection: FakeCollection
    ) -> None:
        # Stamps are stored at BSON millisecond precision, so compare at that grain.
        before = datetime.now(UTC).replace(microsecond=0)

        created = await _notes(collection).create(NoteCreate(body="first"))

        assert created.created_at is not None
        assert before <= created.created_at <= datetime.now(UTC)

    async def test_server_onupdate_now_is_applied_on_update(
        self, collection: FakeCollection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clock = iter([datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 1, 2, tzinfo=UTC)])
        monkeypatch.setattr(repository_module, "_utc_now", lambda: next(clock))
        repo: RepositoryMongo[Stamped, str] = RepositoryMongo(Stamped, collection)
        created = await repo.create(NoteCreate(body="first"))

        updated = await repo.update(created.id, StampedUpdate(body="second"))

        assert updated is not None
        assert updated.body == "second"
        assert updated.created_at == created.created_at == datetime(2026, 1, 1, tzinfo=UTC)
        assert updated.updated_at == datetime(2026, 1, 2, tzinfo=UTC)
        assert created.updated_at == datetime(2026, 1, 1, tzinfo=UTC)

    async def test_server_onupdate_string_form_is_applied_on_update(
        self, collection: FakeCollection, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(repository_module, "_utc_now", lambda: datetime(2026, 1, 2, tzinfo=UTC))
        repo: RepositoryMongo[Audited, str] = RepositoryMongo(Audited, collection)
        created = await repo.create(NoteCreate(body="first"))
        assert created.touched_at is None

        updated = await repo.update(created.id, StampedUpdate(body="second"))

        assert updated is not None
        assert updated.touched_at == datetime(2026, 1, 2, tzinfo=UTC)

    async def test_objectid_policy_yields_string_id(self, collection: FakeCollection) -> None:
        repo = _notes(collection, ObjectIdPolicy())

        created = await repo.create(NoteCreate(body="first"))

        assert isinstance(created.id, str)
        stored_key = next(iter(collection.documents))
        assert isinstance(stored_key, ObjectId)
        assert str(stored_key) == created.id
        assert await repo.get_by_id(created.id) == created
        assert await repo.get_by_id("not-an-object-id") is None
        assert await repo.delete(created.id) is True

    async def test_uuid_policy_accepts_uuid_values(self, collection: FakeCollection) -> None:
        repo = _notes(collection)
        created = await repo.create(NoteCreate(body="first"))

        assert await repo.get_by("id", UUID(created.id)) == created

    async def test_client_supplied_int_id_wins_on_an_autoincrement_model(
        self, collection: FakeCollection
    ) -> None:
        """The boot-time gate, not the repository, judges ``autoincrement``."""
        repo: RepositoryMongo[Counter, int] = RepositoryMongo(Counter, collection)

        created = await repo.create(CounterCreate(id=7, value=1))

        assert created.id == 7
        assert collection.documents[7]["value"] == 1
        assert await repo.get_by_id(7) == created


class TestConflicts:
    async def test_duplicate_create_raises_conflict(self, collection: FakeCollection) -> None:
        repo = _articles(collection)
        await repo.create(ArticleCreate(slug="a", title="A"))
        payload = ArticleCreate(slug="a", title="Again")

        with pytest.raises(Conflict, match="Article with slug='a' already exists"):
            await repo.create(payload)

    async def test_duplicate_in_bulk_raises_conflict_and_persists_nothing(
        self, collection: FakeCollection
    ) -> None:
        repo = _articles(collection)
        await repo.create(ArticleCreate(slug="b", title="B"))
        payload = [ArticleCreate(slug="a", title="A"), ArticleCreate(slug="b", title="B")]

        with pytest.raises(Conflict, match="slug='b'"):
            await repo.create_many(payload)

        assert set(collection.documents) == {"b"}

    async def test_duplicate_in_bulk_inside_a_transaction_leaves_the_undo_to_the_abort(
        self,
    ) -> None:
        class RecordingCollection(FakeCollection):
            deletes = 0

            async def delete_many(
                self, filter: Mapping[str, Any], session: object | None = None
            ) -> DeleteResult:
                self.deletes += 1
                return await super().delete_many(filter, session)

        collection = RecordingCollection()
        session = FakeMongoClient().start_session()
        provider = cast(SessionProvider, lambda: session)
        repo = RepositoryMongo(Article, collection, session_provider=provider)
        payload = [ArticleCreate(slug="a", title="A"), ArticleCreate(slug="a", title="A")]

        with pytest.raises(Conflict):
            await repo.create_many(payload)

        assert collection.deletes == 0
        assert set(collection.documents) == {"a"}

    async def test_failed_undo_surfaces_the_driver_error(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        class BrokenUndoCollection(FakeCollection):
            async def delete_many(
                self, filter: Mapping[str, Any], session: object | None = None
            ) -> DeleteResult:
                raise AutoReconnect("primary stepped down")

        collection = BrokenUndoCollection()
        repo = _articles(collection)
        payload = [ArticleCreate(slug="a", title="A"), ArticleCreate(slug="a", title="A")]

        with (
            caplog.at_level(logging.ERROR, logger=repository_module.__name__),
            pytest.raises(AutoReconnect) as info,
        ):
            await repo.create_many(payload)

        assert "Article with slug='a' already exists" in "".join(info.value.__notes__)
        assert "MongoBulkUndoFailed" in caplog.text
        assert set(collection.documents) == {"a"}

    async def test_other_bulk_write_errors_propagate(self) -> None:
        class FailingCollection(FakeCollection):
            async def insert_many(
                self,
                documents: Iterable[dict[str, Any]],
                ordered: bool = True,
                session: object | None = None,
            ) -> InsertManyResult:
                raise BulkWriteError({"writeErrors": [{"index": 0, "code": 121, "errmsg": "x"}]})

        repo = _articles(FailingCollection())
        payload = [ArticleCreate(slug="a", title="A")]

        with pytest.raises(BulkWriteError):
            await repo.create_many(payload)


class TestCreateMany:
    async def test_outputs_keep_input_order(self, collection: FakeCollection) -> None:
        repo = _articles(collection)

        created = await repo.create_many(
            [ArticleCreate(slug="z", title="Z"), ArticleCreate(slug="a", title="A")]
        )

        assert [item.slug for item in created] == ["z", "a"]
        assert await repo.count() == 2

    async def test_empty_input_makes_no_round_trip(self) -> None:
        class ExplodingCollection(FakeCollection):
            async def insert_many(
                self,
                documents: Iterable[dict[str, Any]],
                ordered: bool = True,
                session: object | None = None,
            ) -> InsertManyResult:
                raise AssertionError("insert_many must not be called")

        assert await _articles(ExplodingCollection()).create_many([]) == ()


class TestListing:
    async def test_list_paginated_orders_by_key_and_counts(
        self, collection: FakeCollection
    ) -> None:
        repo = _articles(collection)
        await _seed(repo, [("c", 1), ("a", 1), ("b", 2)])

        page = await repo.list_paginated(
            PageParams(page=1, limit=1), FilterParams(filters={"views": 1})
        )

        assert [item.slug for item in page.items] == ["a"]
        assert page.total_count == 2
        assert page.has_next is True

    async def test_list_with_query_offset_filters_and_sorts(
        self, collection: FakeCollection
    ) -> None:
        repo = _articles(collection)
        await _seed(repo, [("a", 5), ("b", 3), ("c", 9), ("d", 1)])
        query = QuerySpec(
            filters=FilterGroup(filters=(FilterSpec("views", FilterOp.GTE, 3),)),
            sort=(SortSpec("views", "DESC"),),
            limit=2,
            page=2,
        )

        page = await repo.list_with_query(query)

        assert isinstance(page, PageResult)
        assert [item.slug for item in page.items] == ["b"]
        assert page.total_count == 3
        assert page.has_next is False


class TestCursorPagination:
    @staticmethod
    def _query(cursor: str | None = None) -> QuerySpec:
        return QuerySpec(
            sort=(SortSpec("views", "DESC"),),
            pagination=PaginationMode.CURSOR,
            limit=3,
            cursor=cursor,
        )

    async def test_walk_through_duplicate_sort_values(self, collection: FakeCollection) -> None:
        repo = _articles(collection)
        # Insertion order deliberately disagrees with key order among equal views.
        await _seed(repo, [("c", 3), ("b", 3), ("a", 3), ("f", 1), ("e", 2), ("d", 2), ("g", 1)])

        walked: list[tuple[int, str]] = []
        cursor: str | None = None
        for _ in range(4):
            result = await repo.list_with_query(self._query(cursor))
            assert isinstance(result, CursorResult)
            walked.extend((item.views, item.slug) for item in result.items)
            if not result.has_next:
                assert result.next_cursor is None
                break
            cursor = result.next_cursor
            assert cursor is not None

        assert walked == [(3, "a"), (3, "b"), (3, "c"), (2, "d"), (2, "e"), (1, "f"), (1, "g")]

    async def test_cursor_is_stable_under_inserts_after_position(
        self, collection: FakeCollection
    ) -> None:
        repo = _articles(collection)
        await _seed(repo, [("a", 3), ("b", 2), ("c", 1), ("d", 1)])
        first = await repo.list_with_query(self._query())
        assert isinstance(first, CursorResult)
        assert first.next_cursor is not None
        await _seed(repo, [("z", 9), ("e", 1)])

        second = await repo.list_with_query(self._query(first.next_cursor))

        assert isinstance(second, CursorResult)
        assert [item.slug for item in first.items] == ["a", "b", "c"]
        assert [item.slug for item in second.items] == ["d", "e"]

    async def test_objectid_policy_walks_pages_sorted_by_primary_key(
        self, collection: FakeCollection
    ) -> None:
        repo = _notes(collection, ObjectIdPolicy())
        created = [await repo.create(NoteCreate(body=body)) for body in ("one", "two", "three")]
        query = QuerySpec(sort=(SortSpec("id", "ASC"),), pagination=PaginationMode.CURSOR, limit=2)

        first = await repo.list_with_query(query)
        assert isinstance(first, CursorResult)
        assert first.next_cursor is not None
        second = await repo.list_with_query(
            QuerySpec(
                sort=query.sort, pagination=PaginationMode.CURSOR, limit=2, cursor=first.next_cursor
            )
        )

        assert isinstance(second, CursorResult)
        walked = [item.id for item in (*first.items, *second.items)]
        assert walked == sorted(note.id for note in created)
        assert second.has_next is False

    async def test_objectid_policy_filters_by_primary_key(self, collection: FakeCollection) -> None:
        """Key filter values take the policy's storage form, like ``get_by_id`` does."""
        repo = _notes(collection, ObjectIdPolicy())
        first, second, _ = [
            await repo.create(NoteCreate(body=body)) for body in ("one", "two", "three")
        ]

        by_eq = await repo.list_with_query(
            QuerySpec(filters=FilterGroup(filters=(FilterSpec("id", FilterOp.EQ, first.id),)))
        )
        by_in = await repo.list_with_query(
            QuerySpec(
                filters=FilterGroup(filters=(FilterSpec("id", FilterOp.IN, (first.id, second.id)),))
            )
        )
        page = await repo.list_paginated(
            PageParams(page=1, limit=10), FilterParams(filters={"id": second.id})
        )

        assert isinstance(by_eq, PageResult)
        assert [item.id for item in by_eq.items] == [first.id]
        assert isinstance(by_in, PageResult)
        assert sorted(item.id for item in by_in.items) == sorted((first.id, second.id))
        assert [item.id for item in page.items] == [second.id]
        assert page.total_count == 1

    async def test_sort_naming_the_key_descending_walks_in_that_order(
        self, collection: FakeCollection
    ) -> None:
        """No second ``_id`` pair is appended: pymongo would let the last one win."""
        repo = _articles(collection)
        await _seed(repo, [("a", 1), ("b", 1), ("c", 1)])
        sort = (SortSpec("slug", "DESC"),)
        first = await repo.list_with_query(
            QuerySpec(sort=sort, pagination=PaginationMode.CURSOR, limit=2)
        )
        assert isinstance(first, CursorResult)
        assert first.next_cursor is not None

        second = await repo.list_with_query(
            QuerySpec(
                sort=sort, pagination=PaginationMode.CURSOR, limit=2, cursor=first.next_cursor
            )
        )

        assert isinstance(second, CursorResult)
        assert [item.slug for item in (*first.items, *second.items)] == ["c", "b", "a"]
        assert second.has_next is False

    async def test_foreign_token_is_unsupported(self, collection: FakeCollection) -> None:
        token = encode_cursor("sqlalchemy", [3], "a")
        repo = _articles(collection)
        query = self._query(token)

        with pytest.raises(UnsupportedQuery, match="another backend"):
            await repo.list_with_query(query)

    async def test_legacy_token_is_unsupported(self, collection: FakeCollection) -> None:
        repo = _articles(collection)
        query = self._query("bm90LWEtdG9rZW4=")

        with pytest.raises(UnsupportedQuery, match="not valid"):
            await repo.list_with_query(query)

    async def test_key_count_mismatch_is_unsupported(self, collection: FakeCollection) -> None:
        token = encode_cursor("mongo", [3, "x"], "a")
        repo = _articles(collection)
        query = self._query(token)

        with pytest.raises(UnsupportedQuery, match="does not match the sort"):
            await repo.list_with_query(query)


class TestSessions:
    async def test_session_provider_is_passed_to_the_driver(self) -> None:
        calls: list[object] = []

        class RecordingCollection(FakeCollection):
            async def find_one(
                self, filter: Mapping[str, Any] | None = None, session: object | None = None
            ) -> dict[str, Any] | None:
                calls.append(session)
                return await super().find_one(filter, session)

        session = FakeMongoClient().start_session()
        # The repository only forwards the session to the driver; the fake
        # session stands in for pymongo's nominal AsyncClientSession.
        provider = cast(SessionProvider, lambda: session)
        repo = RepositoryMongo(Article, RecordingCollection(), session_provider=provider)

        await repo.get_by_id("x")

        assert calls == [session]


class TestCacheHooks:
    def test_entity_name_is_the_model_table_name(self, collection: FakeCollection) -> None:
        assert _articles(collection).entity_name == "articles"
        assert _notes(collection).entity_name == "notes"
        assert _articles(collection).model is Article
