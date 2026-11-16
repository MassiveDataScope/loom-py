from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from pymongo import ReturnDocument
from pymongo.errors import BulkWriteError, DuplicateKeyError

from loom.core.repository.mongo.query_compiler import MongoQueryCompiler
from loom.core.stores.repository import Cursor, FilterGroup, FilterOp, FilterSpec, SortSpec

from ._fake import FakeCollection

# Stored form: the repository writes aware UTC datetimes.
_DAY = datetime(2026, 1, 1, tzinfo=UTC)
_DOCS: tuple[dict[str, Any], ...] = (
    {"_id": "a", "title": "Loom intro", "views": 10, "published_at": _DAY},
    {"_id": "b", "title": "loom deep dive", "views": 20, "published_at": None},
    {"_id": "c", "title": "Other", "views": 20, "published_at": _DAY},
    {"_id": "d", "title": "Draft_1", "views": 5, "published_at": None},
    {"_id": "e", "title": "Draft.1", "views": 30, "published_at": _DAY},
)


def _single(field: str, op: FilterOp, value: object = None) -> FilterGroup:
    return FilterGroup(filters=(FilterSpec(field, op, value),))


@pytest.fixture
async def collection() -> FakeCollection:
    fake = FakeCollection()
    await fake.insert_many([dict(doc) for doc in _DOCS])
    return fake


async def _ids(collection: FakeCollection, query: dict[str, Any]) -> list[str]:
    return [doc["_id"] for doc in await collection.find(query).sort([("_id", 1)]).to_list()]


class TestFilterEvaluation:
    @pytest.mark.parametrize(
        ("group", "expected"),
        [
            (_single("slug", FilterOp.EQ, "c"), ["c"]),
            (_single("title", FilterOp.EQ, {"$gt": ""}), []),
            (_single("views", FilterOp.NE, 20), ["a", "d", "e"]),
            (_single("published_at", FilterOp.NE, "x"), ["a", "c", "e"]),
            (_single("views", FilterOp.GT, 20), ["e"]),
            (_single("views", FilterOp.GTE, 20), ["b", "c", "e"]),
            (_single("views", FilterOp.LT, 10), ["d"]),
            (_single("views", FilterOp.LTE, 10), ["a", "d"]),
            (_single("slug", FilterOp.IN, ("a", "e", "zz")), ["a", "e"]),
            (_single("title", FilterOp.LIKE, "Draft_1"), ["d", "e"]),
            (_single("title", FilterOp.LIKE, "Draft.1"), ["e"]),
            (_single("title", FilterOp.LIKE, "%oom%"), ["a", "b"]),
            (_single("title", FilterOp.LIKE, "loom%"), ["b"]),
            (_single("title", FilterOp.ILIKE, "loom%"), ["a", "b"]),
            (_single("published_at", FilterOp.IS_NULL), ["b", "d"]),
            (_single("published_at", FilterOp.GT, datetime(2025, 12, 31)), ["a", "c", "e"]),
        ],
        ids=lambda value: value if isinstance(value, list) else "",
    )
    async def test_compiled_filter_selects_expected_documents(
        self,
        collection: FakeCollection,
        compiler: MongoQueryCompiler,
        group: FilterGroup,
        expected: list[str],
    ) -> None:
        assert await _ids(collection, compiler.compile_filter(group)) == expected

    async def test_or_group(self, collection: FakeCollection, compiler: MongoQueryCompiler) -> None:
        group = FilterGroup(
            filters=(FilterSpec("views", FilterOp.LT, 10), FilterSpec("slug", FilterOp.EQ, "e")),
            op="OR",
        )

        assert await _ids(collection, compiler.compile_filter(group)) == ["d", "e"]

    async def test_empty_filter_selects_everything(self, collection: FakeCollection) -> None:
        assert await _ids(collection, {}) == ["a", "b", "c", "d", "e"]

    async def test_unknown_field_operator_is_visible(self, collection: FakeCollection) -> None:
        with pytest.raises(NotImplementedError, match=r"\$exists"):
            await collection.count_documents({"title": {"$exists": True}})

    async def test_unknown_top_level_operator_is_visible(self, collection: FakeCollection) -> None:
        with pytest.raises(NotImplementedError, match=r"\$nor"):
            await collection.count_documents({"$nor": [{"title": "x"}]})


class TestCursorAndPaging:
    async def test_sort_skip_limit(
        self, collection: FakeCollection, compiler: MongoQueryCompiler
    ) -> None:
        sort = compiler.compile_sort((SortSpec("views", "DESC"), SortSpec("slug")))

        page = await collection.find({}).sort(sort).skip(1).limit(2).to_list()

        assert [doc["_id"] for doc in page] == ["b", "c"]

    async def test_nulls_sort_first_ascending(self, collection: FakeCollection) -> None:
        docs = await collection.find({}).sort([("published_at", 1), ("_id", 1)]).to_list()

        assert [doc["_id"] for doc in docs] == ["b", "d", "a", "c", "e"]

    async def test_to_list_copies_the_documents(self, collection: FakeCollection) -> None:
        docs = await collection.find({}).sort([("views", -1)]).limit(1).to_list()
        docs[0]["views"] = -1

        assert [doc["_id"] for doc in docs] == ["e"]
        assert collection.documents["e"]["views"] == 30

    async def test_duplicate_sort_key_is_rejected(self, collection: FakeCollection) -> None:
        """pymongo folds the pairs into one document, where the last direction wins."""
        query = collection.find({})

        with pytest.raises(ValueError, match="_id"):
            query.sort([("_id", -1), ("_id", 1)])

    async def test_keyset_walk_visits_every_document_once(
        self, collection: FakeCollection, compiler: MongoQueryCompiler
    ) -> None:
        sort_spec = (SortSpec("views", "DESC"),)
        sort = compiler.compile_sort(sort_spec) + [("_id", 1)]
        seen: list[str] = []
        cursor: Cursor | None = None
        for _ in range(10):
            query = {} if cursor is None else compiler.compile_cursor_filter(sort_spec, cursor)
            page = await collection.find(query).sort(sort).limit(2).to_list()
            if not page:
                break
            seen.extend(doc["_id"] for doc in page)
            cursor = Cursor(backend="mongo", keys=(page[-1]["views"],), tie_breaker=page[-1]["_id"])

        assert seen == ["e", "b", "c", "a", "d"]


class TestWrites:
    async def test_find_one_and_count(
        self, collection: FakeCollection, compiler: MongoQueryCompiler
    ) -> None:
        query = compiler.compile_filter(_single("views", FilterOp.EQ, 20))

        found = await collection.find_one(query)
        assert found is not None
        assert found["_id"] == "b"
        assert await collection.count_documents(query) == 2
        assert await collection.find_one({"_id": "zz"}) is None

    async def test_find_one_and_update_returns_the_document_after(
        self, collection: FakeCollection
    ) -> None:
        after = await collection.find_one_and_update(
            {"views": 20}, {"$set": {"views": 21}}, return_document=ReturnDocument.AFTER
        )
        missing = await collection.find_one_and_update(
            {"_id": "zz"}, {"$set": {"views": 21}}, return_document=ReturnDocument.AFTER
        )

        assert after is not None
        assert (after["_id"], after["views"]) == ("b", 21)
        assert missing is None
        assert collection.documents["b"]["views"] == 21
        assert collection.documents["c"]["views"] == 20

    async def test_find_one_and_update_rejects_other_operators(
        self, collection: FakeCollection
    ) -> None:
        with pytest.raises(NotImplementedError, match=r"\$inc"):
            await collection.find_one_and_update(
                {"_id": "a"}, {"$inc": {"views": 1}}, return_document=ReturnDocument.AFTER
            )

    async def test_delete_one(self, collection: FakeCollection) -> None:
        result = await collection.delete_one({"_id": "a"})
        missing = await collection.delete_one({"_id": "a"})

        assert (result.deleted_count, missing.deleted_count) == (1, 0)
        assert "a" not in collection.documents

    async def test_insert_one_rejects_duplicates(self) -> None:
        collection = FakeCollection()
        inserted = await collection.insert_one({"_id": "a"})

        assert inserted.inserted_id == "a"
        with pytest.raises(DuplicateKeyError):
            await collection.insert_one({"_id": "a"})

    async def test_insert_many_ordered_keeps_earlier_documents_and_reports_the_index(
        self,
    ) -> None:
        collection = FakeCollection()

        with pytest.raises(BulkWriteError) as info:
            await collection.insert_many([{"_id": 1}, {"_id": 2}, {"_id": 1}, {"_id": 3}])

        assert sorted(collection.documents) == [1, 2]
        assert info.value.details["writeErrors"][0]["index"] == 2
        assert info.value.details["writeErrors"][0]["code"] == 11000
