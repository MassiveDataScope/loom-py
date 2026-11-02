"""In-memory stand-in for pymongo's asynchronous collection and client.

Scope is deliberately narrow (plan decision 10): the calls the Mongo
repository and unit of work issue, over the operators
:class:`~loom.core.repository.mongo.query_compiler.MongoQueryCompiler` emits.
Any other query or update operator raises :class:`NotImplementedError` naming
it, so a drift between compiler and fake is visible instead of silently
matching nothing.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from pymongo.errors import BulkWriteError, DuplicateKeyError
from pymongo.results import DeleteResult, InsertManyResult, InsertOneResult

_DUPLICATE_KEY = 11000
_Document = dict[str, Any]


def _ordered(value: Any, other: Any, verdict: Callable[[Any, Any], bool]) -> bool:
    """Mongo type bracketing: an ordering comparison never matches ``null``."""
    return value is not None and verdict(value, other)


_FIELD_OPS: dict[str, Callable[[Any, Any], bool]] = {
    "$eq": lambda value, other: value == other,
    "$ne": lambda value, other: value != other,
    "$gt": lambda value, other: _ordered(value, other, lambda a, b: a > b),
    "$gte": lambda value, other: _ordered(value, other, lambda a, b: a >= b),
    "$lt": lambda value, other: _ordered(value, other, lambda a, b: a < b),
    "$lte": lambda value, other: _ordered(value, other, lambda a, b: a <= b),
    "$in": lambda value, other: value in other,
}


def _regex_matches(value: Any, condition: Mapping[str, Any]) -> bool:
    flags = re.IGNORECASE if "i" in condition.get("$options", "") else 0
    return isinstance(value, str) and re.search(condition["$regex"], value, flags) is not None


def _field_matches(value: Any, condition: Any) -> bool:
    if not isinstance(condition, Mapping):
        return value == condition
    for operator in condition:
        if operator == "$options":
            continue
        if operator == "$regex":
            if not _regex_matches(value, condition):
                return False
            continue
        verdict = _FIELD_OPS.get(operator)
        if verdict is None:
            raise NotImplementedError(
                f"Fake collection does not evaluate field operator {operator!r}"
            )
        if not verdict(value, condition[operator]):
            return False
    return True


def _matches(document: _Document, query: Mapping[str, Any]) -> bool:
    for key, condition in query.items():
        if key == "$and":
            if not all(_matches(document, clause) for clause in condition):
                return False
        elif key == "$or":
            if not any(_matches(document, clause) for clause in condition):
                return False
        elif key.startswith("$"):
            raise NotImplementedError(f"Fake collection does not evaluate operator {key!r}")
        elif not _field_matches(document.get(key), condition):
            return False
    return True


def _sort_key(field: str, document: _Document) -> tuple[bool, Any]:
    """Order ``null`` before every other value, as MongoDB does."""
    value = document.get(field)
    return (value is not None, value)


class FakeCursor:
    """Result of :meth:`FakeCollection.find`; evaluated lazily by :meth:`to_list`."""

    def __init__(self, documents: list[_Document]) -> None:
        self._documents = documents
        self._sort: list[tuple[str, int]] = []
        self._skip = 0
        self._limit = 0

    def sort(self, key_or_list: list[tuple[str, int]]) -> FakeCursor:
        """Record the sort; a repeated key is refused.

        pymongo folds the pairs into one document, where the last direction
        for a key silently wins; the fake makes that a hard error.
        """
        fields = [field for field, _ in key_or_list]
        duplicates = {field for field in fields if fields.count(field) > 1}
        if duplicates:
            raise ValueError(f"duplicate sort key: {', '.join(sorted(duplicates))}")
        self._sort = list(key_or_list)
        return self

    def skip(self, skip: int) -> FakeCursor:
        self._skip = skip
        return self

    def limit(self, limit: int) -> FakeCursor:
        self._limit = limit
        return self

    async def to_list(self) -> list[_Document]:
        documents = list(self._documents)
        for field, direction in reversed(self._sort):
            documents.sort(key=lambda doc: _sort_key(field, doc), reverse=direction == -1)
        documents = documents[self._skip :]
        if self._limit:
            documents = documents[: self._limit]
        return [dict(doc) for doc in documents]


class FakeCollection:
    """In-memory stand-in for :class:`pymongo.asynchronous.collection.AsyncCollection`.

    Documents are keyed by ``_id``; inserting a duplicate raises a real
    :class:`pymongo.errors.DuplicateKeyError` (``insert_one``) or
    :class:`pymongo.errors.BulkWriteError` (``insert_many``, ordered: earlier
    documents stay inserted), exactly as the driver does. ``session``
    arguments are accepted and ignored.
    """

    def __init__(self) -> None:
        self.documents: dict[Any, _Document] = {}
        self.last_session: object | None = None

    async def insert_one(
        self, document: _Document, session: object | None = None
    ) -> InsertOneResult:
        self.last_session = session
        return InsertOneResult(self._store(document), acknowledged=True)

    async def insert_many(
        self, documents: Iterable[_Document], ordered: bool = True, session: object | None = None
    ) -> InsertManyResult:
        self.last_session = session
        if not ordered:
            raise NotImplementedError("fake insert_many supports ordered inserts only")
        inserted: list[Any] = []
        for index, document in enumerate(documents):
            try:
                inserted.append(self._store(document))
            except DuplicateKeyError as exc:
                raise self._bulk_error(index, exc, inserted) from exc
        return InsertManyResult(inserted, acknowledged=True)

    async def find_one(
        self, filter: Mapping[str, Any] | None = None, session: object | None = None
    ) -> _Document | None:
        self.last_session = session
        found = await self.find(filter, session).limit(1).to_list()
        return found[0] if found else None

    def find(
        self, filter: Mapping[str, Any] | None = None, session: object | None = None
    ) -> FakeCursor:
        self.last_session = session
        return FakeCursor(self._select(filter or {}))

    async def count_documents(
        self, filter: Mapping[str, Any], session: object | None = None
    ) -> int:
        self.last_session = session
        return len(self._select(filter))

    async def find_one_and_update(
        self,
        filter: Mapping[str, Any],
        update: Mapping[str, Any],
        *,
        return_document: bool = False,
        session: object | None = None,
    ) -> _Document | None:
        """Apply ``$set`` to the first match and return it as it is afterwards."""
        self.last_session = session
        unknown = [operator for operator in update if operator != "$set"]
        if unknown:
            raise NotImplementedError(
                f"Fake collection does not apply update operator {unknown[0]!r}"
            )
        if not return_document:
            raise NotImplementedError("Fake collection returns the document after the update")
        matched = self._select(filter)[:1]
        if not matched:
            return None
        matched[0].update(update.get("$set", {}))
        return dict(matched[0])

    async def delete_one(
        self, filter: Mapping[str, Any], session: object | None = None
    ) -> DeleteResult:
        return await self._delete(self._select(filter)[:1], session)

    async def delete_many(
        self, filter: Mapping[str, Any], session: object | None = None
    ) -> DeleteResult:
        return await self._delete(self._select(filter), session)

    async def _delete(self, matched: list[_Document], session: object | None) -> DeleteResult:
        self.last_session = session
        for document in matched:
            del self.documents[document["_id"]]
        return DeleteResult({"n": len(matched)}, acknowledged=True)

    def _select(self, query: Mapping[str, Any]) -> list[_Document]:
        return [doc for doc in self.documents.values() if _matches(doc, query)]

    def _store(self, document: _Document) -> Any:
        key = document["_id"]
        if key in self.documents:
            raise DuplicateKeyError(f"E11000 duplicate key error: _id {key!r}", code=_DUPLICATE_KEY)
        self.documents[key] = document
        return key

    @staticmethod
    def _bulk_error(index: int, exc: DuplicateKeyError, inserted: list[Any]) -> BulkWriteError:
        write_error = {"index": index, "code": _DUPLICATE_KEY, "errmsg": str(exc)}
        return BulkWriteError({"nInserted": len(inserted), "writeErrors": [write_error]})


class FakeSession:
    """Stand-in for :class:`pymongo.asynchronous.client_session.AsyncClientSession`.

    Supports the explicit ``start_transaction`` / ``commit_transaction`` /
    ``abort_transaction`` / ``end_session`` calls the unit of work issues.
    """

    def __init__(self, client: FakeMongoClient) -> None:
        self._client = client
        self.in_transaction = False
        self.ended = False
        self._snapshot: dict[str, dict[Any, _Document]] = {}

    async def start_transaction(self) -> None:
        if self._client.start_transaction_error is not None:
            raise self._client.start_transaction_error
        self.in_transaction = True
        self._snapshot = {
            name: dict(collection.documents)
            for name, collection in self._client.collections.items()
        }

    async def commit_transaction(self) -> None:
        if self._client.commit_error is not None:
            raise self._client.commit_error
        self._client.committed += 1
        self.in_transaction = False

    async def abort_transaction(self) -> None:
        """Abort, restoring every collection to its state at ``start_transaction``."""
        if self._client.abort_error is not None:
            raise self._client.abort_error
        if self._client.abort_gate is not None:
            await self._client.abort_gate.wait()
        for name, collection in self._client.collections.items():
            collection.documents = dict(self._snapshot.get(name, {}))
        self._client.aborted += 1
        self.in_transaction = False

    async def end_session(self) -> None:
        self.ended = True
        if self._client.end_error is not None:
            raise self._client.end_error


class FakeTopology:
    """The topology description of an unconnected client: its seeds, unknown."""

    def server_descriptions(self) -> dict[tuple[str, int], None]:
        return {("mongo-b", 27018): None, ("mongo-a", 27017): None}


class FakeAdmin:
    """The ``admin`` database: answers ``ping`` unless the client is told to fail."""

    def __init__(self, client: FakeMongoClient) -> None:
        self._client = client

    async def command(self, command: str) -> dict[str, Any]:
        if self._client.ping_error is not None:
            raise self._client.ping_error
        return {"ok": 1.0}


class FakeDatabase:
    """Named database of a :class:`FakeMongoClient`; collections are shared per client."""

    def __init__(self, client: FakeMongoClient, name: str) -> None:
        self._client = client
        self.name = name

    def __getitem__(self, name: str) -> FakeCollection:
        return self._client.collection(name)


class FakeMongoClient:
    """Stand-in for :class:`pymongo.asynchronous.mongo_client.AsyncMongoClient`.

    Mirrors the driver's shapes: ``start_session()`` is synchronous,
    ``start_transaction()`` is awaited, ``client[database][collection]``
    addresses a collection and ``admin.command("ping")`` answers the
    readiness probe. Commits and aborts are counted; an abort restores the
    documents present when the transaction started.
    """

    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}
        self.sessions: list[FakeSession] = []
        self.committed = 0
        self.aborted = 0
        self.closed = False
        self.ping_error: Exception | None = None
        self.start_transaction_error: Exception | None = None
        self.abort_error: Exception | None = None
        self.abort_gate: asyncio.Event | None = None
        self.commit_error: Exception | None = None
        self.end_error: Exception | None = None

    @property
    def admin(self) -> FakeAdmin:
        return FakeAdmin(self)

    @property
    def topology_description(self) -> FakeTopology:
        return FakeTopology()

    def __getitem__(self, name: str) -> FakeDatabase:
        return FakeDatabase(self, name)

    def collection(self, name: str) -> FakeCollection:
        return self.collections.setdefault(name, FakeCollection())

    def start_session(self) -> FakeSession:
        session = FakeSession(self)
        self.sessions.append(session)
        return session

    async def close(self) -> None:
        self.closed = True
