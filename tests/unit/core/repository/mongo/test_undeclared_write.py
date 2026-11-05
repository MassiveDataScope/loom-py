"""A Mongo write to a store the running execution did not declare never reaches the driver (T046).

Two Mongo stores run in one invocation. The audit repository's collection
records every call made on it: a refused write must leave it untouched,
whether or not the audit store rolls back.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast

import pytest
from pymongo import AsyncMongoClient

from loom.core.command import Command
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.abc import CursorCodec
from loom.core.repository.mongo.repository import MongoCollection, RepositoryMongo
from loom.core.repository.mongo.uow import MongoSessions, MongoUnitOfWorkFactory
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import Frame, UndeclaredStoreWrite, invocation
from loom.core.use_case import UseCase

from ._fake import FakeCollection, FakeMongoClient
from .conftest import Article

pytestmark = pytest.mark.timeout(30)

AUDIT = StoreKey("audit")


class _RecordedCollection:
    """Delegates to a fake collection and records each method called on it."""

    def __init__(self, inner: FakeCollection) -> None:
        self._inner = inner
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Callable[..., Any]:
        method = getattr(self._inner, name)

        def recorded(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            return method(*args, **kwargs)

        return recorded


class _NewArticle(Command, frozen=True):
    slug: str
    title: str
    views: int


class _ArticleViews(Command, frozen=True):
    views: int


ArticleWrite = Callable[[RepositoryMongo[Article, str]], Awaitable[object]]

_WRITES: dict[str, ArticleWrite] = {
    "create": lambda repo: repo.create(_NewArticle(slug="b", title="B", views=0)),
    "create_many": lambda repo: repo.create_many([_NewArticle(slug="b", title="B", views=0)]),
    "update": lambda repo: repo.update("a", _ArticleViews(views=2)),
    "delete": lambda repo: repo.delete("a"),
}


def _client(fake: FakeMongoClient) -> AsyncMongoClient[Any]:
    return cast(AsyncMongoClient[Any], fake)


class _Stores:
    """A transactional default store and an audit store, its articles seeded with ``a``."""

    def __init__(self, audit_rolls_back: bool) -> None:
        self.default = FakeMongoClient()
        self.audit = FakeMongoClient()
        self.audit_rolls_back = audit_rolls_back
        inner = self.audit.collection("articles")
        inner.documents["a"] = {"_id": "a", "title": "A", "views": 1, "published_at": None}
        self.collection = _RecordedCollection(inner)
        self.articles = RepositoryMongo(
            Article,
            cast(MongoCollection, self.collection),
            session_provider=MongoSessions(_client(self.audit), AUDIT),
            cursor_codec=CursorCodec(signing="ephemeral"),
        )

    def coordinator(self) -> OrderedCommits:
        audit = (
            MongoUnitOfWorkFactory.transactional(_client(self.audit), AUDIT)
            if self.audit_rolls_back
            else MongoUnitOfWorkFactory.without_transactions()
        )
        default = MongoUnitOfWorkFactory.transactional(_client(self.default), DEFAULT_STORE)
        return OrderedCommits({DEFAULT_STORE: default, AUDIT: audit})


@pytest.fixture(params=[True, False], ids=["transactional-audit", "audit-without-transactions"])
def stores(request: pytest.FixtureRequest) -> _Stores:
    return _Stores(audit_rolls_back=request.param)


@pytest.mark.parametrize("operation", sorted(_WRITES))
async def test_an_undeclared_write_is_refused_before_the_driver(
    stores: _Stores, operation: str
) -> None:
    async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
        with pytest.raises(UndeclaredStoreWrite, match="store 'audit'"):
            await _WRITES[operation](stores.articles)

    assert stores.collection.calls == []


@pytest.mark.parametrize("operation", sorted(_WRITES))
async def test_a_joined_write_reaches_the_driver(stores: _Stores, operation: str) -> None:
    joined = Frame(DEFAULT_STORE, frozenset({AUDIT}))

    async with invocation(stores.coordinator()) as scope, scope.frame(joined):
        await _WRITES[operation](stores.articles)

    assert stores.collection.calls != []


async def test_a_read_of_an_undeclared_store_is_not_refused(stores: _Stores) -> None:
    async with invocation(stores.coordinator()) as scope, scope.frame(Frame(DEFAULT_STORE)):
        found = await stores.articles.get_by_id("a")

    assert found is not None
    assert found.title == "A"


class _Write(UseCase[Article, None]):
    def __init__(self, articles: RepositoryMongo[Article, str], write: ArticleWrite) -> None:
        self.articles = articles
        self.write = write

    async def execute(self) -> None:
        await self.write(self.articles)


@pytest.mark.parametrize("operation", sorted(_WRITES))
async def test_a_top_level_read_only_execution_may_not_write(
    stores: _Stores, operation: str
) -> None:
    executor = RuntimeExecutor(UseCaseCompiler(), coordinator=stores.coordinator())
    use_case = _Write(stores.articles, _WRITES[operation])

    with pytest.raises(UndeclaredStoreWrite, match="is read-only"):
        await executor.execute(use_case, read_only=True)

    assert stores.collection.calls == []
