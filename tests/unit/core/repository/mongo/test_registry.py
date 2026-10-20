"""Mongo registration module: collection naming, id policy and capability bindings."""

from __future__ import annotations

from typing import Any, cast

import pytest

from loom.core.di.container import LoomContainer
from loom.core.model.introspection import scalar_column_annotations
from loom.core.repository import DefaultRepositoryBuilder, RepositoryBuildContext
from loom.core.repository.abc import (
    Creatable,
    CursorCodec,
    CursorResult,
    Listable,
    PaginationMode,
    QuerySpec,
    Readable,
    SortSpec,
)
from loom.core.repository.mongo.ids import IdPolicy, ObjectIdPolicy, Uuid4IdPolicy
from loom.core.repository.mongo.registry import (
    MongoDatabase,
    MongoDefaultRepositoryBuilder,
    build_mongo_repository_registration_module,
)
from loom.core.repository.mongo.repository import RepositoryMongo
from loom.core.repository.mongo.uow import active_session

from ._fake import FakeMongoClient
from .conftest import Article
from .test_repository import ArticleCreate, Note, NoteCreate


def _database(client: FakeMongoClient) -> MongoDatabase:
    return cast(MongoDatabase, client["demo"])


def _container(
    client: FakeMongoClient,
    *,
    collections: dict[str, str] | None = None,
    id_policy: IdPolicy | None = None,
) -> LoomContainer:
    container = LoomContainer()
    module = build_mongo_repository_registration_module(
        _database(client),
        (Article, Note),
        collections=collections or {},
        id_policy=id_policy or Uuid4IdPolicy(),
        session_provider=active_session,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )
    module(container)
    return container


def test_module_registers_default_builder() -> None:
    container = _container(FakeMongoClient())

    assert isinstance(container.resolve(DefaultRepositoryBuilder), MongoDefaultRepositoryBuilder)


def test_module_registers_capability_bindings() -> None:
    container = _container(FakeMongoClient())

    assert isinstance(container.resolve(Readable[Article]), RepositoryMongo)
    assert container.resolve_repo(Article) is not None


@pytest.mark.asyncio
async def test_model_binds_to_the_collection_named_by_tablename() -> None:
    client = FakeMongoClient()
    container = _container(client)

    await container.resolve(Creatable[Article]).create(ArticleCreate(slug="a", title="A"))

    assert "a" in client.collection("articles").documents


@pytest.mark.asyncio
async def test_collections_override_replaces_the_table_name() -> None:
    client = FakeMongoClient()
    container = _container(client, collections={"Article": "posts"})

    await container.resolve(Creatable[Article]).create(ArticleCreate(slug="a", title="A"))

    assert "a" in client.collection("posts").documents
    assert "articles" not in client.collections


@pytest.mark.asyncio
async def test_id_policy_is_applied_to_generated_keys() -> None:
    client = FakeMongoClient()
    container = _container(client, id_policy=ObjectIdPolicy())

    created = await container.resolve(Creatable[Note]).create(NoteCreate(body="hi"))

    assert ObjectIdPolicy().to_storage(created.id) in client.collection("notes").documents


def test_builder_rejects_non_persistible_types() -> None:
    builder = MongoDefaultRepositoryBuilder(
        database=_database(FakeMongoClient()),
        collections={},
        id_policy=Uuid4IdPolicy(),
        session_provider=active_session,
        cursor_codec=CursorCodec(signing="ephemeral"),
    )

    class NotAModel:
        pass

    context = RepositoryBuildContext(model=cast(Any, NotAModel))

    with pytest.raises(RuntimeError, match="NotAModel"):
        builder(context)


@pytest.mark.asyncio
async def test_the_registered_codec_verifies_the_cursors_repositories_issue() -> None:
    container = _container(FakeMongoClient())
    for slug in ("a", "b", "c"):
        await container.resolve(Creatable[Article]).create(ArticleCreate(slug=slug, title=slug))
    query = QuerySpec(sort=(SortSpec("slug"),), pagination=PaginationMode.CURSOR, limit=2)
    page = await container.resolve(Listable[Article]).list_with_query(query)
    assert isinstance(page, CursorResult)
    assert page.next_cursor is not None

    position = container.resolve(CursorCodec).decode(
        page.next_cursor,
        backend="mongo",
        model=f"{Article.__module__}.Article",
        query=query,
        scope="",
        columns=scalar_column_annotations(Article),
        primary_key=("slug",),
    )

    assert position.keys == ("b",)
