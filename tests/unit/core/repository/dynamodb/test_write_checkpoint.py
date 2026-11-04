"""DynamoDB writes go through the invocation scope's checkpoint (ADR 0008, T096)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Annotated, Any

import pytest

from loom.core.model import BaseModel, Field, Integer, String
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory
from loom.core.uow.coordinator import NonTransactional, OrderedCommits
from loom.core.uow.scope import Frame, ScopeOwnershipError, UndeclaredStoreWrite, invocation

from .conftest import FakeClient, Product, ProductCreate, ProductUpdate

AUDIT = StoreKey("audit")

Write = Callable[[RepositoryDynamoDB[Any, int]], Awaitable[object]]


class AuditEntry(BaseModel):
    __tablename__ = "audit_entries"
    __store__ = "audit"
    id: Annotated[int, Integer, Field(primary_key=True)]
    name: Annotated[str, String] = ""


WRITES = [
    pytest.param(lambda repo: repo.create(ProductCreate(id=1)), id="create"),
    pytest.param(lambda repo: repo.update(1, ProductUpdate(name="n")), id="update"),
    pytest.param(lambda repo: repo.delete(1), id="delete"),
]


@pytest.fixture
def coordinator() -> OrderedCommits:
    return OrderedCommits(
        {DEFAULT_STORE: DynamoUnitOfWorkFactory(), AUDIT: DynamoUnitOfWorkFactory()}
    )


@pytest.mark.parametrize("write", WRITES)
async def test_a_write_to_an_undeclared_store_sends_nothing(
    coordinator: OrderedCommits, fake_client: FakeClient, write: Write
) -> None:
    audit: RepositoryDynamoDB[Any, int] = RepositoryDynamoDB(fake_client, "products", AuditEntry)

    async with invocation(coordinator) as scope, scope.frame(Frame(DEFAULT_STORE)):
        with pytest.raises(UndeclaredStoreWrite, match="store 'audit'"):
            await write(audit)

    assert fake_client.items["products"] == {}


async def test_a_declared_write_is_reported_as_written(
    coordinator: OrderedCommits, fake_client: FakeClient
) -> None:
    products: RepositoryDynamoDB[Any, int] = RepositoryDynamoDB(fake_client, "products", Product)

    async with invocation(coordinator) as scope:
        async with scope.frame(Frame(DEFAULT_STORE)):
            await products.create(ProductCreate(id=1))
        outcome = await coordinator.finish(scope, None)

    assert outcome.stores == (NonTransactional(DEFAULT_STORE, wrote=True),)


async def test_a_write_from_a_gather_child_is_refused(
    coordinator: OrderedCommits, fake_client: FakeClient
) -> None:
    products: RepositoryDynamoDB[Any, int] = RepositoryDynamoDB(fake_client, "products", Product)

    async with invocation(coordinator) as scope, scope.frame(Frame(DEFAULT_STORE)):
        child = asyncio.ensure_future(products.create(ProductCreate(id=1)))
        with pytest.raises(ScopeOwnershipError):
            await child

    assert fake_client.items["products"] == {}
