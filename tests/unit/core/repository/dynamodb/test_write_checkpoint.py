"""DynamoDB writes go through the invocation scope's checkpoint (ADR 0008, T096)."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, NoReturn

import pytest
from botocore.exceptions import ClientError

from loom.core.errors import Conflict
from loom.core.model import BaseModel, Field, Integer, String
from loom.core.persistence.stores import DEFAULT_STORE, StoreKey
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB
from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory
from loom.core.uow.coordinator import NonTransactional, OrderedCommits
from loom.core.uow.scope import (
    Frame,
    ScopeOwnershipError,
    StoresNotCommitted,
    UndeclaredStoreWrite,
    invocation,
    invocation_error,
)
from tests.unit.core.uow._coordinator_fakes import Factory, Ledger, PhasedStore

from .conftest import FakeClient, Product, ProductCreate, ProductUpdate

AUDIT = StoreKey("audit")

Write = Callable[[RepositoryDynamoDB[Any, int]], Awaitable[object]]


class AuditEntry(BaseModel):
    __tablename__ = "audit_entries"
    __store__ = "audit"
    id: Annotated[int, Integer, Field(primary_key=True)]
    name: Annotated[str, String] = ""


WRITES = [
    pytest.param(lambda repo: repo.create(ProductCreate(id=2)), id="create"),
    pytest.param(lambda repo: repo.update(1, ProductUpdate(name="n")), id="update"),
    pytest.param(lambda repo: repo.delete(1), id="delete"),
]


@pytest.fixture
def coordinator() -> OrderedCommits:
    return OrderedCommits(
        {DEFAULT_STORE: DynamoUnitOfWorkFactory(), AUDIT: DynamoUnitOfWorkFactory()}
    )


@pytest.mark.parametrize("write", WRITES)
async def test_a_write_to_an_undeclared_store_calls_no_client_method(
    coordinator: OrderedCommits,
    fake_client: FakeClient,
    write: Write,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for method in ("get_item", "put_item", "delete_item"):
        monkeypatch.setattr(fake_client, method, _refuse_call)
    audit: RepositoryDynamoDB[Any, int] = RepositoryDynamoDB(fake_client, "products", AuditEntry)

    async with invocation(coordinator) as scope, scope.frame(Frame(DEFAULT_STORE)):
        with pytest.raises(UndeclaredStoreWrite, match="store 'audit'"):
            await write(audit)


def _refuse_call(**kwargs: Any) -> NoReturn:
    raise AssertionError(f"the client was called with {kwargs}")


async def test_a_declared_write_is_reported_as_written(
    coordinator: OrderedCommits, fake_client: FakeClient
) -> None:
    products: RepositoryDynamoDB[Any, int] = RepositoryDynamoDB(fake_client, "products", Product)

    async with invocation(coordinator) as scope:
        async with scope.frame(Frame(DEFAULT_STORE)):
            await products.create(ProductCreate(id=1))
        outcome = await coordinator.finish(scope, None)

    assert outcome.stores == (NonTransactional(DEFAULT_STORE, wrote=True),)


@pytest.mark.parametrize(
    "write",
    [
        pytest.param(lambda repo: repo.update(9, ProductUpdate(name="n")), id="update"),
        pytest.param(lambda repo: repo.delete(9), id="delete"),
    ],
)
async def test_a_write_to_a_missing_item_is_not_reported_as_written(
    coordinator: OrderedCommits, fake_client: FakeClient, write: Write
) -> None:
    products: RepositoryDynamoDB[Any, int] = RepositoryDynamoDB(fake_client, "products", Product)

    async with invocation(coordinator) as scope:
        async with scope.frame(Frame(DEFAULT_STORE)):
            await write(products)
        outcome = await coordinator.finish(scope, None)

    assert outcome.stores == (NonTransactional(DEFAULT_STORE, wrote=False),)


async def test_a_write_from_a_gather_child_is_refused(
    coordinator: OrderedCommits, fake_client: FakeClient
) -> None:
    products: RepositoryDynamoDB[Any, int] = RepositoryDynamoDB(fake_client, "products", Product)

    async with invocation(coordinator) as scope, scope.frame(Frame(DEFAULT_STORE)):
        child = asyncio.ensure_future(products.create(ProductCreate(id=1)))
        with pytest.raises(ScopeOwnershipError):
            await child

    assert fake_client.items["products"] == {}


def _unavailable(**kwargs: Any) -> NoReturn:
    raise ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "x"}}, "PutItem")


@pytest.mark.parametrize(
    ("unavailable", "raised", "wrote"),
    [
        pytest.param(False, Conflict, False, id="condition-failed"),
        pytest.param(True, StoresNotCommitted, True, id="may-have-written"),
    ],
)
async def test_a_failed_write_beside_a_transactional_store_counts_only_if_it_may_have_landed(
    fake_client: FakeClient,
    monkeypatch: pytest.MonkeyPatch,
    unavailable: bool,
    raised: type[Exception],
    wrote: bool,
) -> None:
    audit = PhasedStore(AUDIT, Ledger())
    coordinator = OrderedCommits({DEFAULT_STORE: DynamoUnitOfWorkFactory(), AUDIT: Factory(audit)})
    products: RepositoryDynamoDB[Any, int] = RepositoryDynamoDB(fake_client, "products", Product)
    await products.create(ProductCreate(id=1))
    if unavailable:
        monkeypatch.setattr(fake_client, "put_item", _unavailable)
    again = ProductCreate(id=1)

    async with invocation(coordinator) as scope:
        async with scope.frame(Frame(DEFAULT_STORE, frozenset({AUDIT}))):
            scope.participant_for(AUDIT, audit.owner, write=True)
            with pytest.raises((Conflict, ClientError)) as failed:
                await products.create(again)
        outcome = await coordinator.finish(scope, failed.value)

    assert isinstance(invocation_error(outcome, failed.value), raised)
    assert NonTransactional(DEFAULT_STORE, wrote=wrote) in outcome.stores
