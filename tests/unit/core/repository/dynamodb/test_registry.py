from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer
from loom.core.persistence.stores import DEFAULT_STORE
from loom.core.repository import DefaultRepositoryBuilder
from loom.core.repository.abc import Creatable, Readable
from loom.core.repository.dynamodb.registry import (
    DynamoDBDefaultRepositoryBuilder,
    build_dynamodb_repository_registration_module,
)
from loom.core.repository.dynamodb.repository import RepositoryDynamoDB

from .conftest import FakeClient, Product, ProductCreate


def _container(fake_client: FakeClient) -> LoomContainer:
    container = LoomContainer()
    module = build_dynamodb_repository_registration_module(
        fake_client, "products", (Product,), store=DEFAULT_STORE
    )
    module(container)
    return container


def test_module_registers_default_builder(fake_client: FakeClient) -> None:
    container = _container(fake_client)

    builder = container.resolve(DefaultRepositoryBuilder)
    assert isinstance(builder, DynamoDBDefaultRepositoryBuilder)


def test_module_registers_capability_bindings(fake_client: FakeClient) -> None:
    container = _container(fake_client)

    repo = container.resolve(Readable[Product])
    assert isinstance(repo, RepositoryDynamoDB)
    assert container.resolve_repo(Product) is not None


@pytest.mark.asyncio
async def test_registered_repository_persists_through_fake_client(
    fake_client: FakeClient,
) -> None:
    container = _container(fake_client)

    repo = container.resolve(Creatable[Product])
    created = await repo.create(ProductCreate(id=1, name="Widget", price=2.5))
    assert created == Product(id=1, name="Widget", price=2.5)

    reader = container.resolve(Readable[Product])
    assert await reader.get_by_id(1) == Product(id=1, name="Widget", price=2.5)
