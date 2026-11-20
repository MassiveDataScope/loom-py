from __future__ import annotations

from typing import Any

import pytest

from loom.core.di.container import LoomContainer
from loom.core.stores.catalog import DEFAULT_STORE
from loom.core.stores.repository import Creatable, DefaultRepositoryBuilder, Readable
from loom.stores.dynamodb.registry import (
    DynamoDBDefaultRepositoryBuilder,
    build_dynamodb_repository_registration_module,
)
from loom.stores.dynamodb.repository import RepositoryDynamoDB
from tests.helpers.dynamodb import MotoTable

from .conftest import Product, ProductCreate


def _container(dynamodb: Any) -> LoomContainer:
    container = LoomContainer()
    module = build_dynamodb_repository_registration_module(
        dynamodb, "products", (Product,), store=DEFAULT_STORE
    )
    module(container)
    return container


class TestDynamoRegistry:
    def test_module_registers_default_builder(self, products_table: MotoTable) -> None:
        container = _container(products_table.client)

        builder = container.resolve(DefaultRepositoryBuilder)
        assert isinstance(builder, DynamoDBDefaultRepositoryBuilder)

    def test_module_registers_capability_bindings(self, products_table: MotoTable) -> None:
        container = _container(products_table.client)

        repo = container.resolve(Readable[Product])
        assert isinstance(repo, RepositoryDynamoDB)
        assert container.resolve_repo(Product) is not None

    @pytest.mark.asyncio
    async def test_registered_repository_persists_through_dynamodb(
        self,
        products_table: MotoTable,
    ) -> None:
        container = _container(products_table.client)

        repo = container.resolve(Creatable[Product])
        created = await repo.create(ProductCreate(id=1, name="Widget", price=2.5))
        assert created == Product(id=1, name="Widget", price=2.5)

        reader = container.resolve(Readable[Product])
        assert await reader.get_by_id(1) == Product(id=1, name="Widget", price=2.5)
