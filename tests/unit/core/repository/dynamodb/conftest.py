from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated, Any, NoReturn

import pytest

from loom.core.model import BaseModel, Field, Float, Integer, String
from tests.helpers.aws import moto_client
from tests.helpers.dynamodb import create_table, record_transactions


class Product(BaseModel):
    __tablename__ = "products"
    id: Annotated[int, Integer, Field(primary_key=True)]
    name: Annotated[str, String] = ""
    price: Annotated[float, Float] = 0.0


class ProductCreate(BaseModel):
    id: Annotated[int, Integer, Field(primary_key=True)]
    name: Annotated[str, String] = ""
    price: Annotated[float, Float] = 0.0


class ProductUpdate(BaseModel):
    name: Annotated[str, String] = ""


@pytest.fixture
def product_model() -> type[Product]:
    return Product


@pytest.fixture
def dynamodb(mocked_aws: None) -> Any:
    """A DynamoDB client of the moto account with the ``products`` table."""
    client = moto_client("dynamodb")
    create_table(client, "products")
    return client


@pytest.fixture
def transactions(dynamodb: Any) -> Iterator[list[int]]:
    """The size of every transaction ``dynamodb`` commits during the test, in order."""
    with record_transactions(dynamodb) as sizes:
        yield sizes


@pytest.fixture
def refusing_client(dynamodb: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """The client with every item call failing the test."""

    def _refuse(**kwargs: Any) -> NoReturn:
        raise AssertionError(f"the client was called with {kwargs}")

    for method in ("get_item", "put_item", "delete_item", "transact_write_items"):
        monkeypatch.setattr(dynamodb, method, _refuse)
    return dynamodb
