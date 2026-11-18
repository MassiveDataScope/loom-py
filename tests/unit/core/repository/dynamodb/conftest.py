from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated, Any, NoReturn

import pytest

from loom.core.model import BaseModel, Field, Float, Integer, String
from tests.helpers.aws import build_moto_client, moto_client
from tests.helpers.dynamodb import MotoTable


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
def products_table(mocked_aws: None) -> MotoTable:
    """The ``products`` table of the moto account."""
    return MotoTable(moto_client("dynamodb"), "products").create()


@pytest.fixture
def dynamodb(products_table: MotoTable) -> Any:
    """The DynamoDB client of the ``products`` table."""
    return products_table.client


@pytest.fixture
def transactions(products_table: MotoTable) -> Iterator[list[int]]:
    """The size of every transaction committed on ``products`` during the test, in order."""
    with products_table.recording_transactions() as sizes:
        yield sizes


@pytest.fixture
def refusing_client() -> Any:
    """A client of its own whose every call fails the test before it leaves the client."""

    def refuse(model: Any, params: Any, **_: Any) -> NoReturn:
        raise AssertionError(f"{model.name} was called with {params}")

    client = build_moto_client("dynamodb")
    client.meta.events.register("before-call", refuse)
    return client
