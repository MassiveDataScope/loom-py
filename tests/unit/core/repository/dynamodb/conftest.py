from __future__ import annotations

from typing import Annotated, Any, NoReturn

import pytest

from loom.core.model import BaseModel, Field, Float, Integer, String

from ._fake import FakeClient as FakeClient


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
def fake_client() -> FakeClient:
    return FakeClient(key_name="id")


@pytest.fixture
def refusing_client(fake_client: FakeClient, monkeypatch: pytest.MonkeyPatch) -> FakeClient:
    """The fake client with every item call failing the test."""

    def _refuse(**kwargs: Any) -> NoReturn:
        raise AssertionError(f"the client was called with {kwargs}")

    for method in ("get_item", "put_item", "delete_item", "transact_write_items"):
        monkeypatch.setattr(fake_client, method, _refuse)
    return fake_client
