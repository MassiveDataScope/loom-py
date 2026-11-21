from __future__ import annotations

from typing import cast

from loom.core.stores.repository.query import CursorResult, PageResult, QuerySpec
from loom.core.use_case import Compute, F, Input, Rule
from loom.core.use_case.use_case import UseCase
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.repository_contract import ProductRepo
from tests.integration.fake_repo.product.schemas import CreateProduct, UpdateProduct


def _normalize_name(value: str) -> str:
    return value.strip()


def _normalize_price(value: float) -> float:
    return round(value, 2)


def _name_must_not_be_blank(name: str) -> str | None:
    if not name.strip():
        return "name must not be blank"
    return None


def _price_must_be_positive(price: float) -> str | None:
    if price <= 0:
        return "price must be positive"
    return None


def _normalize_name_with_product_context(name: str | None, _product_id: str) -> str | None:
    if name is None:
        return None
    return _normalize_name(name)


CREATE_NORMALIZE_NAME = Compute.set(F(CreateProduct).name).from_command(
    F(CreateProduct).name, via=_normalize_name
)
CREATE_NORMALIZE_PRICE = Compute.set(F(CreateProduct).price).from_command(
    F(CreateProduct).price, via=_normalize_price
)
UPDATE_NORMALIZE_NAME = (
    Compute.set(F(UpdateProduct).name)
    .from_command(F(UpdateProduct).name, via=_normalize_name_with_product_context)
    .from_params("product_id")
    .when_present(F(UpdateProduct).name)
)
UPDATE_NORMALIZE_PRICE = (
    Compute.set(F(UpdateProduct).price)
    .from_command(F(UpdateProduct).price, via=_normalize_price)
    .when_present(F(UpdateProduct).price)
)

CREATE_NAME_RULE = Rule.check(F(CreateProduct).name, via=_name_must_not_be_blank)
CREATE_PRICE_RULE = Rule.check(F(CreateProduct).price, via=_price_must_be_positive)
UPDATE_NAME_RULE = Rule.check(F(UpdateProduct).name, via=_name_must_not_be_blank).when_present(
    F(UpdateProduct).name
)
UPDATE_PRICE_RULE = Rule.check(F(UpdateProduct).price, via=_price_must_be_positive).when_present(
    F(UpdateProduct).price
)


def _patch_payload_is_empty(_cmd: UpdateProduct, fields: frozenset[str]) -> bool:
    return len(fields) == 0


def _is_system_product_name_update_forbidden(
    _cmd: UpdateProduct,
    _fields_set: frozenset[str],
    product_id: str,
) -> bool:
    return str(product_id) == "1"


def _name_cannot_match_price(name: str | None, price: float | None) -> bool:
    if name is None or price is None:
        return False
    return name.strip() == str(price)


UPDATE_NOT_EMPTY_RULE = Rule.forbid(
    _patch_payload_is_empty,
    message="at least one field must be provided",
).from_command()


UPDATE_SYSTEM_NAME_IMMUTABLE_RULE = (
    Rule.forbid(
        _is_system_product_name_update_forbidden,
        message="system product name cannot be changed",
    )
    .from_command(F(UpdateProduct).name)
    .from_params("product_id")
    .when_present(F(UpdateProduct).name)
)

UPDATE_NAME_PRICE_MISMATCH_RULE = (
    Rule.forbid(
        _name_cannot_match_price,
        message="name cannot be equal to price",
    )
    .from_command(F(UpdateProduct).name, F(UpdateProduct).price)
    .when_present(F(UpdateProduct).name & F(UpdateProduct).price)
)


class CreateProductUseCase(UseCase[Product, Product]):
    computes = [CREATE_NORMALIZE_NAME, CREATE_NORMALIZE_PRICE]
    rules = [CREATE_NAME_RULE, CREATE_PRICE_RULE]

    async def execute(self, cmd: CreateProduct = Input()) -> Product:
        return cast(Product, await self.main_repo.create(cmd))


class GetProductUseCase(UseCase[Product, Product | None]):
    async def execute(self, product_id: str, profile: str = "default") -> Product | None:
        return await self.main_repo.get_by_id(int(product_id), profile=profile)


class ListProductsUseCase(UseCase[Product, PageResult[Product] | CursorResult[Product]]):
    async def execute(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[Product] | CursorResult[Product]:
        return await self.main_repo.list_with_query(query, profile=profile)


class UpdateProductUseCase(UseCase[Product, Product | None]):
    computes = [UPDATE_NORMALIZE_NAME, UPDATE_NORMALIZE_PRICE]
    rules = [
        UPDATE_NOT_EMPTY_RULE,
        UPDATE_NAME_RULE,
        UPDATE_PRICE_RULE,
        UPDATE_SYSTEM_NAME_IMMUTABLE_RULE,
        UPDATE_NAME_PRICE_MISMATCH_RULE,
    ]

    async def execute(
        self,
        product_id: str,
        cmd: UpdateProduct = Input(),
    ) -> Product | None:
        return await self.main_repo.update(int(product_id), cmd)


class DeleteProductUseCase(UseCase[Product, bool]):
    async def execute(self, product_id: str) -> bool:
        return await self.main_repo.delete(int(product_id))


class FindProductByNameUseCase(UseCase[Product, Product | None]):
    def __init__(self, product_repo: ProductRepo) -> None:
        super().__init__()
        self._product_repo = product_repo

    async def execute(self, name: str) -> Product | None:
        return await self._product_repo.get_by_name(name)
