from __future__ import annotations

from loom.core.use_case.field_ref import F
from loom.rest.model import RestInterface, RestRoute
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.use_cases import (
    CreateProductUseCase,
    DeleteProductUseCase,
    FindProductByNameUseCase,
    GetProductUseCase,
    ListProductsUseCase,
    UpdateProductUseCase,
)


class ProductRestInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    profile_default = "default"
    allowed_profiles = ("default", "with_details")
    routes = (
        RestRoute(
            use_case=CreateProductUseCase,
            method="POST",
            path="/",
            status_code=201,
        ),
        RestRoute(
            use_case=ListProductsUseCase,
            method="GET",
            path="/",
            expose_profile=True,
            filterable=(F(Product).name, F(Product).price),
            sortable=(F(Product).price,),
        ),
        RestRoute(
            use_case=FindProductByNameUseCase,
            method="GET",
            path="/by-name/{name}",
        ),
        RestRoute(
            use_case=GetProductUseCase,
            method="GET",
            path="/{product_id}",
            expose_profile=True,
        ),
        RestRoute(
            use_case=UpdateProductUseCase,
            method="PATCH",
            path="/{product_id}",
        ),
        RestRoute(
            use_case=DeleteProductUseCase,
            method="DELETE",
            path="/{product_id}",
        ),
    )
