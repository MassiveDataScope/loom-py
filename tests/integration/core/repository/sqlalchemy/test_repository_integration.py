from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.repository.abc import (
    FilterGroup,
    FilterOp,
    FilterParams,
    FilterSpec,
    PageParams,
    PaginationMode,
    QuerySpec,
    SortSpec,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy, with_session_scope
from loom.testing import RepositoryIntegrationHarness, ScenarioDict
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.review.schemas import CreateProductReview
from tests.integration.fake_repo.product.schemas import CreateProduct, UpdateProduct


class _CustomProductRepository(RepositorySQLAlchemy[Product, int]):
    """Example custom repository using the compiled CoreModel read path."""

    @with_session_scope
    async def list_ids_with_price_gt(self, session: Any, *, min_price: float) -> list[int]:
        core_model = self._effective_core_model
        stmt = (
            core_model.select("default")
            .where(self._column_for_field("price") > min_price)
            .order_by(*self._id_columns())
        )
        rows = await core_model.fetch_all(session, stmt, profile="default")
        return [int(item.id) for item in rows]


class TestRepositorySQLAlchemyIntegration:
    @pytest.mark.asyncio
    async def test_crud_flow_with_real_sqlite(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        created = await integration_context.product.repository.create(
            CreateProduct(name="keyboard", price=120.0)
        )
        assert isinstance(created, Product)
        assert created.id == 1

        loaded = await integration_context.product.repository.get_by_id(1)
        assert loaded is not None
        assert isinstance(loaded, Product)
        assert loaded.name == "keyboard"

        updated = await integration_context.product.repository.update(1, UpdateProduct(price=99.9))
        assert updated is not None
        assert float(updated.price) == pytest.approx(99.9)

        deleted = await integration_context.product.repository.delete(1)
        assert deleted is True

        missing = await integration_context.product.repository.get_by_id(1)
        assert missing is None

    @pytest.mark.asyncio
    async def test_create_many_persists_rows_in_input_order(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        created = await integration_context.product.repository.create_many(
            [
                CreateProduct(name="mouse", price=25.0),
                CreateProduct(name="monitor", price=300.0),
                CreateProduct(name="cable", price=5.0),
            ]
        )

        assert [item.name for item in created] == ["mouse", "monitor", "cable"]
        assert [item.id for item in created] == [1, 2, 3]
        assert await integration_context.product.repository.count() == 3

    @pytest.mark.asyncio
    async def test_paginated_list_with_filters(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)

        page = await integration_context.product.repository.list_paginated(
            PageParams(page=1, limit=10),
            FilterParams(filters={"price": 20.0}),
        )

        assert page.total_count == 2
        assert len(page.items) == 2
        assert {item.name for item in page.items} == {"b", "c"}

    @pytest.mark.asyncio
    async def test_list_with_query_offset_supports_filters_and_pagination(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)

        query_page_1 = QuerySpec(
            filters=FilterGroup(filters=(FilterSpec(field="price", op=FilterOp.EQ, value=20.0),)),
            sort=(SortSpec(field="id", direction="ASC"),),
            pagination=PaginationMode.OFFSET,
            limit=1,
            page=1,
        )
        page_1 = await integration_context.product.repository.list_with_query(query_page_1)

        assert page_1.total_count == 2
        assert page_1.page == 1
        assert page_1.limit == 1
        assert page_1.has_next is True
        assert [item.name for item in page_1.items] == ["b"]

        query_page_2 = QuerySpec(
            filters=query_page_1.filters,
            sort=query_page_1.sort,
            pagination=PaginationMode.OFFSET,
            limit=1,
            page=2,
        )
        page_2 = await integration_context.product.repository.list_with_query(query_page_2)

        assert page_2.total_count == 2
        assert page_2.page == 2
        assert page_2.limit == 1
        assert page_2.has_next is False
        assert [item.name for item in page_2.items] == ["c"]

    @pytest.mark.asyncio
    async def test_list_with_query_cursor_supports_filters_and_next_cursor(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)

        first_query = QuerySpec(
            filters=FilterGroup(filters=(FilterSpec(field="price", op=FilterOp.EQ, value=20.0),)),
            sort=(SortSpec(field="id", direction="ASC"),),
            pagination=PaginationMode.CURSOR,
            limit=1,
        )
        first_page = await integration_context.product.repository.list_with_query(first_query)

        assert first_page.has_next is True
        assert first_page.next_cursor is not None
        assert [item.name for item in first_page.items] == ["b"]

        second_query = QuerySpec(
            filters=first_query.filters,
            sort=first_query.sort,
            pagination=PaginationMode.CURSOR,
            limit=1,
            cursor=first_page.next_cursor,
        )
        second_page = await integration_context.product.repository.list_with_query(second_query)

        assert second_page.has_next is False
        assert second_page.next_cursor is None
        assert [item.name for item in second_page.items] == ["c"]

    @pytest.mark.asyncio
    async def test_profile_default_omits_unloaded_fields(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        created = await integration_context.product.repository.create(
            CreateProduct(name="monitor", price=180.0)
        )

        loaded = await integration_context.product.repository.get_by_id(
            created.id, profile="default"
        )
        assert loaded is not None
        data = msgspec.json.decode(msgspec.json.encode(loaded))
        assert "countReviews" not in data
        assert "hasReviews" not in data
        assert "reviewSnippets" not in data

    @pytest.mark.asyncio
    async def test_with_details_loads_orm_and_projection_fields(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_one_product_with_details: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_one_product_with_details)
        loaded = await integration_context.product.repository.get_by_id(1, profile="with_details")
        assert loaded is not None
        assert isinstance(loaded, Product)

        categories = loaded.categories
        assert isinstance(categories, list)
        assert {category_item["name"] for category_item in categories} == {"electronics"}

        assert loaded.has_reviews is True
        assert loaded.count_reviews == 2
        assert isinstance(loaded.review_snippets, list)
        assert {snippet["comment"] for snippet in loaded.review_snippets} == {"great", "solid"}

    @pytest.mark.asyncio
    async def test_count_returns_zero_on_empty_table(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        total = await integration_context.product.repository.count()
        assert total == 0

    @pytest.mark.asyncio
    async def test_count_returns_exact_row_count(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)
        total = await integration_context.product.repository.count()
        assert total == 3

    @pytest.mark.asyncio
    async def test_update_returns_updated_entity_in_single_roundtrip(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        created = await integration_context.product.repository.create(
            CreateProduct(name="desk", price=200.0)
        )
        assert created.id is not None

        updated = await integration_context.product.repository.update(
            created.id, UpdateProduct(price=150.0)
        )
        assert updated is not None
        assert float(updated.price) == pytest.approx(150.0)
        assert updated.name == "desk"

    @pytest.mark.asyncio
    async def test_update_returns_none_for_missing_id(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = await integration_context.product.repository.update(9999, UpdateProduct(price=1.0))
        assert result is None

    @pytest.mark.asyncio
    async def test_count_after_delete_reflects_removal(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_two_products: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_two_products)
        assert await integration_context.product.repository.count() == 2
        await integration_context.product.repository.delete(1)
        assert await integration_context.product.repository.count() == 1

    @pytest.mark.asyncio
    async def test_custom_repository_can_use_core_model_for_custom_read(
        self,
        integration_context: RepositoryIntegrationHarness,
        scenario_catalog_with_price_20: ScenarioDict,
    ) -> None:
        await integration_context.load(scenario_catalog_with_price_20)
        custom_repo = _CustomProductRepository(
            session_manager=integration_context.session_manager,
            model=Product,
        )

        ids = await custom_repo.list_ids_with_price_gt(min_price=15.0)
        assert ids == [2, 3]


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason=(
        "A filter on a related model's column compiles without a join, so each "
        "matching related row multiplies every root row; relation filtering is a "
        "follow-up feature."
    ),
)
async def test_a_filter_on_a_related_column_selects_each_matching_row_once(
    integration_context: RepositoryIntegrationHarness,
) -> None:
    await integration_context.load(
        {
            "product": [CreateProduct(name="a", price=1.0), CreateProduct(name="b", price=2.0)],
            "review": [
                CreateProductReview(product_id=1, rating=5, comment="x"),
                CreateProductReview(product_id=1, rating=4, comment="y"),
                CreateProductReview(product_id=2, rating=5, comment="z"),
            ],
        }
    )

    page = await integration_context.product.repository.list_with_query(
        QuerySpec(
            filters=FilterGroup(
                filters=(FilterSpec(field="reviews.rating", op=FilterOp.EQ, value=5),)
            ),
            sort=(SortSpec(field="id", direction="ASC"),),
        )
    )

    assert [item.name for item in page.items] == ["a", "b"]
    assert page.total_count == 2
