from __future__ import annotations

import msgspec
import pytest

from loom.core.stores.repository import (
    FilterGroup,
    FilterOp,
    FilterParams,
    FilterSpec,
    PageParams,
    PaginationMode,
    QuerySpec,
    SortSpec,
    build_page_result,
)


class AcmeQueryOutput(msgspec.Struct):
    id: int


class TestPageParams:
    def test_offset_calculation(self) -> None:
        params = PageParams(page=3, limit=25)
        assert params.offset == 50

    @pytest.mark.parametrize(
        ("match", "page", "limit"),
        [
            pytest.param("page must be >= 1", 0, 10, id="invalid_page_raises_error"),
            pytest.param("limit must be in", 1, 0, id="invalid_limit_raises_error"),
        ],
    )
    def test_invalid_page_params_raise_error(self, match: str, page: int, limit: int) -> None:
        with pytest.raises(ValueError, match=match):
            PageParams(page=page, limit=limit)


class TestBuildPageResult:
    def test_build_page_result_calculates_has_next(self, page_params: PageParams) -> None:
        result = build_page_result(
            items=[AcmeQueryOutput(id=1)],
            total_count=25,
            page_params=page_params,
        )

        assert result.page == 2
        assert result.limit == 10
        assert result.has_next is True
        assert result.items[0].id == 1


class TestFilterParams:
    def test_filter_params_default_dict(self) -> None:
        params = FilterParams()
        assert params.filters == {}


class TestQueryContracts:
    def test_value_objects_support_positional_construction(self) -> None:
        filter_spec = FilterSpec("price", FilterOp.GTE, 10.0)
        filter_group = FilterGroup((filter_spec,), "OR")
        sort_spec = SortSpec("name", "DESC")
        query = QuerySpec(
            filter_group,
            (sort_spec,),
            PaginationMode.CURSOR,
            25,
            3,
            "cursor-1",
        )

        assert filter_spec.field == "price"
        assert filter_spec.op is FilterOp.GTE
        assert filter_spec.value == pytest.approx(10.0)
        assert filter_group.filters == (filter_spec,)
        assert filter_group.op == "OR"
        assert sort_spec.field == "name"
        assert sort_spec.direction == "DESC"
        assert query.filters == filter_group
        assert query.sort == (sort_spec,)
        assert query.pagination is PaginationMode.CURSOR
        assert query.limit == 25
        assert query.page == 3
        assert query.cursor == "cursor-1"

    def test_value_objects_are_frozen(self) -> None:
        query = QuerySpec()

        with pytest.raises(AttributeError):
            query.limit = 100  # type: ignore[misc]

    def test_value_objects_roundtrip_with_msgspec(self) -> None:
        original = QuerySpec(
            filters=FilterGroup(
                filters=(FilterSpec("price", FilterOp.LTE, 99.0),),
                op="AND",
            ),
            sort=(SortSpec("created_at", "DESC"),),
            pagination=PaginationMode.OFFSET,
            limit=20,
            page=2,
        )

        restored = msgspec.json.decode(
            msgspec.json.encode(original),
            type=QuerySpec,
        )

        assert restored == original
