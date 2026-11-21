"""``make`` builds the store query values with the defaults their types declare."""

from __future__ import annotations

from loom.core.stores.repository.query import (
    FIRST_PAGE,
    FilterParams,
    PageParams,
    PaginationMode,
    QuerySpec,
)
from loom.testing import make


class TestRegisteredStoreFactories:
    def test_a_query_is_unfiltered_unsorted_and_offset_by_default(self) -> None:
        query = make(QuerySpec)

        assert (query.filters, query.sort, query.pagination, query.page, query.cursor) == (
            None,
            (),
            PaginationMode.OFFSET,
            FIRST_PAGE,
            None,
        )

    def test_a_given_query_field_wins_over_the_default(self) -> None:
        assert make(QuerySpec, limit=2).limit == 2

    def test_a_page_is_the_first_one_by_default(self) -> None:
        assert make(PageParams).page == FIRST_PAGE

    def test_a_filter_container_matches_nothing_by_default(self) -> None:
        assert make(FilterParams).filters == {}
