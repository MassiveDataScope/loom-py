"""``list_with_query`` yields the rows the pure-Python oracle selects.

``EXISTS`` / ``NOT_EXISTS`` are relation operators; the flat ``AcmeContractOrder`` model
has no relation, so they are outside this contract.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.repository.abc import (
    FilterGroup,
    FilterOp,
    FilterSpec,
    Listable,
    PageResult,
    QuerySpec,
    SortSpec,
)

from .conftest import BackendCase, OrderStatus, oracle, require, seed_rows

_ALL = 50

_FILTERS: dict[str, FilterGroup] = {
    "eq": FilterGroup(filters=(FilterSpec("status", FilterOp.EQ, OrderStatus.PAID),)),
    "ne": FilterGroup(filters=(FilterSpec("status", FilterOp.NE, OrderStatus.PAID),)),
    "gt": FilterGroup(filters=(FilterSpec("amount", FilterOp.GT, 20),)),
    "gte": FilterGroup(filters=(FilterSpec("amount", FilterOp.GTE, 20),)),
    "lt": FilterGroup(filters=(FilterSpec("amount", FilterOp.LT, 20),)),
    "lte": FilterGroup(filters=(FilterSpec("amount", FilterOp.LTE, 20),)),
    "in": FilterGroup(filters=(FilterSpec("customer", FilterOp.IN, ["alice", "bob", "zed"]),)),
    "like": FilterGroup(filters=(FilterSpec("customer", FilterOp.LIKE, "an%"),)),
    "like-single-char": FilterGroup(filters=(FilterSpec("customer", FilterOp.LIKE, "ann_"),)),
    "ilike": FilterGroup(filters=(FilterSpec("customer", FilterOp.ILIKE, "AN%"),)),
    "is-null": FilterGroup(filters=(FilterSpec("note", FilterOp.IS_NULL),)),
    "ne-on-nullable": FilterGroup(filters=(FilterSpec("note", FilterOp.NE, "rush"),)),
    "in-on-nullable": FilterGroup(filters=(FilterSpec("note", FilterOp.IN, ["gift", "none"]),)),
    "gt-on-nullable": FilterGroup(filters=(FilterSpec("note", FilterOp.GT, "a"),)),
    "and": FilterGroup(
        filters=(
            FilterSpec("amount", FilterOp.GTE, 20),
            FilterSpec("status", FilterOp.EQ, OrderStatus.PAID),
        )
    ),
    "or": FilterGroup(
        filters=(
            FilterSpec("status", FilterOp.EQ, OrderStatus.CANCELLED),
            FilterSpec("amount", FilterOp.LT, 20),
        ),
        op="OR",
    ),
    "and-empty-result": FilterGroup(
        filters=(
            FilterSpec("amount", FilterOp.GT, 20),
            FilterSpec("amount", FilterOp.LT, 20),
        )
    ),
    "or-with-null-check": FilterGroup(
        filters=(
            FilterSpec("note", FilterOp.IS_NULL),
            FilterSpec("customer", FilterOp.LIKE, "b%"),
        ),
        op="OR",
    ),
}

# Ties are left to the backend's id tie-breaker; the oracle applies the same one.
_SORTS: dict[str, tuple[SortSpec, ...]] = {
    "amount-desc-customer-asc": (SortSpec("amount", "DESC"), SortSpec("customer")),
    "status-asc-amount-desc": (SortSpec("status"), SortSpec("amount", "DESC")),
    "created-at-asc": (SortSpec("created_at"),),
}


async def _ids(repository: Any, query: QuerySpec) -> list[int]:
    result = await repository.list_with_query(query)
    assert isinstance(result, PageResult)
    return [row.id for row in result.items]


class TestContractQuery:
    @pytest.mark.parametrize("name", list(_FILTERS))
    async def test_filter_selects_the_oracle_rows(
        self, case: BackendCase, seeded: Any, name: str
    ) -> None:
        require(case, Listable)
        filters = _FILTERS[name]

        found = await _ids(seeded, QuerySpec(filters=filters, limit=_ALL))

        assert sorted(found) == sorted(oracle(seed_rows(), filters))

    @pytest.mark.parametrize("name", list(_SORTS))
    async def test_sort_orders_rows_like_the_oracle(
        self, case: BackendCase, seeded: Any, name: str
    ) -> None:
        require(case, Listable)
        sort = _SORTS[name]

        assert await _ids(seeded, QuerySpec(sort=sort, limit=_ALL)) == oracle(
            seed_rows(), sort=sort
        )

    async def test_filter_and_sort_compose(self, case: BackendCase, seeded: Any) -> None:
        require(case, Listable)
        filters = _FILTERS["gte"]
        sort = _SORTS["status-asc-amount-desc"]

        found = await _ids(seeded, QuerySpec(filters=filters, sort=sort, limit=_ALL))

        assert found == oracle(seed_rows(), filters, sort)
