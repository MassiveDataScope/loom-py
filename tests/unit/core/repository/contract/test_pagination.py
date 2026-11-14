from __future__ import annotations

from typing import Any

import pytest

from loom.core.repository.abc import (
    CursorResult,
    FilterGroup,
    FilterOp,
    FilterSpec,
    Listable,
    PageResult,
    PaginationMode,
    QuerySpec,
    SortSpec,
)

from .conftest import SEED, BackendCase, OrderStatus, oracle, require, seed_rows

_PAGE = 2
_PAID = FilterGroup(filters=(FilterSpec("status", FilterOp.EQ, OrderStatus.PAID),))
_NONE_MATCH = FilterGroup(filters=(FilterSpec("amount", FilterOp.GT, 10_000),))

# Every sort has duplicate key values across page boundaries at limit 2.
_CURSOR_SORTS: dict[str, tuple[SortSpec, ...]] = {
    "default": (),
    "single-desc": (SortSpec("amount", "DESC"),),
    "asc-then-desc": (SortSpec("amount"), SortSpec("status", "DESC")),
    "desc-then-asc": (SortSpec("status", "DESC"), SortSpec("created_at")),
    "pk-desc": (SortSpec("id", "DESC"),),
}


async def _page(repository: Any, page: int, filters: FilterGroup | None = None) -> PageResult[Any]:
    result = await repository.list_with_query(QuerySpec(filters=filters, limit=_PAGE, page=page))
    assert isinstance(result, PageResult)
    return result


async def _walk(
    repository: Any, sort: tuple[SortSpec, ...], filters: FilterGroup | None = None
) -> list[int]:
    seen: list[int] = []
    cursor: str | None = None
    for _ in range(len(SEED) + 1):
        result = await repository.list_with_query(
            QuerySpec(
                filters=filters,
                sort=sort,
                pagination=PaginationMode.CURSOR,
                limit=_PAGE,
                cursor=cursor,
            )
        )
        assert isinstance(result, CursorResult)
        assert len(result.items) <= _PAGE
        seen.extend(row.id for row in result.items)
        if not result.has_next:
            assert result.next_cursor is None
            return seen
        cursor = result.next_cursor
    raise AssertionError("cursor walk did not terminate")


class TestContractPagination:
    async def test_offset_pages_carry_totals(self, case: BackendCase, seeded: Any) -> None:
        require(case, Listable)
        pages = len(SEED) // _PAGE

        first = await _page(seeded, 1)
        last = await _page(seeded, pages)
        beyond = await _page(seeded, pages + 1)

        assert (first.total_count, first.page, first.limit, first.has_next) == (
            len(SEED),
            1,
            _PAGE,
            True,
        )
        assert (last.total_count, last.has_next, len(last.items)) == (len(SEED), False, _PAGE)
        assert (beyond.total_count, beyond.has_next, beyond.items) == (len(SEED), False, ())

    @pytest.mark.parametrize("name", list(_CURSOR_SORTS))
    async def test_offset_pages_cover_every_row_once(
        self, case: BackendCase, seeded: Any, name: str
    ) -> None:
        """Ties across page boundaries at limit 2 are only broken exactly by the id."""
        require(case, Listable)
        sort = _CURSOR_SORTS[name]

        seen: list[int] = []
        for page in range(1, len(SEED) // _PAGE + 1):
            result = await seeded.list_with_query(QuerySpec(sort=sort, limit=_PAGE, page=page))
            assert isinstance(result, PageResult)
            seen.extend(row.id for row in result.items)

        assert seen == oracle(seed_rows(), sort=sort)

    @pytest.mark.parametrize("filters", [_PAID, _NONE_MATCH])
    async def test_offset_total_reflects_the_filter(
        self, case: BackendCase, seeded: Any, filters: FilterGroup
    ) -> None:
        require(case, Listable)
        expected = oracle(seed_rows(), filters)

        first = await _page(seeded, 1, filters)
        beyond = await _page(seeded, len(expected) // _PAGE + 2, filters)

        assert first.total_count == len(expected)
        assert [row.id for row in first.items] == expected[:_PAGE]
        assert first.has_next == (len(expected) > _PAGE)
        assert (beyond.total_count, beyond.items) == (len(expected), ())

    @pytest.mark.parametrize("name", list(_CURSOR_SORTS))
    async def test_cursor_walk_visits_every_row_once(
        self, case: BackendCase, seeded: Any, name: str
    ) -> None:
        require(case, Listable)
        sort = _CURSOR_SORTS[name]

        assert await _walk(seeded, sort) == oracle(seed_rows(), sort=sort)

    async def test_cursor_walk_honours_the_filter(self, case: BackendCase, seeded: Any) -> None:
        require(case, Listable)
        sort = _CURSOR_SORTS["single-desc"]

        assert await _walk(seeded, sort, _PAID) == oracle(seed_rows(), _PAID, sort)
