"""The query cache key carries the wrapped repository's cache scope.

A first cursor page and its token are valid only in the scope that issued
them, so the same ``QuerySpec`` from another scope must miss the cache.
"""

from __future__ import annotations

import pytest

from loom.core.cache import CacheConfig
from loom.core.repository.abc import SupportsCacheScope
from loom.core.repository.abc.query import PaginationMode, QuerySpec

from ._doubles import CountingRepository, Widget, wrap_with_cache

_QUERY = QuerySpec(pagination=PaginationMode.CURSOR, limit=2)
_ROWS = [Widget(id=n, name=f"w{n}") for n in (1, 2, 3)]


class _ScopedRepository(CountingRepository[Widget]):
    """A repository whose caller scope a test switches."""

    def __init__(self) -> None:
        super().__init__(_ROWS, Widget)
        self.scope = ""

    def cache_scope(self) -> str:
        return self.scope


@pytest.mark.asyncio
async def test_the_same_query_in_two_scopes_is_cached_under_two_keys(
    cache_config: CacheConfig,
) -> None:
    repository = _ScopedRepository()
    assert isinstance(repository, SupportsCacheScope)
    wrapper = wrap_with_cache(repository, cache_config).wrapper

    for scope in ("tenant-a", "tenant-b", "tenant-a"):
        repository.scope = scope
        await wrapper.list_with_query(_QUERY)

    assert repository.list_with_query_calls == 2


@pytest.mark.asyncio
async def test_a_repository_without_a_cache_scope_shares_one_key(
    cache_config: CacheConfig,
) -> None:
    repository = CountingRepository(_ROWS, Widget)
    assert not isinstance(repository, SupportsCacheScope)
    wrapper = wrap_with_cache(repository, cache_config).wrapper

    await wrapper.list_with_query(_QUERY)
    await wrapper.list_with_query(_QUERY)

    assert repository.list_with_query_calls == 1
