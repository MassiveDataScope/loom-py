"""Every cache key carries the caller's scope, and a write still reaches every scope."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest

from loom.core.cache import CacheConfig, CachedRepository, GenerationalDependencyResolver
from loom.core.cache.decorators import cache_query
from loom.core.cache.keys import custom_query_key, entity_key, full_hash, list_index_key
from loom.core.repository import PageParams

from ._doubles import (
    CachedEnv,
    CountingCacheBackend,
    CountingRepository,
    Widget,
    WidgetUpdate,
    WritableRepository,
    wrap_with_cache,
)

ROW_A = 1
PAGE = PageParams(page=1, limit=10)
SCOPE_A = '[["loom.scope.tenant","a"]]'
SCOPE_B = '[["loom.scope.tenant","b"]]'


class ScopedRepository(WritableRepository[Widget]):
    """Writable double that reports the scope a test sets on it, counting the reads."""

    def __init__(self, rows: Sequence[Widget]) -> None:
        super().__init__(rows, Widget)
        self.scope = SCOPE_A
        self.reads = 0

    def cache_scope(self) -> str:
        self.reads += 1
        return self.scope


class SwitchingRepository(ScopedRepository):
    """Double whose caller scope changes while a read is loading."""

    async def get_by_id(self, obj_id: Any, profile: str = "default") -> Widget | None:
        self.scope = SCOPE_B
        return await super().get_by_id(obj_id, profile)

    async def list_paginated(self, *args: Any, **kwargs: Any) -> Any:
        self.scope = SCOPE_B
        return await super().list_paginated(*args, **kwargs)

    @cache_query(scope="list")
    async def all_names(self) -> list[str]:
        self.scope = SCOPE_B
        return [row.name for row in self.storage.values()]


class _SessionManager:
    def __init__(self, has_session_settings: bool) -> None:
        self.has_session_settings = has_session_settings


class SettingsRepository(CountingRepository[Widget]):
    """Double whose sessions may carry settings, but which reports no scope."""

    def __init__(self, has_session_settings: bool) -> None:
        super().__init__([Widget(id=ROW_A, name="a")], Widget)
        self.session_manager = _SessionManager(has_session_settings)


def _entity_entries(backend: CountingCacheBackend) -> list[str]:
    return [key for key in backend.data if key.startswith("widget:") and ":list:" not in key]


def _wrap(repository: Any, backend: CountingCacheBackend) -> CachedRepository[Any, Any, Any, Any]:
    return CachedRepository(
        repository,
        config=CacheConfig(enabled=True, default_ttl=100, default_list_ttl=50),
        cache=backend,
        dependency_resolver=GenerationalDependencyResolver(backend),
    )


@pytest.fixture
def env(cache_config: CacheConfig) -> CachedEnv[Widget]:
    rows = [Widget(id=ROW_A, name="a"), Widget(id=2, name="b")]
    return wrap_with_cache(ScopedRepository(rows), cache_config)


def _scoped(env: CachedEnv[Widget]) -> ScopedRepository:
    assert isinstance(env.repository, ScopedRepository)
    return env.repository


class TestKeys:
    def test_an_entity_key_differs_by_scope(self) -> None:
        keys = {entity_key("widget", 1, "default", "f", scope=s) for s in ("", SCOPE_A, SCOPE_B)}

        assert len(keys) == 3

    def test_a_list_index_key_differs_by_scope(self) -> None:
        keys = {
            list_index_key("widget", "f", 1, 10, "default", "d", scope=s)
            for s in ("", SCOPE_A, SCOPE_B)
        }

        assert len(keys) == 3

    def test_a_custom_query_key_differs_by_scope(self) -> None:
        keys = {
            custom_query_key("widget", "all_names", "h", "d", scope=s)
            for s in ("", SCOPE_A, SCOPE_B)
        }

        assert len(keys) == 3

    def test_an_unscoped_key_carries_an_empty_scope(self) -> None:
        assert entity_key("widget", 1, "default", "f", scope="").endswith(":scope=")
        assert list_index_key("widget", "f", 1, 10, "default", "d", scope="").endswith(":scope=")

    def test_a_key_carries_the_scope_digest_not_its_values(self) -> None:
        key = entity_key("widget", 1, "default", "f", scope=SCOPE_A)

        assert "tenant" not in key
        assert key.endswith(f":scope={full_hash(SCOPE_A)}")

    def test_the_full_hash_is_the_whole_sha256(self) -> None:
        assert len(full_hash("x")) == 64


class TestReadsStayInTheirScope:
    @pytest.mark.asyncio
    async def test_a_row_cached_for_one_scope_is_read_again_for_another(
        self, env: CachedEnv[Widget]
    ) -> None:
        await env.wrapper.get_by_id(ROW_A)
        await env.wrapper.get_by_id(ROW_A)
        _scoped(env).scope = SCOPE_B
        await env.wrapper.get_by_id(ROW_A)

        assert env.repository.get_by_id_calls == 2

    @pytest.mark.asyncio
    async def test_a_page_cached_for_one_scope_is_read_again_for_another(
        self, env: CachedEnv[Widget]
    ) -> None:
        await env.wrapper.list_paginated(PAGE)
        await env.wrapper.list_paginated(PAGE)
        _scoped(env).scope = SCOPE_B
        await env.wrapper.list_paginated(PAGE)

        assert env.repository.list_paginated_calls == 2

    @pytest.mark.asyncio
    async def test_a_custom_list_read_cached_for_one_scope_runs_again_for_another(
        self, env: CachedEnv[Widget]
    ) -> None:
        await env.wrapper.all_names()
        await env.wrapper.all_names()
        _scoped(env).scope = SCOPE_B
        await env.wrapper.all_names()

        assert _scoped(env).all_names_calls == 2

    @pytest.mark.asyncio
    async def test_a_custom_entity_read_cached_for_one_scope_runs_again_for_another(
        self, env: CachedEnv[Widget]
    ) -> None:
        await env.wrapper.note_count(ROW_A)
        await env.wrapper.note_count(ROW_A)
        _scoped(env).scope = SCOPE_B
        await env.wrapper.note_count(ROW_A)

        assert env.repository.note_count_calls == 2


class TestAWriteReachesEveryScope:
    @pytest.mark.asyncio
    async def test_an_update_evicts_the_row_in_every_scope(self, env: CachedEnv[Widget]) -> None:
        repository = _scoped(env)
        for scope in (SCOPE_A, SCOPE_B, SCOPE_A, SCOPE_B):
            repository.scope = scope
            await env.wrapper.get_by_id(ROW_A)
        assert repository.get_by_id_calls == 2

        repository.scope = SCOPE_A
        await env.wrapper.update(ROW_A, WidgetUpdate(name="a-updated"))

        for scope in (SCOPE_A, SCOPE_B):
            repository.scope = scope
            row = await env.wrapper.get_by_id(ROW_A)
            assert row is not None
            assert row.name == "a-updated"
        assert repository.get_by_id_calls == 4

    @pytest.mark.asyncio
    async def test_an_update_evicts_the_page_in_every_scope(self, env: CachedEnv[Widget]) -> None:
        repository = _scoped(env)
        for scope in (SCOPE_A, SCOPE_B, SCOPE_A, SCOPE_B):
            repository.scope = scope
            await env.wrapper.list_paginated(PAGE)
        assert repository.list_paginated_calls == 2

        repository.scope = SCOPE_B
        await env.wrapper.update(ROW_A, WidgetUpdate(name="a-updated"))

        for scope in (SCOPE_A, SCOPE_B):
            repository.scope = scope
            await env.wrapper.list_paginated(PAGE)
        assert repository.list_paginated_calls == 4

    @pytest.mark.asyncio
    async def test_an_update_evicts_the_custom_reads_in_every_scope(
        self, env: CachedEnv[Widget]
    ) -> None:
        repository = _scoped(env)
        for scope in (SCOPE_A, SCOPE_B, SCOPE_A, SCOPE_B):
            repository.scope = scope
            await env.wrapper.all_names()
            await env.wrapper.note_count(ROW_A)
        assert (repository.all_names_calls, repository.note_count_calls) == (2, 2)

        repository.scope = SCOPE_A
        await env.wrapper.update(ROW_A, WidgetUpdate(name="a-updated"))

        for scope in (SCOPE_A, SCOPE_B):
            repository.scope = scope
            assert await env.wrapper.all_names() == ["a-updated", "b"]
            assert await env.wrapper.note_count(ROW_A) == len("a-updated")
        assert (repository.all_names_calls, repository.note_count_calls) == (4, 4)


class TestTheScopeIsNeverLost:
    def test_the_wrapper_forwards_the_wrapped_scope(self, env: CachedEnv[Widget]) -> None:
        _scoped(env).scope = SCOPE_B

        assert env.wrapper.cache_scope() == SCOPE_B

    def test_an_unscoped_repository_reports_the_empty_scope(self) -> None:
        wrapper = _wrap(
            CountingRepository([Widget(id=ROW_A, name="a")], Widget), CountingCacheBackend()
        )

        assert wrapper.cache_scope() == ""

    @pytest.mark.asyncio
    async def test_a_wrapper_around_a_wrapper_keys_by_the_innermost_scope(self) -> None:
        repository = ScopedRepository([Widget(id=ROW_A, name="a")])
        outer = _wrap(_wrap(repository, CountingCacheBackend()), CountingCacheBackend())

        await outer.get_by_id(ROW_A)
        repository.scope = SCOPE_B
        await outer.get_by_id(ROW_A)

        assert repository.get_by_id_calls == 2

    def test_a_repository_with_session_settings_and_no_scope_is_refused(self) -> None:
        with pytest.raises(RuntimeError, match="cache_scope"):
            _wrap(SettingsRepository(has_session_settings=True), CountingCacheBackend())

    def test_a_repository_without_session_settings_needs_no_scope(self) -> None:
        wrapper = _wrap(SettingsRepository(has_session_settings=False), CountingCacheBackend())

        assert wrapper.cache_scope() == ""


class TestAScopeChangeSkipsTheWrite:
    """The scope is read once per operation; a write is skipped if it changed since."""

    @pytest.mark.asyncio
    async def test_an_entity_loaded_under_one_scope_is_not_stored_after_a_change(self) -> None:
        repository = SwitchingRepository([Widget(id=ROW_A, name="a")])
        backend = CountingCacheBackend()
        wrapper = _wrap(repository, backend)

        row = await wrapper.get_by_id(ROW_A)

        assert row is not None
        assert not _entity_entries(backend)

    @pytest.mark.asyncio
    async def test_a_page_loaded_under_one_scope_is_not_stored_after_a_change(self) -> None:
        repository = SwitchingRepository([Widget(id=ROW_A, name="a")])
        backend = CountingCacheBackend()
        wrapper = _wrap(repository, backend)

        page = await wrapper.list_paginated(PAGE)

        assert [row.id for row in page.items] == [ROW_A]
        assert not [key for key in backend.data if not key.startswith("tag:")]

    @pytest.mark.asyncio
    async def test_a_custom_read_under_one_scope_is_not_stored_after_a_change(self) -> None:
        repository = SwitchingRepository([Widget(id=ROW_A, name="a")])
        backend = CountingCacheBackend()
        wrapper = _wrap(repository, backend)

        assert await wrapper.all_names() == ["a"]
        assert not [key for key in backend.data if ":custom:" in key]

    @pytest.mark.asyncio
    async def test_the_scope_is_read_once_for_the_keys_of_a_page(self) -> None:
        repository = ScopedRepository([Widget(id=ROW_A, name="a")])
        repository.reads = 0
        wrapper = _wrap(repository, CountingCacheBackend())

        await wrapper.list_paginated(PAGE)

        # One read for the keys and one check before the writes.
        assert repository.reads == 2
