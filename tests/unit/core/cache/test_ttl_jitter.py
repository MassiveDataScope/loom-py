"""TTL jitter on every cache write.

A fixed TTL makes everything written in one burst expire at the same instant,
so the database sees a synchronised spike that re-synchronises on every refill
cycle.  Every TTL that reaches the backend is spread inside a band around the
configured value instead, while the configured value itself stays readable.
"""

from __future__ import annotations

import msgspec
import pytest

from loom.core.cache import CacheConfig
from loom.core.stores.repository import PageParams

from ._doubles import AcmeCacheWidget, CachedEnv, CountingRepository, wrap_with_cache

ENTITY_TTL = 100
LIST_TTL = 50
JITTER = 0.1
ROW_COUNT = 40


def _widgets(count: int) -> list[AcmeCacheWidget]:
    return [AcmeCacheWidget(id=index, name=f"w{index}") for index in range(1, count + 1)]


def _config(*, jitter: float, entity_ttl: int = ENTITY_TTL) -> CacheConfig:
    return CacheConfig(
        default_ttl=entity_ttl,
        default_list_ttl=LIST_TTL,
        ttl_jitter=jitter,
    )


def _env(*, jitter: float, entity_ttl: int = ENTITY_TTL) -> CachedEnv[AcmeCacheWidget]:
    return wrap_with_cache(
        CountingRepository(_widgets(ROW_COUNT), AcmeCacheWidget),
        _config(jitter=jitter, entity_ttl=entity_ttl),
    )


def _within(value: int | None, ttl: int, jitter: float) -> bool:
    assert value is not None
    return ttl * (1 - jitter) <= value <= ttl * (1 + jitter)


class TestJitteredBand:
    """Every TTL written lands inside the configured band, and never below one."""

    async def test_entity_writes_stay_inside_the_band(self) -> None:
        env = _env(jitter=JITTER)

        for widget in _widgets(ROW_COUNT):
            await env.wrapper.get_by_id(widget.id)

        assert len(env.backend.set_ttls) == ROW_COUNT
        assert all(_within(ttl, ENTITY_TTL, JITTER) for ttl in env.backend.set_ttls)

    async def test_list_index_and_entity_batch_stay_inside_their_bands(self) -> None:
        env = _env(jitter=JITTER)

        await env.wrapper.list_paginated(PageParams(page=1, limit=ROW_COUNT))

        assert all(_within(ttl, LIST_TTL, JITTER) for ttl in env.backend.set_ttls)
        assert all(_within(ttl, ENTITY_TTL, JITTER) for ttl in env.backend.multi_set_ttls)

    async def test_a_jittered_ttl_is_never_zero_or_negative(self) -> None:
        """A one-second TTL with a wide band must not round down to expiry."""
        env = _env(jitter=0.9, entity_ttl=1)

        for widget in _widgets(ROW_COUNT):
            await env.wrapper.get_by_id(widget.id)

        assert env.backend.set_ttls
        assert all(ttl is not None and ttl >= 1 for ttl in env.backend.set_ttls)

    async def test_repeated_writes_do_not_all_get_the_same_ttl(self) -> None:
        env = _env(jitter=JITTER)

        for widget in _widgets(ROW_COUNT):
            await env.wrapper.get_by_id(widget.id)

        assert len(set(env.backend.set_ttls)) > 1


class TestNoJitter:
    """``ttl_jitter=0`` reproduces the fixed TTLs the wrapper wrote before."""

    async def test_entity_writes_use_the_configured_ttl(self) -> None:
        env = _env(jitter=0.0)

        for widget in _widgets(ROW_COUNT):
            await env.wrapper.get_by_id(widget.id)

        assert set(env.backend.set_ttls) == {ENTITY_TTL}

    async def test_list_writes_use_the_configured_ttls(self) -> None:
        env = _env(jitter=0.0)

        await env.wrapper.list_paginated(PageParams(page=1, limit=ROW_COUNT))

        assert set(env.backend.set_ttls) == {LIST_TTL}
        assert set(env.backend.multi_set_ttls) == {ENTITY_TTL}

    def test_the_resolved_ttls_stay_deterministic(self) -> None:
        """``ttl_for_*`` must keep returning the configured value verbatim."""
        config = _config(jitter=JITTER)

        assert config.ttl_for_single("widget") == ENTITY_TTL
        assert config.ttl_for_list("widget") == LIST_TTL


class TestJitterConfiguration:
    """The jitter is a configured value like any other, with a validated range."""

    def test_the_default_is_ten_percent(self) -> None:
        assert CacheConfig().ttl_jitter == 0.1

    def test_it_is_read_from_a_yaml_section(self) -> None:
        config = msgspec.convert({"ttl_jitter": 0.25}, CacheConfig)

        assert config.ttl_jitter == 0.25

    def test_from_mapping_reads_it_too(self) -> None:
        assert CacheConfig.from_mapping({"ttl_jitter": 0.25}).ttl_jitter == 0.25

    @pytest.mark.parametrize("value", [-0.1, 1.0, 1.5])
    def test_a_value_outside_the_unit_range_is_rejected(self, value: float) -> None:
        with pytest.raises(ValueError, match="ttl_jitter"):
            CacheConfig(ttl_jitter=value)

    @pytest.mark.parametrize("value", [-0.1, 1.0])
    def test_a_yaml_section_is_validated_too(self, value: float) -> None:
        """msgspec re-raises the ``__post_init__`` rejection as a decode error."""
        with pytest.raises(msgspec.ValidationError, match="ttl_jitter"):
            msgspec.convert({"ttl_jitter": value}, CacheConfig)
