"""Unit tests for ``CacheGateway.apply_config`` validation and translation."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, cast
from unittest.mock import patch

import pytest

from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.gateway import CacheGateway
from loom.core.config.errors import ConfigError

_MEMORY = {"cache": "aiocache.SimpleMemoryCache"}
_BOUNDED = "loom.core.cache.memory.BoundedMemoryCache"


class TestApplyConfigAliasValidation:
    def test_a_declared_alias_missing_from_the_config_is_refused(self) -> None:
        config = CacheConfig(
            aiocache_alias="sessions",
            counter_alias="counters",
            aiocache_config={"sessions": dict(_MEMORY)},
        )
        with pytest.raises(ConfigError, match="'counters'"):
            CacheGateway.apply_config(config)

    def test_the_refusal_happens_before_aiocache_is_touched(self) -> None:
        config = CacheConfig(aiocache_alias="sessions", aiocache_config={})
        with (
            patch.object(CacheGateway, "configure") as configure,
            pytest.raises(ConfigError),
        ):
            CacheGateway.apply_config(config)
        configure.assert_not_called()

    def test_the_default_alias_keeps_its_sanctioned_fallback(self) -> None:
        with patch.object(CacheGateway, "configure") as configure:
            CacheGateway.apply_config(CacheConfig(aiocache_config={}))
        assert "default" in configure.call_args.args[0]


def _translated(config: CacheConfig) -> dict[str, Any]:
    """Return the raw mapping ``apply_config`` hands to ``configure``."""
    with patch.object(CacheGateway, "configure") as configure:
        CacheGateway.apply_config(config)
    configure.assert_called_once()
    return cast(dict[str, Any], configure.call_args.args[0])


class TestApplyConfigAliasMessages:
    def test_the_data_alias_is_checked_before_the_counter_alias(self) -> None:
        config = CacheConfig(aiocache_alias="sessions", counter_alias="counters")
        expected = "Cache alias 'sessions' is declared but missing from 'aiocache_config'."
        with pytest.raises(ConfigError) as caught:
            CacheGateway.apply_config(config)
        assert str(caught.value) == expected

    def test_a_missing_counter_alias_names_the_counter_alias(self) -> None:
        config = CacheConfig(
            aiocache_alias="sessions",
            counter_alias="counters",
            aiocache_config={"sessions": dict(_MEMORY)},
        )
        expected = "Cache alias 'counters' is declared but missing from 'aiocache_config'."
        with pytest.raises(ConfigError) as caught:
            CacheGateway.apply_config(config)
        assert str(caught.value) == expected


class TestApplyConfigTranslation:
    def test_an_unbounded_memory_entry_is_copied_unchanged(self) -> None:
        entry = dict(_MEMORY)
        raw = _translated(CacheConfig(aiocache_alias="data", aiocache_config={"data": entry}))
        assert raw["data"] == _MEMORY
        assert raw["data"] is not entry

    def test_the_caller_entry_is_not_mutated(self) -> None:
        entry = dict(_MEMORY)
        _translated(CacheConfig(aiocache_alias="data", max_size=3, aiocache_config={"data": entry}))
        assert entry == _MEMORY

    def test_a_non_dict_entry_is_forwarded_as_the_same_object(self) -> None:
        entry = MappingProxyType(dict(_MEMORY))
        raw = _translated(
            CacheConfig(aiocache_alias="data", max_size=3, aiocache_config={"data": entry})
        )
        assert raw["data"] is entry
        assert raw["default"] == _MEMORY
        assert type(raw["default"]) is dict

    def test_a_global_max_size_bounds_a_memory_entry(self) -> None:
        raw = _translated(
            CacheConfig(aiocache_alias="data", max_size=3, aiocache_config={"data": dict(_MEMORY)})
        )
        assert raw["data"] == {"cache": _BOUNDED, "max_size": 3}

    def test_a_global_max_bytes_bounds_a_memory_entry(self) -> None:
        raw = _translated(
            CacheConfig(
                aiocache_alias="data", max_bytes=64, aiocache_config={"data": dict(_MEMORY)}
            )
        )
        assert raw["data"] == {"cache": _BOUNDED, "max_bytes": 64}

    def test_both_global_bounds_are_applied(self) -> None:
        config = CacheConfig(
            aiocache_alias="data",
            max_size=3,
            max_bytes=64,
            aiocache_config={"data": dict(_MEMORY)},
        )
        assert _translated(config)["data"] == {"cache": _BOUNDED, "max_size": 3, "max_bytes": 64}

    def test_an_entry_bound_alone_switches_the_class_without_adding_globals(self) -> None:
        config = CacheConfig(
            aiocache_alias="data", aiocache_config={"data": {**_MEMORY, "max_bytes": 9}}
        )
        assert _translated(config)["data"] == {"cache": _BOUNDED, "max_bytes": 9}

    def test_entry_bounds_win_over_global_bounds(self) -> None:
        config = CacheConfig(
            aiocache_alias="data",
            max_size=100,
            max_bytes=1000,
            aiocache_config={"data": {**_MEMORY, "max_size": 5, "max_bytes": 50}},
        )
        assert _translated(config)["data"] == {"cache": _BOUNDED, "max_size": 5, "max_bytes": 50}

    def test_the_memory_class_is_matched_by_substring(self) -> None:
        entry = {"cache": "aiocache.backends.memory.SimpleMemoryCache"}
        raw = _translated(
            CacheConfig(aiocache_alias="data", max_size=2, aiocache_config={"data": entry})
        )
        assert raw["data"] == {"cache": _BOUNDED, "max_size": 2}

    def test_a_bounded_non_memory_entry_is_left_alone(self) -> None:
        entry = {"cache": "aiocache.RedisCache", "max_size": 5}
        raw = _translated(
            CacheConfig(aiocache_alias="data", max_size=2, aiocache_config={"data": entry})
        )
        assert raw["data"] == {"cache": "aiocache.RedisCache", "max_size": 5}

    def test_a_bounded_entry_without_a_cache_class_is_left_alone(self) -> None:
        entry = {"max_size": 5}
        raw = _translated(CacheConfig(aiocache_alias="data", aiocache_config={"data": entry}))
        assert raw["data"] == {"max_size": 5}


class TestApplyConfigDefaultAlias:
    def test_a_missing_default_copies_the_translated_data_entry_last(self) -> None:
        config = CacheConfig(
            aiocache_alias="data",
            counter_alias="counters",
            max_size=4,
            aiocache_config={"data": dict(_MEMORY), "counters": dict(_MEMORY)},
        )
        raw = _translated(config)
        assert list(raw) == ["data", "counters", "default"]
        assert raw["default"] == {"cache": _BOUNDED, "max_size": 4}
        assert raw["default"] is not raw["data"]

    def test_an_empty_data_entry_falls_back_to_memory(self) -> None:
        raw = _translated(CacheConfig(aiocache_alias="data", aiocache_config={"data": {}}))
        assert raw == {"data": {}, "default": _MEMORY}

    def test_the_default_alias_without_config_gets_the_memory_fallback(self) -> None:
        assert _translated(CacheConfig()) == {"default": _MEMORY}

    def test_an_explicit_default_entry_is_kept_in_place(self) -> None:
        redis = {"cache": "aiocache.RedisCache"}
        config = CacheConfig(
            aiocache_alias="data",
            aiocache_config={"default": dict(redis), "data": dict(_MEMORY)},
        )
        raw = _translated(config)
        assert list(raw) == ["default", "data"]
        assert raw["default"] == redis
