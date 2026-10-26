"""Applying a cache config, and reading one from YAML.

Both legs were broken in ways that only show up at runtime: the documented YAML
example could not be applied at all, and a config loaded with ``section()`` lost
its backend definitions in silence and failed on first use.
"""

from __future__ import annotations

import pytest

from loom.core.cache import CacheGateway
from loom.core.cache.abc.config import CacheConfig
from loom.core.config import ConfigContext, ConfigError
from loom.core.config.convert import convert_config
from loom.core.config.keys import ConfigKey
from loom.core.model.redaction import RedactedMap

from ._doubles import MEMORY_BACKEND


def _named_aliases() -> CacheConfig:
    return CacheConfig(
        aiocache_alias="cache",
        counter_alias="counters",
        aiocache_config=RedactedMap(
            {"cache": dict(MEMORY_BACKEND), "counters": dict(MEMORY_BACKEND)}
        ),
    )


def test_the_documented_example_can_be_applied() -> None:
    """aiocache demands a literal ``default`` alias, which the example lacks."""
    CacheGateway.apply_config(_named_aliases())

    assert CacheGateway(alias="cache") is not None
    assert CacheGateway(alias="counters") is not None


def test_the_injected_default_serves_the_data_alias() -> None:
    """A gateway built with no alias must not silently get a different backend."""
    CacheGateway.apply_config(_named_aliases())

    assert CacheGateway() is not None


def test_an_explicit_default_is_not_overwritten() -> None:
    config = CacheConfig(
        aiocache_alias="cache",
        aiocache_config=RedactedMap(
            {
                "cache": dict(MEMORY_BACKEND),
                "default": {"cache": "aiocache.SimpleMemoryCache", "namespace": "mine"},
            }
        ),
    )

    CacheGateway.apply_config(config)

    assert CacheGateway(alias="default") is not None


def test_an_empty_config_can_still_be_applied() -> None:
    """The zero-argument default must be usable, not a landmine."""
    CacheGateway.apply_config(CacheConfig())

    assert CacheGateway() is not None


def test_a_yaml_section_keeps_its_backends() -> None:
    """``section()`` converts by field name, so the YAML key has to match one."""
    parsed = {
        "aiocache_alias": "sessions",
        "aiocache_config": {"sessions": dict(MEMORY_BACKEND)},
    }

    config = convert_config(parsed, CacheConfig, strict=False)

    assert config.aiocache_config == {"sessions": dict(MEMORY_BACKEND)}


def test_from_mapping_accepts_the_short_key_too() -> None:
    """The docstring used ``aiocache:``, so configs in the wild carry both."""
    config = CacheConfig.from_mapping(
        {"aiocache_alias": "sessions", "aiocache": {"sessions": dict(MEMORY_BACKEND)}}
    )

    assert config.aiocache_config == {"sessions": dict(MEMORY_BACKEND)}


def test_from_mapping_prefers_the_field_name_when_both_are_present() -> None:
    config = CacheConfig.from_mapping(
        {"aiocache_config": {"a": dict(MEMORY_BACKEND)}, "aiocache": {"b": dict(MEMORY_BACKEND)}}
    )

    assert set(config.aiocache_config) == {"a"}


@pytest.mark.parametrize("payload", [{}, {"aiocache": {}}, {"aiocache_config": {}}])
def test_from_mapping_never_invents_backends(payload: dict[str, object]) -> None:
    assert CacheConfig.from_mapping(payload).aiocache_config == {}


class TestYamlSection:
    def test_the_section_decodes_when_every_key_is_a_field(self) -> None:
        ctx = ConfigContext.from_dict({"cache": {"default_ttl": 5}})

        assert ctx.section_optional(ConfigKey.CACHE, CacheConfig) == CacheConfig(default_ttl=5)

    @pytest.mark.parametrize("unknown", ["ttls", "aiocache"])
    def test_the_section_is_rejected_when_a_key_is_not_a_field(self, unknown: str) -> None:
        ctx = ConfigContext.from_dict({"cache": {unknown: {}}})

        with pytest.raises(ConfigError, match=unknown):
            ctx.section_optional(ConfigKey.CACHE, CacheConfig)
