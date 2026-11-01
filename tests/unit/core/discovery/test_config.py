from __future__ import annotations

import pytest

from loom.core.config import ConfigError
from loom.core.discovery.config import DiscoveryConfig, decode_discovery, discover


def test_an_absent_mode_takes_the_default_the_reader_names() -> None:
    decoded = decode_discovery({}, default_mode="modules", strict=False)

    assert decoded.mode == "modules"


def test_a_declared_mode_wins_over_the_default() -> None:
    decoded = decode_discovery({"mode": "manifest"}, default_mode="modules", strict=False)

    assert decoded.mode == "manifest"


def test_a_lenient_reader_ignores_unknown_keys_at_every_level() -> None:
    section = {"typo": 1, "modules": {"include": ["app.jobs"], "extra": True}}

    decoded = decode_discovery(section, strict=False)

    assert decoded.modules.include == ["app.jobs"]


def test_a_strict_reader_refuses_an_unknown_key() -> None:
    with pytest.raises(ConfigError, match=r"app\.discovery"):
        decode_discovery({"typo": 1}, strict=True)


def test_a_section_that_is_not_a_mapping_fails_naming_its_path() -> None:
    with pytest.raises(ConfigError, match=r"app\.discovery"):
        decode_discovery(None, strict=False)


def test_an_unknown_mode_runs_no_engine() -> None:
    with pytest.raises(ValueError, match="Unsupported discovery mode: 'scan'"):
        discover(DiscoveryConfig(mode="scan"))
