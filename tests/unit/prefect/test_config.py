"""Tests for loom.prefect._config.FlowConfig and _load_flow_config.

Verifies:
- FlowConfig is a frozen msgspec.Struct with correct defaults.
- FlowConfig fields: flow_retries (int=2), flow_retry_delay_seconds (int=60).
- FlowConfig is immutable (frozen=True).
- _load_flow_config() returns a FlowConfig from a valid YAML file.
- _load_flow_config() raises KeyError when flow_name is not found in YAML.
- _load_flow_config() picks the correct flow section from multi-flow YAML.
- _load_flow_config() reads only the flows.<flow_name> section, ignoring others.
- flow_config_from_mapping() resolves a policy declared beside the schedule.
- flow_config_from_mapping() refuses shapes Prefect drops at run time.
"""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from loom.core.config.errors import ConfigError
from loom.prefect._config import FlowConfig, _load_flow_config, flow_config_from_mapping

# ---------------------------------------------------------------------------
# _load_flow_config tests
# ---------------------------------------------------------------------------


@pytest.fixture
def yaml_file(tmp_path: Path) -> Path:
    """Create a temp YAML file with two flow configs."""
    content = textwrap.dedent("""\
        flows:
          my_etl:
            flow_retries: 3
            flow_retry_delay_seconds: 90
          other_etl:
            flow_retries: 1
            flow_retry_delay_seconds: 30
    """)
    config_file = tmp_path / "flows.yaml"
    config_file.write_text(content)
    return config_file


class TestFlowConfig:
    # ---------------------------------------------------------------------------
    # FlowConfig struct tests
    # ---------------------------------------------------------------------------

    def test_default_values(self) -> None:
        """FlowConfig has correct default values for all fields."""
        config = FlowConfig()
        assert config.flow_retries == 2
        assert config.flow_retry_delay_seconds == 60

    def test_accepts_explicit_values(self) -> None:
        """FlowConfig accepts explicit values for all fields."""
        config = FlowConfig(flow_retries=5, flow_retry_delay_seconds=120)
        assert config.flow_retries == 5
        assert config.flow_retry_delay_seconds == 120

    def test_is_immutable(self) -> None:
        """FlowConfig must be frozen (immutable)."""
        config = FlowConfig()
        with pytest.raises((TypeError, AttributeError)):
            config.flow_retries = 99  # type: ignore[misc]

    def test_partial_overrides_keep_defaults(self) -> None:
        """FlowConfig allows partial overrides; non-specified fields keep defaults."""
        config = FlowConfig(flow_retry_delay_seconds=30)
        assert config.flow_retries == 2
        assert config.flow_retry_delay_seconds == 30


class TestLoadFlowConfig:
    def test_reads_the_specified_flow(self, yaml_file: Path) -> None:
        """_load_flow_config returns config for the specified flow name."""
        config = _load_flow_config(str(yaml_file), "my_etl")
        assert config.flow_retries == 3
        assert config.flow_retry_delay_seconds == 90

    def test_reads_the_second_flow(self, yaml_file: Path) -> None:
        """_load_flow_config can load the second flow from a multi-flow YAML."""
        config = _load_flow_config(str(yaml_file), "other_etl")
        assert config.flow_retries == 1
        assert config.flow_retry_delay_seconds == 30

    def test_returns_a_flow_config_instance(self, yaml_file: Path) -> None:
        """_load_flow_config returns a FlowConfig instance."""
        config = _load_flow_config(str(yaml_file), "my_etl")
        assert isinstance(config, FlowConfig)

    def test_raises_key_error_when_the_flow_is_not_found(self, yaml_file: Path) -> None:
        """_load_flow_config raises KeyError when flow_name is not in YAML."""
        with pytest.raises(KeyError):
            _load_flow_config(str(yaml_file), "nonexistent_flow")

    def test_uses_defaults_for_missing_keys(self, tmp_path: Path) -> None:
        """_load_flow_config uses FlowConfig defaults for missing optional keys."""
        content = textwrap.dedent("""\
            flows:
              minimal_etl:
                flow_retries: 1
        """)
        config_file = tmp_path / "minimal.yaml"
        config_file.write_text(content)

        config = _load_flow_config(str(config_file), "minimal_etl")
        assert config.flow_retries == 1
        # Non-specified keys must fall back to FlowConfig defaults
        assert config.flow_retry_delay_seconds == 60

    def test_raises_when_the_flows_key_is_missing(self, tmp_path: Path) -> None:
        """_load_flow_config raises KeyError when 'flows' top-level key is absent."""
        content = textwrap.dedent("""\
            settings:
              debug: true
        """)
        config_file = tmp_path / "no_flows.yaml"
        config_file.write_text(content)

        with pytest.raises(KeyError):
            _load_flow_config(str(config_file), "any_flow")


class TestFlowConfigFromMapping:
    def test_falls_back_to_defaults(self) -> None:
        assert flow_config_from_mapping(None) == FlowConfig()
        assert flow_config_from_mapping({}) == FlowConfig()

    def test_reads_an_hour_scale_delay(self) -> None:
        cfg = flow_config_from_mapping({"flow_retries": 3, "flow_retry_delay_seconds": 7200})
        assert cfg.flow_retries == 3
        assert cfg.flow_retry_delay_seconds == 7200

    def test_rejects_a_list_of_delays(self) -> None:
        with pytest.raises(TypeError, match="single number"):
            flow_config_from_mapping({"flow_retry_delay_seconds": [5400, 12600]})

    def test_rejects_a_negative_delay(self) -> None:
        with pytest.raises(ConfigError, match="negative"):
            flow_config_from_mapping({"flow_retry_delay_seconds": -1})
