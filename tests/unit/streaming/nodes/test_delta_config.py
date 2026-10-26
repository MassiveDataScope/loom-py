"""Tests for Delta sink configuration parsing."""

from __future__ import annotations

import pytest

from loom.streaming.nodes._table import DeltaSinkConfig


def test_delta_sink_config_parses_staging_and_writer_properties() -> None:
    config = DeltaSinkConfig.from_config(
        {
            "uri": "s3://lake/cdc_events",
            "table": "cdc_events",
            "partition_by": ["year", "month"],
            "target_file_size": 134_217_728,
            "spool_max_bytes": 1_024,
            "staging": {
                "compression": "zstd",
            },
            "writer_properties": {
                "compression": "SNAPPY",
                "max_row_group_size": 256,
            },
        },
        default_table="cdc_events",
    )

    assert config.uri == "s3://lake/cdc_events"
    assert config.table == "cdc_events"
    assert config.partition_by == ("year", "month")
    assert config.target_file_size == 134_217_728
    assert config.spool_max_bytes == 1_024
    assert config.staging_compression == "zstd"
    assert config.writer_properties.compression == "SNAPPY"
    assert config.writer_properties.max_row_group_size == 256


def test_delta_sink_config_defaults_the_row_group_size() -> None:
    config = DeltaSinkConfig.from_config({"uri": "s3://lake/events"}, default_table="events")

    assert config.writer_properties.max_row_group_size == 10_000


def test_delta_sink_config_refuses_part_max_records() -> None:
    with pytest.raises(ValueError, match="unknown key 'part_max_records'"):
        DeltaSinkConfig.from_config(
            {"uri": "s3://lake/events", "part_max_records": 128}, default_table="events"
        )


def test_delta_sink_config_refuses_an_unknown_staging_key() -> None:
    with pytest.raises(ValueError, match="unknown key 'staging.compresion'"):
        DeltaSinkConfig.from_config(
            {"uri": "s3://lake/events", "staging": {"compresion": "zstd"}},
            default_table="events",
        )


@pytest.mark.parametrize("key", ["staging", "writer_properties"])
def test_delta_sink_config_refuses_a_section_key_that_is_not_a_mapping(key: str) -> None:
    with pytest.raises(ValueError, match=f"'{key}' must be a mapping"):
        DeltaSinkConfig.from_config(
            {"uri": "s3://lake/events", key: "zstd"}, default_table="events"
        )
