"""Unit tests for storage locator implementations and coercion helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.storage._locator import (
    MappingLocator,
    PrefixLocator,
    TableLocation,
    _as_location,
    _as_locator,
)


class TestStorageLocators:
    def test_prefix_locator_resolves_dotted_refs_and_preserves_defaults(self) -> None:
        locator = PrefixLocator(
            root="/var/lib/loom/lake/",
            storage_options={"AWS_REGION": "eu-west-1"},
            writer={"compression": "SNAPPY"},
            target_file_size=268435456,
            delta_config={"delta.appendOnly": "true"},
            commit={"userName": "etl"},
        )

        location = locator.locate(TableRef("raw.orders"))

        assert location.uri == "/var/lib/loom/lake/raw/orders"
        assert location.storage_options == {"AWS_REGION": "eu-west-1"}
        assert location.writer == {"compression": "SNAPPY"}
        assert location.target_file_size == 268435456
        assert location.delta_config == {"delta.appendOnly": "true"}
        assert location.commit == {"userName": "etl"}

    def test_mapping_locator_uses_explicit_mapping_first(self) -> None:
        explicit = TableLocation(uri="s3://raw-account/orders/")
        locator = MappingLocator(mapping={"raw.orders": explicit})

        resolved = locator.locate(TableRef("raw.orders"))

        assert resolved is explicit

    def test_mapping_locator_uses_default_for_unmapped_refs(self) -> None:
        default = TableLocation(
            uri="s3://default-lake/",
            storage_options={"AWS_REGION": "eu-west-1"},
            writer={"compression": "ZSTD"},
            target_file_size=134217728,
        )
        locator = MappingLocator(mapping={}, default=default)

        resolved = locator.locate(TableRef("staging.daily"))

        assert resolved.uri == "s3://default-lake/staging/daily"
        assert resolved.storage_options == {"AWS_REGION": "eu-west-1"}
        assert resolved.writer == {"compression": "ZSTD"}
        assert resolved.target_file_size == 134217728

    def test_mapping_locator_raises_when_ref_is_missing_and_no_default(self) -> None:
        locator = MappingLocator(mapping={})

        table_ref = TableRef("raw.missing")
        with pytest.raises(KeyError, match="No storage location configured"):
            locator.locate(table_ref)

    def test_as_locator_coerces_pathlike_and_keeps_custom_locator(self) -> None:
        class _Locator:
            def locate(self, ref: TableRef) -> TableLocation:
                return TableLocation(uri=f"memory://{ref.ref}")

        custom = _Locator()
        assert _as_locator(custom) is custom

        coerced = _as_locator(Path("/var/lib/loom/lake"))
        assert isinstance(coerced, PrefixLocator)
        assert coerced.locate(TableRef("raw.orders")).uri == "/var/lib/loom/lake/raw/orders"

    def test_as_location_coerces_pathlike_and_keeps_table_location(self) -> None:
        existing = TableLocation(uri="s3://bucket/table")
        assert _as_location(existing) is existing

        coerced = _as_location(Path("/var/lib/loom/runs"))
        assert isinstance(coerced, TableLocation)
        assert coerced.uri == "/var/lib/loom/runs"
