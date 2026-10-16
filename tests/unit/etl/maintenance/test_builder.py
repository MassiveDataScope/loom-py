"""Tests for MaintainTable and MaintainSchema fluent builders."""

from __future__ import annotations

import pytest

from loom.etl.maintenance._builder import MaintainSchema, MaintainTable
from loom.etl.maintenance._ops import CompactSpec, VacuumSpec, ZOrderSpec
from loom.etl.storage._config import StorageConfig, StorageDefaults, TablePathConfig, TableRoute

# ---------------------------------------------------------------------------
# MaintainTable
# ---------------------------------------------------------------------------


class TestMaintainTable:
    def test_empty_ref_raises(self) -> None:
        with pytest.raises(ValueError, match="table_ref"):
            MaintainTable("")

    def test_vacuum_defaults_dry_run_true(self) -> None:
        spec = MaintainTable("raw.events").vacuum()._to_spec()
        assert spec.ops == (VacuumSpec(retention_hours=None, dry_run=True),)

    def test_vacuum_explicit_params(self) -> None:
        spec = MaintainTable("raw.events").vacuum(retention_hours=48, dry_run=False)._to_spec()
        assert spec.ops == (VacuumSpec(retention_hours=48, dry_run=False),)

    def test_vacuum_full(self) -> None:
        spec = MaintainTable("raw.events").vacuum(dry_run=False, full=True)._to_spec()
        assert spec.ops == (VacuumSpec(dry_run=False, full=True),)

    def test_compact_default(self) -> None:
        spec = MaintainTable("raw.events").compact()._to_spec()
        assert spec.ops == (CompactSpec(target_size=None),)

    def test_compact_with_target_size(self) -> None:
        spec = MaintainTable("raw.events").compact(target_size=128 * 1024 * 1024)._to_spec()
        assert spec.ops == (CompactSpec(target_size=128 * 1024 * 1024),)

    def test_z_order_by(self) -> None:
        spec = MaintainTable("raw.events").z_order_by(["date", "id"])._to_spec()
        assert spec.ops == (ZOrderSpec(columns=["date", "id"]),)

    def test_z_order_empty_columns_raises(self) -> None:
        table = MaintainTable("raw.events")
        with pytest.raises(ValueError, match="at least one column"):
            table.z_order_by([])

    def test_compact_and_z_order_mutually_exclusive(self) -> None:
        builder = MaintainTable("raw.events").compact().z_order_by(["date"])
        with pytest.raises(TypeError, match="mutually exclusive"):
            builder._to_spec()

    def test_chain_vacuum_and_compact(self) -> None:
        spec = (
            MaintainTable("raw.events")
            .vacuum(retention_hours=168, dry_run=False)
            .compact()
            ._to_spec()
        )
        assert spec.ops == (
            VacuumSpec(retention_hours=168, dry_run=False),
            CompactSpec(),
        )

    def test_table_ref_preserved(self) -> None:
        spec = MaintainTable("staging.events").vacuum()._to_spec()
        assert spec.table_ref == "staging.events"

    def test_repr(self) -> None:
        assert repr(MaintainTable("raw.events")) == "MaintainTable('raw.events')"


# ---------------------------------------------------------------------------
# MaintainSchema
# ---------------------------------------------------------------------------


class TestMaintainSchema:
    def test_empty_prefix_raises(self) -> None:
        with pytest.raises(ValueError, match="schema_prefix"):
            MaintainSchema("")

    def test_expand_matches_prefix(self) -> None:
        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri="/lake/")),
            tables=(
                TableRoute(name="raw.events", path=TablePathConfig(uri="/lake/raw/events")),
                TableRoute(name="raw.snapshots", path=TablePathConfig(uri="/lake/raw/snapshots")),
                TableRoute(name="staging.events", path=TablePathConfig(uri="/lake/staging/events")),
            ),
        )
        specs = MaintainSchema("raw").vacuum()._expand(config)
        refs = [s.table_ref for s in specs]
        assert refs == ["raw.events", "raw.snapshots"]

    def test_expand_no_match_returns_empty(self) -> None:
        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri="/lake/")),
            tables=(
                TableRoute(name="staging.events", path=TablePathConfig(uri="/lake/staging/events")),
            ),
        )
        specs = MaintainSchema("raw").vacuum()._expand(config)
        assert specs == []

    def test_expand_propagates_ops(self) -> None:
        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri="/lake/")),
            tables=(TableRoute(name="raw.events", path=TablePathConfig(uri="/lake/raw/events")),),
        )
        specs = (
            MaintainSchema("raw")
            .vacuum(retention_hours=72, dry_run=False)
            .compact()
            ._expand(config)
        )
        assert len(specs) == 1
        assert specs[0].ops == (
            VacuumSpec(retention_hours=72, dry_run=False),
            CompactSpec(),
        )

    def test_expand_propagates_full_vacuum(self) -> None:
        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri="/lake/")),
            tables=(TableRoute(name="raw.events", path=TablePathConfig(uri="/lake/raw/events")),),
        )
        specs = MaintainSchema("raw").vacuum(dry_run=False, full=True)._expand(config)
        assert specs[0].ops == (VacuumSpec(dry_run=False, full=True),)

    def test_compact_and_z_order_mutually_exclusive(self) -> None:
        config = StorageConfig(
            defaults=StorageDefaults(table_path=TablePathConfig(uri="/lake/")),
            tables=(TableRoute(name="raw.events", path=TablePathConfig(uri="/lake/raw/events")),),
        )
        builder = MaintainSchema("raw").compact().z_order_by(["date"])
        with pytest.raises(TypeError, match="mutually exclusive"):
            builder._expand(config)

    def test_repr(self) -> None:
        assert repr(MaintainSchema("raw")) == "MaintainSchema('raw')"
