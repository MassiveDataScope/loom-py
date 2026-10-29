"""Tests for TargetSpec variant structs — frozen dataclasses, field contracts."""

from __future__ import annotations

import pytest

from loom.etl import col
from loom.etl.checkpoint import CheckpointScope
from loom.etl.declarative._format import Format
from loom.etl.declarative.expr._params import params
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import SchemaMode, TargetSpec
from loom.etl.declarative.target._file import FileSpec
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    ReplaceWhereSpec,
    UpdateSpec,
    UpsertSpec,
)
from loom.etl.declarative.target._temp import TempFanInSpec, TempSpec

_REF = TableRef("staging.out")
_PRED = col("year") == params.run_date.year


class TestTableVariants:
    def test_append_defaults(self) -> None:
        spec = AppendSpec(table_ref=_REF)
        assert spec.table_ref is _REF
        assert spec.schema_mode is SchemaMode.STRICT

    def test_replace_defaults(self) -> None:
        spec = ReplaceSpec(table_ref=_REF)
        assert spec.table_ref is _REF
        assert spec.schema_mode is SchemaMode.STRICT

    def test_replace_partitions_stores_cols(self) -> None:
        spec = ReplacePartitionsSpec(table_ref=_REF, partition_cols=("year", "month"))
        assert spec.partition_cols == ("year", "month")
        assert spec.schema_mode is SchemaMode.STRICT

    def test_replace_where_stores_predicate(self) -> None:
        spec = ReplaceWhereSpec(table_ref=_REF, replace_predicate=_PRED)
        assert spec.replace_predicate is _PRED

    def test_upsert_keys_and_defaults(self) -> None:
        spec = UpsertSpec(table_ref=_REF, upsert_keys=("id",))
        assert spec.upsert_keys == ("id",)
        assert spec.partition_cols == ()
        assert spec.upsert_exclude == ()
        assert spec.upsert_include == ()
        assert spec.delete_flag is None

    def test_update_keys_and_defaults(self) -> None:
        spec = UpdateSpec(table_ref=_REF, keys=("id",))
        assert spec.keys == ("id",)
        assert spec.partition_cols == ()
        assert spec.exclude == ()
        assert spec.include == ()

    def test_update_aliases_satisfy_merge_protocol(self) -> None:
        spec = UpdateSpec(table_ref=_REF, keys=("id",), exclude=("created_at",))
        assert spec.upsert_keys == spec.keys
        assert spec.upsert_exclude == spec.exclude
        assert spec.upsert_include == spec.include

    def test_upsert_include_exclude(self) -> None:
        spec = UpsertSpec(table_ref=_REF, upsert_keys=("id",), upsert_exclude=("created_at",))
        assert spec.upsert_exclude == ("created_at",)
        spec2 = UpsertSpec(table_ref=_REF, upsert_keys=("id",), upsert_include=("status",))
        assert spec2.upsert_include == ("status",)

    @pytest.mark.parametrize(
        "spec",
        [
            AppendSpec(table_ref=_REF),
            ReplaceSpec(table_ref=_REF),
            ReplacePartitionsSpec(table_ref=_REF, partition_cols=("year",)),
            ReplaceWhereSpec(table_ref=_REF, replace_predicate=_PRED),
            UpsertSpec(table_ref=_REF, upsert_keys=("id",)),
            UpdateSpec(table_ref=_REF, keys=("id",)),
        ],
    )
    def test_table_variants_are_frozen(self, spec: object) -> None:
        with pytest.raises((AttributeError, TypeError)):
            spec.table_ref = TableRef("other.table")  # type: ignore[union-attr]

    @pytest.mark.parametrize(
        "spec",
        [
            AppendSpec(table_ref=_REF),
            ReplaceSpec(table_ref=_REF),
            ReplacePartitionsSpec(table_ref=_REF, partition_cols=("year",)),
            ReplaceWhereSpec(table_ref=_REF, replace_predicate=_PRED),
            UpsertSpec(table_ref=_REF, upsert_keys=("id",)),
            UpdateSpec(table_ref=_REF, keys=("id",)),
            FileSpec(path="s3://bucket/out.csv", format=Format.CSV),
            TempSpec(temp_name="normalized", temp_scope=CheckpointScope.RUN),
            TempFanInSpec(temp_name="parts", temp_scope=CheckpointScope.RUN),
        ],
    )
    def test_all_variants_are_target_spec(self, spec: object) -> None:
        assert isinstance(spec, TargetSpec)  # type: ignore[arg-type]


class TestFileSpec:
    def test_stores_path_and_format(self) -> None:
        spec = FileSpec(path="s3://bucket/out.csv", format=Format.CSV)
        assert spec.path == "s3://bucket/out.csv"
        assert spec.format is Format.CSV
        assert spec.write_options is None

    def test_is_alias_defaults_false(self) -> None:
        spec = FileSpec(path="s3://bucket/out.csv", format=Format.CSV)
        assert spec.is_alias is False

    def test_is_alias_true_when_set(self) -> None:
        spec = FileSpec(path="exports_daily", format=Format.PARQUET, is_alias=True)
        assert spec.is_alias is True

    def test_frozen(self) -> None:
        spec = FileSpec(path="out.csv", format=Format.CSV)
        with pytest.raises((AttributeError, TypeError)):
            spec.path = "other.csv"  # type: ignore[misc]


class TestTempVariants:
    def test_temp_spec(self) -> None:
        spec = TempSpec(temp_name="norm", temp_scope=CheckpointScope.RUN)
        assert spec.temp_name == "norm"
        assert spec.temp_scope is CheckpointScope.RUN

    def test_temp_fan_in_spec(self) -> None:
        spec = TempFanInSpec(temp_name="parts", temp_scope=CheckpointScope.RUN)
        assert spec.temp_name == "parts"

    def test_temp_and_fan_in_are_distinct_types(self) -> None:
        assert type(TempSpec(temp_name="x", temp_scope=CheckpointScope.RUN)) is not type(
            TempFanInSpec(temp_name="x", temp_scope=CheckpointScope.RUN)
        )
