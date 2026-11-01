"""Tests for IntoTable and IntoFile target declarations."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from loom.core.routing import LogicalRef
from loom.etl import col
from loom.etl.checkpoint import CheckpointScope
from loom.etl.declarative._format import Format
from loom.etl.declarative.expr._params import params
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import (
    IntoFile,
    IntoTable,
    IntoTemp,
    SchemaMode,
    UpsertDeleteFlagError,
)
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


class TestIntoTableModes:
    @pytest.mark.parametrize(
        "build,expected_type,expected_partitions,expect_predicate",
        [
            (lambda t: t, ReplaceSpec, (), False),
            (lambda t: t.append(), AppendSpec, (), False),
            (lambda t: t.replace(), ReplaceSpec, (), False),
            (
                lambda t: t.replace_partitions("year", "month"),
                ReplacePartitionsSpec,
                ("year", "month"),
                False,
            ),
            (
                lambda t: t.replace_matching("day"),
                ReplacePartitionsSpec,
                ("day",),
                False,
            ),
            (
                lambda t: t.replace_physical_partitions("year", "month"),
                ReplacePartitionsSpec,
                ("year", "month"),
                False,
            ),
            (
                lambda t: t.replace_partition(
                    year=params.run_date.year, month=params.run_date.month
                ),
                ReplaceWhereSpec,
                (),  # columns encoded in predicate, not in partition_cols
                True,
            ),
            (
                lambda t: t.replace_where(col("date") >= params.run_date),
                ReplaceWhereSpec,
                (),
                True,
            ),
            (
                lambda t: t.upsert(keys=("order_id",)),
                UpsertSpec,
                (),
                False,
            ),
            (
                lambda t: t.update(keys=("order_id",)),
                UpdateSpec,
                (),
                False,
            ),
        ],
    )
    def test_into_table_mode_contract(
        self,
        build: Callable[[IntoTable], IntoTable],
        expected_type: type,
        expected_partitions: tuple[str, ...],
        expect_predicate: bool,
    ) -> None:
        spec = build(IntoTable("staging.orders"))._to_spec()
        assert isinstance(spec, expected_type)
        assert getattr(spec, "partition_cols", ()) == expected_partitions
        assert (getattr(spec, "replace_predicate", None) is not None) is expect_predicate
        if isinstance(spec, (UpsertSpec, UpdateSpec)):
            assert spec.upsert_keys == ("order_id",)

    def test_replace_physical_partitions_sets_the_flag(self) -> None:
        spec = IntoTable("staging.orders").replace_physical_partitions("year")._to_spec()
        assert isinstance(spec, ReplacePartitionsSpec)
        assert spec.require_physical is True

    def test_replace_matching_does_not_set_the_flag(self) -> None:
        spec = IntoTable("staging.orders").replace_matching("day")._to_spec()
        assert isinstance(spec, ReplacePartitionsSpec)
        assert spec.require_physical is False

    def test_replace_physical_partitions_raises_when_no_cols(self) -> None:
        table = IntoTable("staging.orders")
        with pytest.raises(ValueError, match="at least one partition column"):
            table.replace_physical_partitions()

    def test_replace_matching_raises_when_no_cols(self) -> None:
        table = IntoTable("staging.orders")
        with pytest.raises(ValueError, match="at least one column"):
            table.replace_matching()

    def test_replace_partitions_raises_when_no_cols(self) -> None:
        table = IntoTable("staging.orders")
        with pytest.raises(ValueError, match="at least one partition column"):
            table.replace_partitions()

    def test_replace_partition_raises_when_no_pairs(self) -> None:
        table = IntoTable("staging.orders")
        with pytest.raises(ValueError, match="at least one column=value pair"):
            table.replace_partition()

    def test_write_methods_return_new_instance(self) -> None:
        base = IntoTable("staging.orders")
        appended = base.append()
        assert appended is not base
        assert isinstance(base._to_spec(), ReplaceSpec)

    @pytest.mark.parametrize(
        "table_ref,expected",
        [
            ("staging.orders", TableRef("staging.orders")),
            (LogicalRef("staging.orders"), TableRef("staging.orders")),
            (TableRef("staging.orders"), TableRef("staging.orders")),
        ],
    )
    def test_table_ref_normalization(
        self,
        table_ref: str | LogicalRef | TableRef,
        expected: TableRef,
    ) -> None:
        spec = IntoTable(table_ref)._to_spec()
        assert spec.table_ref == expected

    @pytest.mark.parametrize("schema", [SchemaMode.STRICT, SchemaMode.EVOLVE, SchemaMode.OVERWRITE])
    def test_replace_propagates_schema_mode(self, schema: SchemaMode) -> None:
        spec = IntoTable("staging.orders").replace(schema=schema)._to_spec()
        assert isinstance(spec, ReplaceSpec)
        assert spec.schema_mode is schema

    def test_replace_allows_an_empty_frame_by_default(self) -> None:
        assert IntoTable("staging.orders").replace()._to_spec().allow_empty is True
        assert IntoTable("staging.orders")._to_spec().allow_empty is True

    def test_replace_propagates_allow_empty(self) -> None:
        spec = IntoTable("staging.orders").replace(allow_empty=False)._to_spec()
        assert isinstance(spec, ReplaceSpec)
        assert spec.allow_empty is False

    @pytest.mark.parametrize(
        "declare",
        [
            lambda: IntoTable("staging.orders").append(allow_empty=False),  # type: ignore[call-arg]
            lambda: IntoTable("staging.orders").upsert(keys=("id",), allow_empty=False),  # type: ignore[call-arg]
        ],
    )
    def test_allow_empty_is_only_a_replace_option(self, declare: Callable[[], Any]) -> None:
        with pytest.raises(TypeError, match="allow_empty"):
            declare()

    @pytest.mark.parametrize("schema", [SchemaMode.STRICT, SchemaMode.EVOLVE, SchemaMode.OVERWRITE])
    def test_replace_partition_propagates_schema_mode(self, schema: SchemaMode) -> None:
        spec = (
            IntoTable("staging.orders")
            .replace_partition(
                schema=schema,
                year=params.run_date.year,
            )
            ._to_spec()
        )
        assert isinstance(spec, ReplaceWhereSpec)
        assert spec.schema_mode is schema

    def test_upsert_propagates_include_exclude_and_partitions(self) -> None:
        spec = (
            IntoTable("staging.orders")
            .upsert(
                keys=("order_id",),
                partition_cols=("year", "month"),
                exclude=("created_at",),
                include=("status",),
            )
            ._to_spec()
        )
        assert isinstance(spec, UpsertSpec)
        assert spec.upsert_keys == ("order_id",)
        assert spec.partition_cols == ("year", "month")
        assert spec.upsert_exclude == ("created_at",)
        assert spec.upsert_include == ("status",)

    def test_upsert_propagates_delete_flag(self) -> None:
        spec = IntoTable("staging.orders").upsert(keys=("id",), delete_flag="_deleted")._to_spec()
        assert isinstance(spec, UpsertSpec)
        assert spec.delete_flag == "_deleted"

    @pytest.mark.parametrize("flag", ["", "   "])
    def test_upsert_rejects_a_blank_delete_flag(self, flag: str) -> None:
        with pytest.raises(UpsertDeleteFlagError, match="must name a source column") as info:
            IntoTable("staging.orders").upsert(keys=("id",), delete_flag=flag)
        assert info.value.column == flag

    def test_update_propagates_include_exclude_and_partitions(self) -> None:
        spec = (
            IntoTable("staging.orders")
            .update(
                keys=("order_id",),
                partition_cols=("year", "month"),
                exclude=("created_at",),
                include=("status",),
            )
            ._to_spec()
        )
        assert isinstance(spec, UpdateSpec)
        assert spec.keys == ("order_id",)
        assert spec.partition_cols == ("year", "month")
        assert spec.exclude == ("created_at",)
        assert spec.include == ("status",)

    def test_repr_includes_mode(self) -> None:
        repr_value = repr(IntoTable("staging.orders").append())
        assert "IntoTable('staging.orders'" in repr_value
        assert "append" in repr_value


class TestIntoFile:
    @pytest.mark.parametrize(
        "target,expected_path,expected_format",
        [
            (
                IntoFile("s3://exports/report_{run_date}.csv", format=Format.CSV),
                "s3://exports/report_{run_date}.csv",
                Format.CSV,
            ),
            (
                IntoFile("s3://out/report.xlsx", format=Format.XLSX),
                "s3://out/report.xlsx",
                Format.XLSX,
            ),
        ],
    )
    def test_into_file_stores_path_and_format(
        self,
        target: IntoFile,
        expected_path: str,
        expected_format: Format,
    ) -> None:
        spec = target._to_spec()
        assert isinstance(spec, FileSpec)
        assert spec.path == expected_path
        assert spec.format is expected_format

    def test_is_alias_false_by_default(self) -> None:
        spec = IntoFile("s3://out/report.csv", format=Format.CSV)._to_spec()
        assert spec.is_alias is False

    def test_repr_includes_path_and_format(self) -> None:
        repr_value = repr(IntoFile("s3://out/report.xlsx", format=Format.XLSX))
        assert "s3://out/report.xlsx" in repr_value
        assert "xlsx" in repr_value


class TestIntoFileAlias:
    def test_alias_sets_is_alias_true(self) -> None:
        spec = IntoFile.alias("exports_daily", format=Format.PARQUET)._to_spec()
        assert spec.is_alias is True

    def test_alias_stores_name_as_path(self) -> None:
        spec = IntoFile.alias("exports_daily", format=Format.CSV)._to_spec()
        assert spec.path == "exports_daily"

    def test_alias_stores_format(self) -> None:
        spec = IntoFile.alias("reports", format=Format.CSV)._to_spec()
        assert spec.format is Format.CSV

    def test_alias_is_file_spec(self) -> None:
        spec = IntoFile.alias("exports_daily", format=Format.PARQUET)._to_spec()
        assert isinstance(spec, FileSpec)

    def test_direct_path_is_not_alias(self) -> None:
        spec = IntoFile("s3://bucket/out.parquet", format=Format.PARQUET)._to_spec()
        assert spec.is_alias is False


class TestIntoTemp:
    @pytest.mark.parametrize(
        "append,expected_type",
        [
            (False, TempSpec),
            (True, TempFanInSpec),
        ],
    )
    def test_to_spec_selects_variant(self, append: bool, expected_type: type[Any]) -> None:
        spec = IntoTemp("normalized", scope=CheckpointScope.CORRELATION, append=append)._to_spec()
        assert isinstance(spec, expected_type)
        assert spec.temp_name == "normalized"
        assert spec.temp_scope is CheckpointScope.CORRELATION

    def test_exposes_properties(self) -> None:
        target = IntoTemp("parts", scope=CheckpointScope.RUN, append=True)
        assert target.temp_name == "parts"
        assert target.scope is CheckpointScope.RUN
        assert target.append is True

    def test_repr_includes_name_scope_and_append(self) -> None:
        repr_value = repr(IntoTemp("parts", scope=CheckpointScope.RUN, append=True))
        assert "IntoTemp('parts'" in repr_value
        assert "append=True" in repr_value
