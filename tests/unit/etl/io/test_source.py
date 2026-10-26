"""Tests for FromTable, FromFile, Sources, and SourceSet."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.core.routing import LogicalRef
from loom.etl.declarative._format import Format
from loom.etl.declarative.expr._params import params
from loom.etl.declarative.expr._refs import TableRef, col
from loom.etl.declarative.source import (
    FromClickHouse,
    FromFile,
    FromTable,
    FromTemp,
    SourceKind,
    Sources,
    SourceSet,
)
from loom.etl.schema._schema import ColumnSchema, LoomDtype


@pytest.fixture
def table_schema() -> tuple[ColumnSchema, ...]:
    return (ColumnSchema("id", LoomDtype.INT64),)


class TestFromTable:
    @pytest.mark.parametrize(
        "ref,expected_ref,is_same_instance",
        [
            ("raw.orders", "raw.orders", False),
            (TableRef("raw.orders"), "raw.orders", True),
            (LogicalRef("raw.orders"), "raw.orders", False),
        ],
    )
    def test_ref_normalization(
        self,
        ref: str | LogicalRef | TableRef,
        expected_ref: str,
        is_same_instance: bool,
    ) -> None:
        src = FromTable(ref)
        assert src.table_ref.ref == expected_ref
        if is_same_instance:
            assert src.table_ref is ref

    def test_default_state(self) -> None:
        src = FromTable("raw.orders")
        spec = src._to_spec("orders")
        assert src.predicates == ()
        assert spec.predicates == ()
        assert spec.columns == ()

    @pytest.mark.parametrize(
        "builder,check",
        [
            (
                lambda s: s.where(col("year") == 2024),
                lambda spec: len(spec.predicates) == 1 and spec.columns == (),
            ),
            (
                lambda s: s.where(
                    col("year") == params.run_date.year,
                    col("month") == params.run_date.month,
                ),
                lambda spec: len(spec.predicates) == 2,
            ),
            (
                lambda s: s.columns("id", "amount"),
                lambda spec: spec.columns == ("id", "amount") and spec.predicates == (),
            ),
            (
                lambda s: s.where(col("year") == 2024).columns("id", "amount"),
                lambda spec: len(spec.predicates) == 1 and spec.columns == ("id", "amount"),
            ),
            (
                lambda s: s.columns("id", "amount").where(col("year") == 2024),
                lambda spec: len(spec.predicates) == 1 and spec.columns == ("id", "amount"),
            ),
            (
                lambda s: s.with_schema((ColumnSchema("id", LoomDtype.INT64),)),
                lambda spec: spec.schema == (ColumnSchema("id", LoomDtype.INT64),),
            ),
        ],
    )
    def test_transformers_return_new_and_preserve_state(
        self,
        builder: Callable[[FromTable], FromTable],
        check: Callable[[object], bool],
    ) -> None:
        src = FromTable("raw.orders")
        mutated = builder(src)
        assert mutated is not src
        assert check(mutated._to_spec("orders"))

    def test_to_spec_table_kind_defaults(self) -> None:
        spec = FromTable("raw.orders")._to_spec("orders")
        assert spec.alias == "orders"
        assert spec.kind is SourceKind.TABLE
        assert spec.format is Format.DELTA
        assert spec.table_ref == TableRef("raw.orders")

    def test_columns_empty_raises(self) -> None:
        table = FromTable("raw.orders")
        with pytest.raises(ValueError, match="at least one"):
            table.columns()

    def test_columns_preserves_schema(self, table_schema: tuple[ColumnSchema, ...]) -> None:
        spec = FromTable("raw.orders").with_schema(table_schema).columns("id")._to_spec("orders")
        assert spec.schema == table_schema
        assert spec.columns == ("id",)


class TestFromFile:
    @pytest.mark.parametrize(
        "path,format",
        [
            ("s3://raw/report_{run_date}.xlsx", Format.XLSX),
            ("s3://raw/data.csv", Format.CSV),
        ],
    )
    def test_basic_fields(self, path: str, format: Format) -> None:
        src = FromFile(path, format=format)
        assert src.path == path
        assert src.format is format

    def test_to_spec_file_kind(self) -> None:
        spec = FromFile("s3://raw/data.csv", format=Format.CSV)._to_spec("data")
        assert spec.alias == "data"
        assert spec.kind is SourceKind.FILE
        assert spec.format is Format.CSV
        assert spec.path == "s3://raw/data.csv"

    def test_to_spec_is_alias_false_by_default(self) -> None:
        spec = FromFile("s3://raw/data.csv", format=Format.CSV)._to_spec("data")
        assert spec.is_alias is False

    @pytest.mark.parametrize(
        "builder,expected_columns",
        [
            (
                lambda s: s.columns("order_id", "amount"),
                ("order_id", "amount"),
            ),
            (
                lambda s: s.columns("id", "amount").with_schema(
                    (ColumnSchema("id", LoomDtype.INT64),)
                ),
                ("id", "amount"),
            ),
        ],
    )
    def test_columns_variants(
        self,
        builder: Callable[[FromFile], FromFile],
        expected_columns: tuple[str, ...],
    ) -> None:
        base = FromFile("s3://raw/data.csv", format=Format.CSV)
        spec = builder(base)._to_spec("data")
        assert spec.columns == expected_columns

    def test_default_columns_empty(self) -> None:
        spec = FromFile("s3://raw/data.csv", format=Format.CSV)._to_spec("data")
        assert spec.columns == ()

    def test_columns_empty_raises(self) -> None:
        file = FromFile("s3://raw/data.csv", format=Format.CSV)
        with pytest.raises(ValueError, match="at least one"):
            file.columns()


class TestFromFileAlias:
    def test_alias_sets_is_alias_true(self) -> None:
        spec = FromFile.alias("events_raw", format=Format.CSV)._to_spec("events_raw")
        assert spec.is_alias is True

    def test_alias_stores_name_as_path(self) -> None:
        spec = FromFile.alias("events_raw", format=Format.CSV)._to_spec("events_raw")
        assert spec.path == "events_raw"

    def test_alias_stores_format(self) -> None:
        spec = FromFile.alias("reports", format=Format.PARQUET)._to_spec("reports")
        assert spec.format is Format.PARQUET

    def test_alias_kind_is_file(self) -> None:
        spec = FromFile.alias("events_raw", format=Format.CSV)._to_spec("events_raw")
        assert spec.kind is SourceKind.FILE

    def test_direct_path_is_not_alias(self) -> None:
        spec = FromFile("s3://bucket/data.csv", format=Format.CSV)._to_spec("data")
        assert spec.is_alias is False

    def test_alias_columns_preserved(self) -> None:
        src = FromFile.alias("events_raw", format=Format.CSV).columns("id", "ts")
        spec = src._to_spec("events_raw")
        assert spec.is_alias is True
        assert spec.columns == ("id", "ts")


class TestFromTemp:
    def test_to_spec_temp_kind(self) -> None:
        spec = FromTemp("normalized")._to_spec("normalized")
        assert spec.alias == "normalized"
        assert spec.kind is SourceKind.TEMP
        assert spec.temp_name == "normalized"

    def test_repr_includes_temp_name(self) -> None:
        assert repr(FromTemp("orders_tmp")) == "FromTemp('orders_tmp')"


class TestSources:
    def test_aliases_and_specs_keep_order(self) -> None:
        sources = Sources(orders=FromTable("raw.orders"), customers=FromTable("raw.customers"))
        assert sources.aliases == ("orders", "customers")
        specs = sources._to_specs()
        assert [spec.alias for spec in specs] == ["orders", "customers"]

    def test_clickhouse_source_is_supported(self) -> None:
        sources = Sources(events=FromClickHouse("analytics.cdc_events").unbounded())
        specs = sources._to_specs()
        assert specs[0].kind is SourceKind.CLICKHOUSE
        assert specs[0].table_ref.ref == "analytics.cdc_events"

    def test_repr_lists_aliases(self) -> None:
        sources = Sources(orders=FromTable("raw.orders"), customers=FromTable("raw.customers"))
        assert repr(sources) == "Sources(orders, customers)"


class TestSourceSet:
    def test_collects_declared_sources(self) -> None:
        class OrderSources(SourceSet[object]):
            orders = FromTable("raw.orders")
            customers = FromTable("raw.customers")

        aliases = {spec.alias for spec in OrderSources()._to_specs()}
        assert aliases == {"orders", "customers"}

    def test_extended_behaviors(self) -> None:
        class Base(SourceSet[object]):
            orders = FromTable("raw.orders")

        extended = Base.extended(customers=FromTable("raw.customers"))
        aliases = {spec.alias for spec in extended._to_specs()}
        assert aliases == {"orders", "customers"}
        assert isinstance(extended, SourceSet)
        assert extended._sources is not Base._sources

    def test_clickhouse_sources_are_collected(self) -> None:
        class Base(SourceSet[object]):
            events = FromClickHouse("analytics.cdc_events").unbounded()

        aliases = {spec.alias for spec in Base()._to_specs()}
        assert aliases == {"events"}

    def test_extended_conflict_raises(self) -> None:
        class Base(SourceSet[object]):
            orders = FromTable("raw.orders")

        orders_v2 = FromTable("raw.orders_v2")
        with pytest.raises(ValueError, match="conflicting source names"):
            Base.extended(orders=orders_v2)

    def test_subclassing_concrete_source_set_raises(self) -> None:
        class Base(SourceSet[object]):
            orders = FromTable("raw.orders")

        with pytest.raises(TypeError, match="already a concrete SourceSet"):

            class Child(Base):
                customers = FromTable("raw.customers")

            _ = Child

    def test_repr_lists_aliases(self) -> None:
        class OrderSources(SourceSet[object]):
            orders = FromTable("raw.orders")
            customers = FromFile("s3://raw/customers.csv", format=Format.CSV)

        assert repr(OrderSources()) == "SourceSet(orders, customers)"
