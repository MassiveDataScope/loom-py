"""Tests for schema primitives and InMemoryCatalog behavior."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.target import IntoTable, SchemaMode
from loom.etl.declarative.target._table import (
    AppendSpec,
    ReplacePartitionsSpec,
    ReplaceSpec,
    UpsertSpec,
)
from loom.etl.schema._schema import ColumnSchema, LoomDtype
from loom.etl.testing import InMemoryCatalog

ORDERS_SCHEMA = (
    ColumnSchema("order_id", LoomDtype.INT64, nullable=False),
    ColumnSchema("amount", LoomDtype.FLOAT64),
    ColumnSchema("year", LoomDtype.INT32, nullable=False),
)


class TestLoomDtype:
    @pytest.mark.parametrize(
        "dtype,expected",
        [
            (LoomDtype.INT64, "Int64"),
            (LoomDtype.UTF8, "Utf8"),
            (LoomDtype.DATE, "Date"),
            (LoomDtype.BOOLEAN, "Boolean"),
            (LoomDtype.FLOAT32, "Float32"),
            (LoomDtype.FLOAT64, "Float64"),
            (LoomDtype.DECIMAL, "Decimal"),
        ],
    )
    def test_value_contract(self, dtype: LoomDtype, expected: str) -> None:
        assert dtype == expected

    @pytest.mark.parametrize(
        "member",
        [
            "INT8",
            "INT16",
            "INT32",
            "INT64",
            "UINT8",
            "UINT16",
            "UINT32",
            "UINT64",
            "DATE",
            "DATETIME",
            "DURATION",
            "TIME",
        ],
    )
    def test_expected_members_present(self, member: str) -> None:
        assert hasattr(LoomDtype, member)


class TestColumnSchema:
    def test_field_storage_and_default_nullable(self) -> None:
        strict_col = ColumnSchema("order_id", LoomDtype.INT64, nullable=False)
        default_col = ColumnSchema("amount", LoomDtype.FLOAT64)
        assert strict_col.name == "order_id"
        assert strict_col.dtype is LoomDtype.INT64
        assert strict_col.nullable is False
        assert default_col.nullable is True

    def test_frozen(self) -> None:
        col = ColumnSchema("x", LoomDtype.UTF8)
        with pytest.raises((AttributeError, TypeError)):
            col.name = "y"  # type: ignore[misc]

    def test_equality_inequality_and_hash(self) -> None:
        a = ColumnSchema("x", LoomDtype.INT32)
        b = ColumnSchema("x", LoomDtype.INT32)
        c = ColumnSchema("x", LoomDtype.INT64)
        assert a == b
        assert a != c
        assert hash(a) == hash(b)
        assert {a, b} == {a}


class TestSchemaModeAndIntoTable:
    def test_schema_mode_values(self) -> None:
        assert SchemaMode.STRICT.value == "strict"
        assert SchemaMode.EVOLVE.value == "evolve"
        assert SchemaMode.OVERWRITE.value == "overwrite"

    @pytest.mark.parametrize(
        "build,expected_type,expected_schema",
        [
            (lambda t: t.replace(), ReplaceSpec, SchemaMode.STRICT),
            (lambda t: t.replace(schema=SchemaMode.EVOLVE), ReplaceSpec, SchemaMode.EVOLVE),
            (
                lambda t: t.replace(schema=SchemaMode.OVERWRITE),
                ReplaceSpec,
                SchemaMode.OVERWRITE,
            ),
            (lambda t: t.append(schema=SchemaMode.EVOLVE), AppendSpec, SchemaMode.EVOLVE),
            (
                lambda t: t.replace_partitions("year", schema=SchemaMode.EVOLVE),
                ReplacePartitionsSpec,
                SchemaMode.EVOLVE,
            ),
            (
                lambda t: t.upsert(keys=("order_id",), schema=SchemaMode.EVOLVE),
                UpsertSpec,
                SchemaMode.EVOLVE,
            ),
        ],
    )
    def test_into_table_write_and_schema_modes(
        self,
        build: Callable[[IntoTable], IntoTable],
        expected_type: type,
        expected_schema: SchemaMode,
    ) -> None:
        spec = build(IntoTable("staging.orders"))._to_spec()
        assert isinstance(spec, expected_type)
        assert spec.schema_mode is expected_schema

    def test_into_table_mode_calls_are_immutable(self) -> None:
        base = IntoTable("staging.orders")
        strict = base.replace()
        evolve = base.replace(schema=SchemaMode.EVOLVE)
        assert strict is not evolve
        assert strict._to_spec().schema_mode is SchemaMode.STRICT
        assert evolve._to_spec().schema_mode is SchemaMode.EVOLVE


class TestInMemoryCatalog:
    @pytest.mark.parametrize(
        "ref,expected",
        [
            ("raw.orders", ORDERS_SCHEMA),
            ("raw.missing", None),
        ],
    )
    def test_schema_lookup(self, ref: str, expected: tuple[ColumnSchema, ...] | None) -> None:
        catalog = InMemoryCatalog(schemas={"raw.orders": ORDERS_SCHEMA})
        assert catalog.schema(TableRef(ref)) == expected

    def test_update_schema_existing_and_new_tables(self) -> None:
        catalog = InMemoryCatalog(schemas={"staging.out": ()})
        new_schema = (ColumnSchema("id", LoomDtype.INT64),)
        catalog.update_schema(TableRef("staging.out"), new_schema)
        assert catalog.schema(TableRef("staging.out")) == new_schema

        catalog.update_schema(TableRef("new.table"), new_schema)
        assert catalog.exists(TableRef("new.table"))
        assert catalog.schema(TableRef("new.table")) == new_schema

    def test_columns_and_exists_derived_from_schema(self) -> None:
        catalog = InMemoryCatalog(schemas={"raw.orders": ORDERS_SCHEMA})
        assert catalog.columns(TableRef("raw.orders")) == ("order_id", "amount", "year")
        assert catalog.exists(TableRef("raw.orders"))
        assert not catalog.exists(TableRef("raw.missing"))

    def test_tables_are_materialized_as_unknown_dtype_schema(self) -> None:
        catalog = InMemoryCatalog(tables={"raw.orders": ("id", "amount")})
        schema = catalog.schema(TableRef("raw.orders"))
        assert schema is not None
        assert [col.name for col in schema] == ["id", "amount"]

    def test_schemas_override_tables_on_same_ref(self) -> None:
        catalog = InMemoryCatalog(
            tables={"raw.orders": ("id",)},
            schemas={"raw.orders": ORDERS_SCHEMA},
        )
        assert catalog.schema(TableRef("raw.orders")) == ORDERS_SCHEMA

    def test_schema_updates_visible_to_subsequent_reads(self) -> None:
        catalog = InMemoryCatalog(schemas={"staging.out": ()})
        evolved = ORDERS_SCHEMA + (ColumnSchema("region", LoomDtype.UTF8),)
        catalog.update_schema(TableRef("staging.out"), evolved)
        assert catalog.schema(TableRef("staging.out")) == evolved
