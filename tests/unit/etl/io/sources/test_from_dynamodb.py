"""Tests for the FromDynamoDb builder and DynamoDbSourceSpec."""

from __future__ import annotations

import pytest

from loom.core.expr.nodes import AndExpr, EqExpr, InExpr
from loom.etl.declarative.expr import col
from loom.etl.declarative.source import ExtraFieldsMode, FromDynamoDb, SourceRef
from loom.etl.declarative.source._specs import DynamoDbSourceSpec, SourceKind
from loom.etl.schema._schema import ColumnSchema, LoomDtype
from tests.unit.etl.io.sources.doubles import OptionalStatusOrderDoc

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_basic_spec(self) -> None:
        spec = FromDynamoDb("orders")._to_spec("orders")
        assert isinstance(spec, DynamoDbSourceSpec)
        assert spec.alias == "orders"
        assert spec.table == "orders"
        assert spec.filter is None
        assert spec.projection is None
        assert spec.schema == ()
        assert spec.extra_fields_mode == "ignore"
        assert spec.batch_size == 1_000
        assert spec.limit is None
        assert spec.consistent_read is False
        assert spec.total_segments is None

    def test_kind_is_dynamodb(self) -> None:
        spec = FromDynamoDb("events")._to_spec("events")
        assert spec.kind == SourceKind.DYNAMODB

    @pytest.mark.parametrize(
        "table",
        [
            pytest.param("bad name!", id="invalid_table_name_raises"),
            pytest.param("ab", id="too_short_table_name_raises"),
        ],
    )
    def test_invalid_table_name_raises(self, table: str) -> None:
        with pytest.raises(ValueError, match="Invalid DynamoDB table"):
            FromDynamoDb(table)

    def test_too_long_table_name_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid DynamoDB table"):
            FromDynamoDb("t" * 256)

    def test_table_with_dot_hyphen_underscore_is_valid(self) -> None:
        spec = FromDynamoDb("prod.order-items_v2")._to_spec("src")
        assert spec.table == "prod.order-items_v2"


# ---------------------------------------------------------------------------
# .where()
# ---------------------------------------------------------------------------


class TestWhere:
    def test_where_sets_filter(self) -> None:
        spec = FromDynamoDb("orders").where(col("status") == "active")._to_spec("orders")
        assert isinstance(spec.filter, EqExpr)

    def test_multiple_where_calls_and_predicates(self) -> None:
        spec = (
            FromDynamoDb("orders")
            .where(col("status") == "active")
            .where(col("year") == 2024)
            ._to_spec("orders")
        )
        assert isinstance(spec.filter, AndExpr)

    def test_where_with_source_ref_isin(self) -> None:
        from loom.etl.declarative.source._from import FromTemp

        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        spec = FromDynamoDb("orders").where(col("pk").isin(ref))._to_spec("orders")
        assert isinstance(spec.filter, InExpr)
        assert isinstance(spec.filter.values, SourceRef)

    def test_where_is_immutable(self) -> None:
        base = FromDynamoDb("orders")
        filtered = base.where(col("status") == "active")
        assert base._filter is None
        assert filtered._filter is not None


# ---------------------------------------------------------------------------
# .project()
# ---------------------------------------------------------------------------


class TestProject:
    def test_project_sets_projection(self) -> None:
        spec = FromDynamoDb("orders").project("pk", "status", "total")._to_spec("orders")
        assert spec.projection == ("pk", "status", "total")

    def test_project_rejects_empty_field(self) -> None:
        source = FromDynamoDb("orders")
        with pytest.raises(ValueError, match="non-empty"):
            source.project("pk", "")

    def test_project_rejects_blank_field(self) -> None:
        source = FromDynamoDb("orders")
        with pytest.raises(ValueError, match="non-empty"):
            source.project("   ")

    def test_project_is_immutable(self) -> None:
        base = FromDynamoDb("orders")
        projected = base.project("pk")
        assert base._projection is None
        assert projected._projection == ("pk",)


# ---------------------------------------------------------------------------
# .with_schema()
# ---------------------------------------------------------------------------


class TestWithSchema:
    def test_schema_stored_in_spec(self) -> None:
        spec = FromDynamoDb("orders").with_schema(OptionalStatusOrderDoc)._to_spec("orders")
        assert spec.schema == (
            ColumnSchema("id", LoomDtype.UTF8, nullable=True),
            ColumnSchema("status", LoomDtype.UTF8, nullable=True),
        )

    def test_with_schema_is_immutable(self) -> None:
        base = FromDynamoDb("orders")
        with_schema = base.with_schema(OptionalStatusOrderDoc)
        assert base._schema == ()
        assert with_schema._schema != ()


# ---------------------------------------------------------------------------
# .on_extra_fields()
# ---------------------------------------------------------------------------


class TestOnExtraFields:
    @pytest.mark.parametrize("mode", [*ExtraFieldsMode, "ignore", "warn", "capture", "error"])
    def test_a_mode_is_carried_as_its_member(self, mode: str) -> None:
        spec = FromDynamoDb("orders").on_extra_fields(mode)._to_spec("orders")
        assert spec.extra_fields_mode is ExtraFieldsMode(mode)

    def test_the_default_mode_is_ignore(self) -> None:
        assert FromDynamoDb("orders")._to_spec("orders").extra_fields_mode is ExtraFieldsMode.IGNORE

    def test_invalid_mode_raises(self) -> None:
        source = FromDynamoDb("orders")
        with pytest.raises(ValueError, match=r"not valid\. Choose one of: \['capture', 'error'"):
            source.on_extra_fields("bad")


# ---------------------------------------------------------------------------
# .batch_size()
# ---------------------------------------------------------------------------


class TestBatchSize:
    def test_default_batch_size(self) -> None:
        spec = FromDynamoDb("orders")._to_spec("orders")
        assert spec.batch_size == 1_000

    def test_custom_batch_size(self) -> None:
        spec = FromDynamoDb("orders").batch_size(500)._to_spec("orders")
        assert spec.batch_size == 500

    @pytest.mark.parametrize(
        "batch_size",
        [
            pytest.param(0, id="batch_size_zero_raises"),
            pytest.param(10001, id="batch_size_too_high_raises"),
        ],
    )
    def test_batch_size_out_of_range_raises(self, batch_size: int) -> None:
        source = FromDynamoDb("orders")
        with pytest.raises(ValueError, match="between 1 and 10000"):
            source.batch_size(batch_size)

    def test_batch_size_boundary_values(self) -> None:
        assert FromDynamoDb("orders").batch_size(1)._batch_size == 1
        assert FromDynamoDb("orders").batch_size(10_000)._batch_size == 10_000


# ---------------------------------------------------------------------------
# .limit()
# ---------------------------------------------------------------------------


class TestLimit:
    def test_limit_stored_in_spec(self) -> None:
        spec = FromDynamoDb("orders").limit(100)._to_spec("orders")
        assert spec.limit == 100

    def test_limit_zero_raises(self) -> None:
        source = FromDynamoDb("orders")
        with pytest.raises(ValueError, match=">= 1"):
            source.limit(0)


# ---------------------------------------------------------------------------
# .consistent_read()
# ---------------------------------------------------------------------------


class TestConsistentRead:
    def test_default_is_eventually_consistent(self) -> None:
        assert FromDynamoDb("orders")._to_spec("orders").consistent_read is False

    def test_enable_consistent_read(self) -> None:
        spec = FromDynamoDb("orders").consistent_read()._to_spec("orders")
        assert spec.consistent_read is True

    def test_consistent_read_is_immutable(self) -> None:
        base = FromDynamoDb("orders")
        consistent = base.consistent_read()
        assert base._consistent_read is False
        assert consistent._consistent_read is True


# ---------------------------------------------------------------------------
# .parallel_scan()
# ---------------------------------------------------------------------------


class TestParallelScan:
    def test_total_segments_stored_in_spec(self) -> None:
        spec = FromDynamoDb("orders").parallel_scan(8)._to_spec("orders")
        assert spec.total_segments == 8

    @pytest.mark.parametrize(
        "segments",
        [
            pytest.param(1, id="below_minimum_raises"),
            pytest.param(65, id="above_maximum_raises"),
        ],
    )
    def test_parallel_scan_out_of_range_raises(self, segments: int) -> None:
        source = FromDynamoDb("orders")
        with pytest.raises(ValueError, match="between 2 and 64"):
            source.parallel_scan(segments)

    def test_boundary_values(self) -> None:
        assert FromDynamoDb("orders").parallel_scan(2)._total_segments == 2
        assert FromDynamoDb("orders").parallel_scan(64)._total_segments == 64


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_includes_table(self) -> None:
        assert "orders" in repr(FromDynamoDb("orders"))


# ---------------------------------------------------------------------------
# DynamoDbSourceSpec direct validation
# ---------------------------------------------------------------------------


class TestDynamoDbSourceSpecValidation:
    def test_batch_size_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="batch_size"):
            DynamoDbSourceSpec(alias="orders", table="orders", batch_size=0)

    def test_total_segments_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="total_segments"):
            DynamoDbSourceSpec(alias="orders", table="orders", total_segments=1)

    def test_limit_below_one_raises(self) -> None:
        with pytest.raises(ValueError, match="limit"):
            DynamoDbSourceSpec(alias="orders", table="orders", limit=0)


# ---------------------------------------------------------------------------
# ETLStep integration — inline FromDynamoDb recognised via duck typing
# ---------------------------------------------------------------------------


class TestETLStepIntegration:
    def test_from_dynamodb_recognised_as_inline_source(self) -> None:
        from loom.etl.declarative.target import IntoTemp
        from loom.etl.pipeline._step import ETLStep, _SourceForm

        class BuildOrdersStep(ETLStep):
            orders = FromDynamoDb("orders")
            target = IntoTemp("raw_orders")

            def execute(self, params, *, orders): ...

        assert BuildOrdersStep._source_form == _SourceForm.INLINE
        assert "orders" in BuildOrdersStep._inline_sources

    def test_source_set_with_from_dynamodb(self) -> None:
        from loom.etl.declarative.source._from import SourceSet

        class OrderSources(SourceSet):
            orders = FromDynamoDb("orders")

        assert "orders" in OrderSources._sources
