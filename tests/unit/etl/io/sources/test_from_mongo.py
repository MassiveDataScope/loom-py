"""Tests for the unified FromMongo builder and MongoSourceSpec."""

from __future__ import annotations

import pytest

from loom.core.expr.nodes import AndExpr, EqExpr, InExpr
from loom.etl.declarative.expr import col
from loom.etl.declarative.source import FromMongo, SourceRef
from loom.etl.declarative.source._specs import MongoSourceSpec, SourceKind
from loom.etl.schema._schema import ColumnSchema, LoomDtype
from tests.unit.etl.io.sources.doubles import OptionalStatusOrderDoc

# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_basic_spec(self) -> None:
        spec = FromMongo("orders")._to_spec("orders")
        assert isinstance(spec, MongoSourceSpec)
        assert spec.alias == "orders"
        assert spec.collection == "orders"
        assert spec.filter is None
        assert spec.projection is None
        assert spec.schema == ()
        assert spec.extra_fields_mode == "ignore"
        assert spec.batch_size == 2_000
        assert spec.limit is None

    def test_kind_is_mongo(self) -> None:
        spec = FromMongo("events")._to_spec("events")
        assert spec.kind == SourceKind.MONGO

    def test_invalid_collection_name_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid MongoDB collection"):
            FromMongo("bad name!")

    def test_collection_with_hyphen_and_underscore_is_valid(self) -> None:
        spec = FromMongo("order-items_v2")._to_spec("src")
        assert spec.collection == "order-items_v2"


# ---------------------------------------------------------------------------
# .where()
# ---------------------------------------------------------------------------


class TestWhere:
    def test_where_sets_filter(self) -> None:
        spec = FromMongo("orders").where(col("status") == "active")._to_spec("orders")
        assert isinstance(spec.filter, EqExpr)

    def test_multiple_where_calls_and_predicates(self) -> None:
        spec = (
            FromMongo("orders")
            .where(col("status") == "active")
            .where(col("year") == 2024)
            ._to_spec("orders")
        )
        assert isinstance(spec.filter, AndExpr)

    def test_where_with_source_ref_isin(self) -> None:
        from loom.etl.declarative.source._from import FromTemp

        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        spec = FromMongo("orders").where(col("_id").isin(ref))._to_spec("orders")
        assert isinstance(spec.filter, InExpr)
        assert isinstance(spec.filter.values, SourceRef)

    def test_where_is_immutable(self) -> None:
        base = FromMongo("orders")
        filtered = base.where(col("status") == "active")
        assert base._filter is None
        assert filtered._filter is not None


# ---------------------------------------------------------------------------
# .project()
# ---------------------------------------------------------------------------


class TestProject:
    def test_project_sets_projection(self) -> None:
        spec = FromMongo("orders").project("_id", "status", "total")._to_spec("orders")
        assert spec.projection == ("_id", "status", "total")

    def test_project_rejects_dollar_operators(self) -> None:
        source = FromMongo("orders")
        with pytest.raises(ValueError, match=r"\$"):
            source.project("$elemMatch")

    def test_project_is_immutable(self) -> None:
        base = FromMongo("orders")
        projected = base.project("_id")
        assert base._projection is None
        assert projected._projection == ("_id",)


# ---------------------------------------------------------------------------
# .with_schema()
# ---------------------------------------------------------------------------


class TestWithSchema:
    def test_schema_stored_in_spec(self) -> None:
        spec = FromMongo("orders").with_schema(OptionalStatusOrderDoc)._to_spec("orders")
        assert spec.schema == (
            ColumnSchema("id", LoomDtype.UTF8, nullable=True),
            ColumnSchema("status", LoomDtype.UTF8, nullable=True),
        )


# ---------------------------------------------------------------------------
# .on_extra_fields()
# ---------------------------------------------------------------------------


class TestOnExtraFields:
    @pytest.mark.parametrize("mode", ["ignore", "warn", "capture", "error"])
    def test_valid_modes(self, mode: str) -> None:
        spec = FromMongo("orders").on_extra_fields(mode)._to_spec("orders")  # type: ignore[arg-type]
        assert spec.extra_fields_mode == mode

    def test_invalid_mode_raises(self) -> None:
        source = FromMongo("orders")
        with pytest.raises(ValueError, match="not valid"):
            source.on_extra_fields("bad")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# .batch_size()
# ---------------------------------------------------------------------------


class TestBatchSize:
    def test_default_batch_size(self) -> None:
        spec = FromMongo("orders")._to_spec("orders")
        assert spec.batch_size == 2_000

    def test_custom_batch_size(self) -> None:
        spec = FromMongo("orders").batch_size(500)._to_spec("orders")
        assert spec.batch_size == 500

    @pytest.mark.parametrize(
        "batch_size",
        [
            pytest.param(0, id="batch_size_zero_raises"),
            pytest.param(50001, id="batch_size_too_high_raises"),
        ],
    )
    def test_batch_size_out_of_range_raises(self, batch_size: int) -> None:
        source = FromMongo("orders")
        with pytest.raises(ValueError, match="between 1 and 50000"):
            source.batch_size(batch_size)

    def test_batch_size_boundary_values(self) -> None:
        assert FromMongo("orders").batch_size(1)._batch_size == 1
        assert FromMongo("orders").batch_size(50_000)._batch_size == 50_000


# ---------------------------------------------------------------------------
# .limit()
# ---------------------------------------------------------------------------


class TestLimit:
    def test_limit_stored_in_spec(self) -> None:
        spec = FromMongo("orders").limit(100)._to_spec("orders")
        assert spec.limit == 100

    def test_limit_zero_raises(self) -> None:
        source = FromMongo("orders")
        with pytest.raises(ValueError, match=">= 1"):
            source.limit(0)


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------


class TestRepr:
    def test_repr_includes_collection(self) -> None:
        assert "orders" in repr(FromMongo("orders"))


# ---------------------------------------------------------------------------
# MongoSourceSpec direct validation
# ---------------------------------------------------------------------------


class TestMongoSourceSpecValidation:
    def test_batch_size_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="batch_size"):
            MongoSourceSpec(alias="orders", collection="orders", batch_size=0)


# ---------------------------------------------------------------------------
# ETLStep integration — inline FromMongo recognised via duck typing
# ---------------------------------------------------------------------------


class TestETLStepIntegration:
    def test_from_mongo_recognised_as_inline_source(self) -> None:
        from loom.etl.declarative.target import IntoTemp
        from loom.etl.pipeline._step import ETLStep, _SourceForm

        class BuildOrdersStep(ETLStep):
            orders = FromMongo("orders")
            target = IntoTemp("raw_orders")

            def execute(self, params, *, orders): ...

        assert BuildOrdersStep._source_form == _SourceForm.INLINE
        assert "orders" in BuildOrdersStep._inline_sources

    def test_source_set_with_from_mongo(self) -> None:
        from loom.etl.declarative.source._from import SourceSet

        class OrderSources(SourceSet):
            orders = FromMongo("orders")

        assert "orders" in OrderSources._sources
