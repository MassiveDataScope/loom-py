"""Tests for FromClickHouse builder and ClickHouseSourceSpec."""

from __future__ import annotations

import pytest

from loom.etl.declarative.source._from_clickhouse import FromClickHouse
from loom.etl.declarative.source._specs import ClickHouseSourceSpec, SourceKind
from loom.etl.schema._schema import ColumnSchema, LoomDtype

# A minimal predicate stand-in — just a truthy object that can be stored in a
# tuple, equivalent to how the existing FromTable tests pass col() expressions.
_PRED = object()
_SCHEMA = (
    ColumnSchema("id", LoomDtype.INT64, nullable=False),
    ColumnSchema("amount", LoomDtype.FLOAT64),
)


class TestToSpecRequiresPredicateOrUnbounded:
    def test_to_spec_requires_predicate_or_unbounded(self) -> None:
        """FromClickHouse without .where() or .unbounded() must raise ValueError."""
        builder = FromClickHouse("cdc_events")
        with pytest.raises(ValueError):
            builder._to_spec("cdc")


class TestToSpecWithWhere:
    def test_to_spec_with_where_produces_spec(self) -> None:
        """Calling .where(pred) yields a ClickHouseSourceSpec with the right fields."""
        builder = FromClickHouse("cdc_events").where(_PRED)
        spec = builder._to_spec("cdc")

        assert isinstance(spec, ClickHouseSourceSpec)
        assert spec.alias == "cdc"
        assert len(spec.predicates) == 1
        assert spec.predicates[0] is _PRED
        assert spec.table_ref.ref == "cdc_events"


class TestToSpecUnbounded:
    def test_to_spec_unbounded_allows_no_predicate(self) -> None:
        """.unbounded() with no .where() must NOT raise ETLCompilationError."""
        builder = FromClickHouse("cdc_events").unbounded()
        # Should not raise
        spec = builder._to_spec("cdc")
        assert isinstance(spec, ClickHouseSourceSpec)


class TestSelectColumns:
    def test_select_columns_stored_in_spec(self) -> None:
        """Selected columns are stored as a tuple on the spec."""
        builder = FromClickHouse("cdc_events").where(_PRED).columns("a", "b")
        spec = builder._to_spec("cdc")

        assert spec.columns == ("a", "b")


class TestWithSchema:
    def test_with_schema_stored_in_spec(self) -> None:
        spec = FromClickHouse("cdc_events").where(_PRED).with_schema(_SCHEMA)._to_spec("cdc")
        assert spec.schema == _SCHEMA

    def test_with_schema_is_immutable(self) -> None:
        original = FromClickHouse("cdc_events").where(_PRED)
        _ = original.with_schema(_SCHEMA)
        assert original._to_spec("cdc").schema == ()


class TestDistinctFlag:
    def test_distinct_flag_stored_in_spec(self) -> None:
        """Calling .distinct() sets spec.distinct to True."""
        builder = FromClickHouse("cdc_events").where(_PRED).distinct()
        spec = builder._to_spec("cdc")

        assert spec.distinct is True

    def test_distinct_defaults_to_false(self) -> None:
        """Without .distinct(), spec.distinct is False."""
        spec = FromClickHouse("cdc_events").where(_PRED)._to_spec("cdc")
        assert spec.distinct is False


class TestSpecKind:
    def test_spec_kind_is_clickhouse(self) -> None:
        """spec.kind must equal SourceKind.CLICKHOUSE."""
        spec = FromClickHouse("cdc_events").where(_PRED)._to_spec("cdc")
        assert spec.kind == SourceKind.CLICKHOUSE


class TestColumnsValidation:
    def test_columns_empty_raises(self) -> None:
        source = FromClickHouse("cdc_events")
        with pytest.raises(ValueError, match="at least one"):
            source.columns()


class TestRepr:
    def test_repr_includes_table(self) -> None:
        """repr(FromClickHouse('cdc_events')) must contain the table name."""
        assert "cdc_events" in repr(FromClickHouse("cdc_events"))
