"""Unit tests for compiler._validators ParamExpr field validation."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest

from loom.etl import ETLParams, col
from loom.etl.compiler import ETLCompilationError
from loom.etl.compiler._errors import ETLErrorCode
from loom.etl.compiler._plan import SourceBinding, TargetBinding
from loom.etl.compiler._validators import validate_param_exprs
from loom.etl.declarative.expr._params import params as p
from loom.etl.declarative.expr._refs import TableRef
from loom.etl.declarative.source import TableSourceSpec
from loom.etl.declarative.target._table import AppendSpec, ReplaceWhereSpec
from tests.unit.etl.compiler.doubles import AppendOutStep, RunDateCountryETLParams


def _append_target() -> TargetBinding:
    return TargetBinding(spec=AppendSpec(table_ref=TableRef("t.out")))


def _replace_where_target(pred: Any) -> TargetBinding:
    return TargetBinding(spec=ReplaceWhereSpec(table_ref=TableRef("t.out"), replace_predicate=pred))


def _source_binding(*predicates: Any) -> SourceBinding:
    return SourceBinding(
        alias="data",
        spec=TableSourceSpec(
            alias="data",
            table_ref=TableRef("raw.data"),
            predicates=predicates,
        ),
    )


class TestParamExprFields:
    # ---------------------------------------------------------------------------
    # No predicates — always valid
    # ---------------------------------------------------------------------------

    def test_no_predicates_passes(self) -> None:
        validate_param_exprs(AppendOutStep, RunDateCountryETLParams, (), _append_target())

    # ---------------------------------------------------------------------------
    # Valid field references
    # ---------------------------------------------------------------------------

    def test_valid_source_predicate_passes(self) -> None:
        pred = col("year") == p.run_date.year  # run_date is a declared field
        validate_param_exprs(
            AppendOutStep, RunDateCountryETLParams, (_source_binding(pred),), _append_target()
        )

    def test_valid_replace_where_predicate_passes(self) -> None:
        pred = col("country") == p.country  # country is a declared field
        validate_param_exprs(
            AppendOutStep, RunDateCountryETLParams, (), _replace_where_target(pred)
        )

    def test_nested_attribute_on_valid_field_passes(self) -> None:
        # params.run_date.year — run_date is valid; .year is datetime.date attr, not validated
        pred = col("year") == p.run_date.year
        validate_param_exprs(
            AppendOutStep, RunDateCountryETLParams, (_source_binding(pred),), _append_target()
        )

    # ---------------------------------------------------------------------------
    # Unknown field references raise ETLCompilationError
    # ---------------------------------------------------------------------------

    def test_unknown_field_in_source_predicate_raises(self) -> None:
        pred = col("year") == p.bad_field
        bindings = (_source_binding(pred),)
        target = _append_target()
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_param_exprs(AppendOutStep, RunDateCountryETLParams, bindings, target)
        assert exc_info.value.code == ETLErrorCode.UNKNOWN_PARAM_FIELD
        assert exc_info.value.field == "bad_field"

    def test_unknown_field_in_replace_where_raises(self) -> None:
        pred = col("region") == p.unknown_region
        target = _replace_where_target(pred)
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_param_exprs(AppendOutStep, RunDateCountryETLParams, (), target)
        assert exc_info.value.code == ETLErrorCode.UNKNOWN_PARAM_FIELD
        assert exc_info.value.field == "unknown_region"

    # ---------------------------------------------------------------------------
    # Compound predicates (AND / OR / NOT / IN) are traversed recursively
    # ---------------------------------------------------------------------------

    def test_and_predicate_both_sides_traversed(self) -> None:
        pred = (col("year") == p.run_date.year) & (col("country") == p.bad_field)
        bindings = (_source_binding(pred),)
        target = _append_target()
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_param_exprs(AppendOutStep, RunDateCountryETLParams, bindings, target)
        assert exc_info.value.field == "bad_field"

    def test_or_predicate_traversed(self) -> None:
        pred = (col("a") == p.run_date) | (col("b") == p.nope)
        bindings = (_source_binding(pred),)
        target = _append_target()
        with pytest.raises(ETLCompilationError):
            validate_param_exprs(AppendOutStep, RunDateCountryETLParams, bindings, target)

    def test_not_predicate_traversed(self) -> None:
        pred = ~(col("a") == p.missing)
        bindings = (_source_binding(pred),)
        target = _append_target()
        with pytest.raises(ETLCompilationError):
            validate_param_exprs(AppendOutStep, RunDateCountryETLParams, bindings, target)

    def test_in_predicate_ref_traversed(self) -> None:
        pred = col("country").isin(p.country)  # type: ignore[arg-type]
        validate_param_exprs(
            AppendOutStep, RunDateCountryETLParams, (_source_binding(pred),), _append_target()
        )

    def test_in_predicate_unknown_ref_raises(self) -> None:
        pred = col("x").isin(p.bad)  # type: ignore[arg-type]
        bindings = (_source_binding(pred),)
        target = _append_target()
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_param_exprs(AppendOutStep, RunDateCountryETLParams, bindings, target)
        assert exc_info.value.field == "bad"


# ---------------------------------------------------------------------------
# Non-msgspec params type is skipped gracefully
# ---------------------------------------------------------------------------


class _PlainParams:
    run_date: date


class TestNonStructParams:
    def test_non_struct_params_skipped(self) -> None:
        pred = col("year") == p.anything_goes
        validate_param_exprs(
            AppendOutStep, _PlainParams, (_source_binding(pred),), _append_target()
        )


# ---------------------------------------------------------------------------
# Read-only properties on the params class are known fields
# ---------------------------------------------------------------------------


class PartitionDayWithMethodParams(ETLParams):
    run_date: date

    @property
    def partition_day(self) -> date:
        return self.run_date

    def not_a_property(self) -> date:
        return self.run_date


class TestParamExprProperties:
    def test_property_reference_passes(self) -> None:
        pred = col("day") == p.partition_day.year
        validate_param_exprs(
            AppendOutStep, PartitionDayWithMethodParams, (_source_binding(pred),), _append_target()
        )

    def test_property_reference_in_replace_where_passes(self) -> None:
        pred = col("day") == p.partition_day
        validate_param_exprs(
            AppendOutStep, PartitionDayWithMethodParams, (), _replace_where_target(pred)
        )

    def test_method_reference_still_raises(self) -> None:
        pred = col("day") == p.not_a_property
        sources = (_source_binding(pred),)
        target = _append_target()
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_param_exprs(AppendOutStep, PartitionDayWithMethodParams, sources, target)
        assert exc_info.value.code == ETLErrorCode.UNKNOWN_PARAM_FIELD
        assert exc_info.value.field == "not_a_property"

    def test_unknown_field_still_raises_with_property_params(self) -> None:
        pred = col("day") == p.bad_field
        sources = (_source_binding(pred),)
        target = _append_target()
        with pytest.raises(ETLCompilationError) as exc_info:
            validate_param_exprs(AppendOutStep, PartitionDayWithMethodParams, sources, target)
        assert exc_info.value.code == ETLErrorCode.UNKNOWN_PARAM_FIELD
        assert exc_info.value.field == "bad_field"
