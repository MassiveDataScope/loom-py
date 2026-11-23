"""Tests for predicate_to_dynamo."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import pytest

from loom.core.plugins.optional import MissingExtraError
from loom.etl.declarative.expr import col, params
from loom.etl.declarative.source import SourceRef
from loom.etl.declarative.source._from import FromTemp
from loom.etl.io.sources._dynamodb_items import normalize_dynamo_item
from loom.etl.io.sources._dynamodb_predicate import (
    DynamoFilterExpression,
    _to_dynamo_value,
    predicate_to_dynamo,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class StatusYearStatusesParams:
    status: str = "active"
    year: int = 2024
    statuses: tuple[str, ...] = ("active", "pending")


NO_PARAMS: Any = None


# ---------------------------------------------------------------------------
# Basic operators
# ---------------------------------------------------------------------------


class TestEqExpr:
    def test_eq_string_literal(self) -> None:
        result = predicate_to_dynamo(col("status") == "active", NO_PARAMS)
        assert isinstance(result, DynamoFilterExpression)
        assert result.expression == "#n0 = :v0"
        assert result.attribute_names == {"#n0": "status"}
        assert result.attribute_values == {":v0": {"S": "active"}}

    def test_eq_integer_literal(self) -> None:
        result = predicate_to_dynamo(col("year") == 2024, NO_PARAMS)
        assert result.expression == "#n0 = :v0"
        assert result.attribute_values == {":v0": {"N": "2024"}}

    def test_eq_bool_literal(self) -> None:
        result = predicate_to_dynamo(col("active") == True, NO_PARAMS)  # noqa: E712
        assert result.attribute_values == {":v0": {"BOOL": True}}


class TestNeExpr:
    def test_ne(self) -> None:
        result = predicate_to_dynamo(col("status") != "cancelled", NO_PARAMS)
        assert result.expression == "#n0 <> :v0"
        assert result.attribute_values == {":v0": {"S": "cancelled"}}


class TestComparisonExprs:
    def test_gt(self) -> None:
        result = predicate_to_dynamo(col("amount") > 100, NO_PARAMS)
        assert result.expression == "#n0 > :v0"
        assert result.attribute_values == {":v0": {"N": "100"}}

    def test_gte(self) -> None:
        assert predicate_to_dynamo(col("amount") >= 100, NO_PARAMS).expression == "#n0 >= :v0"

    def test_lt(self) -> None:
        assert predicate_to_dynamo(col("amount") < 100, NO_PARAMS).expression == "#n0 < :v0"

    def test_lte(self) -> None:
        assert predicate_to_dynamo(col("amount") <= 100, NO_PARAMS).expression == "#n0 <= :v0"


# ---------------------------------------------------------------------------
# Reserved-word safety — every name is aliased, every value is a placeholder
# ---------------------------------------------------------------------------


class TestAliasing:
    @pytest.mark.parametrize(
        "column",
        [
            pytest.param("status", id="reserved_word_status_is_aliased"),
            pytest.param("name", id="reserved_word_name_is_aliased"),
        ],
    )
    def test_reserved_word_is_aliased(self, column: str) -> None:
        result = predicate_to_dynamo(col(column) == "x", NO_PARAMS)
        assert column not in result.expression
        assert result.attribute_names["#n0"] == column

    def test_counters_are_monotonic_across_nodes(self) -> None:
        node = (col("status") == "active") & (col("year") > 2020)
        result = predicate_to_dynamo(node, NO_PARAMS)
        assert result.expression == "(#n0 = :v0) AND (#n1 > :v1)"
        assert result.attribute_names == {"#n0": "status", "#n1": "year"}
        assert result.attribute_values == {":v0": {"S": "active"}, ":v1": {"N": "2020"}}


# ---------------------------------------------------------------------------
# InExpr
# ---------------------------------------------------------------------------


class TestInExpr:
    def test_in_tuple(self) -> None:
        node = col("status").isin(("active", "pending", "shipped"))
        result = predicate_to_dynamo(node, NO_PARAMS)
        assert result.expression == "#n0 IN (:v0, :v1, :v2)"
        assert result.attribute_values == {
            ":v0": {"S": "active"},
            ":v1": {"S": "pending"},
            ":v2": {"S": "shipped"},
        }

    def test_in_list(self) -> None:
        node = col("pk").isin(["id1", "id2"])
        result = predicate_to_dynamo(node, NO_PARAMS)
        assert result.expression == "#n0 IN (:v0, :v1)"

    def test_in_param_expr(self) -> None:
        node = col("status").isin(params.statuses)
        result = predicate_to_dynamo(node, StatusYearStatusesParams())
        assert result.expression == "#n0 IN (:v0, :v1)"
        assert result.attribute_values == {":v0": {"S": "active"}, ":v1": {"S": "pending"}}

    def test_in_empty_values_raises(self) -> None:
        predicate = col("status").isin(())
        with pytest.raises(ValueError, match="at least one value"):
            predicate_to_dynamo(predicate, NO_PARAMS)

    def test_in_source_ref_raises_before_materialize(self) -> None:
        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        node = col("pk").isin(ref)
        with pytest.raises(TypeError, match="materialize_filter"):
            predicate_to_dynamo(node, NO_PARAMS)


# ---------------------------------------------------------------------------
# Boolean composition
# ---------------------------------------------------------------------------


class TestBooleanComposition:
    def test_and(self) -> None:
        node = (col("status") == "active") & (col("year") == 2024)
        result = predicate_to_dynamo(node, NO_PARAMS)
        assert result.expression == "(#n0 = :v0) AND (#n1 = :v1)"

    def test_or(self) -> None:
        node = (col("status") == "active") | (col("status") == "pending")
        result = predicate_to_dynamo(node, NO_PARAMS)
        assert result.expression == "(#n0 = :v0) OR (#n1 = :v1)"

    def test_not(self) -> None:
        node = ~(col("status") == "cancelled")
        result = predicate_to_dynamo(node, NO_PARAMS)
        assert result.expression == "NOT (#n0 = :v0)"

    def test_nested_and_or_not(self) -> None:
        node = ((col("a") == 1) | (col("b") == 2)) & ~(col("c") == 3)
        result = predicate_to_dynamo(node, NO_PARAMS)
        assert result.expression == "((#n0 = :v0) OR (#n1 = :v1)) AND (NOT (#n2 = :v2))"
        assert result.attribute_names == {"#n0": "a", "#n1": "b", "#n2": "c"}


# ---------------------------------------------------------------------------
# Param resolution
# ---------------------------------------------------------------------------


class TestParamResolution:
    def test_param_expr_resolved(self) -> None:
        node = col("status") == params.status
        result = predicate_to_dynamo(node, StatusYearStatusesParams())
        assert result.attribute_values == {":v0": {"S": "active"}}

    def test_param_expr_int(self) -> None:
        node = col("year") >= params.year
        result = predicate_to_dynamo(node, StatusYearStatusesParams())
        assert result.attribute_values == {":v0": {"N": "2024"}}


# ---------------------------------------------------------------------------
# AttributeValue serialization shapes
# ---------------------------------------------------------------------------


class TestValueSerialization:
    def test_float_becomes_decimal_number(self) -> None:
        result = predicate_to_dynamo(col("amount") > 99.5, NO_PARAMS)
        assert result.attribute_values == {":v0": {"N": "99.5"}}

    def test_datetime_becomes_iso_string(self) -> None:
        at = datetime(2024, 5, 1, 12, 30, tzinfo=UTC)
        result = predicate_to_dynamo(col("created_at") >= at, NO_PARAMS)
        assert result.attribute_values == {":v0": {"S": "2024-05-01T12:30:00+00:00"}}

    def test_date_becomes_iso_string(self) -> None:
        result = predicate_to_dynamo(col("day") == date(2024, 5, 1), NO_PARAMS)
        assert result.attribute_values == {":v0": {"S": "2024-05-01"}}

    def test_none_becomes_null(self) -> None:
        result = predicate_to_dynamo(col("deleted_at") == None, NO_PARAMS)  # noqa: E711
        assert result.attribute_values == {":v0": {"NULL": True}}

    def test_list_value_becomes_l_with_nested_floats_as_decimals(self) -> None:
        result = predicate_to_dynamo(col("box") == [1, 2.5], NO_PARAMS)
        assert result.attribute_values == {":v0": {"L": [{"N": "1"}, {"N": "2.5"}]}}

    def test_to_dynamo_value_set_becomes_list_with_decimal_floats(self) -> None:
        from decimal import Decimal

        assert _to_dynamo_value({1.5}) == [Decimal("1.5")]

    def test_to_dynamo_value_frozenset_becomes_list(self) -> None:
        assert _to_dynamo_value(frozenset({"a"})) == ["a"]

    def test_to_dynamo_value_nested_dict_recursed(self) -> None:
        from decimal import Decimal

        assert _to_dynamo_value({"price": 9.99, "qty": 2}) == {
            "price": Decimal("9.99"),
            "qty": 2,
        }


# ---------------------------------------------------------------------------
# Unsupported nodes
# ---------------------------------------------------------------------------


class TestUnsupportedNodeRaises:
    def test_raw_string_raises(self) -> None:
        with pytest.raises(TypeError, match="unsupported predicate node type"):
            predicate_to_dynamo("not_a_node", NO_PARAMS)


class TestBoto3Missing:
    def test_predicate_without_boto3_names_the_aws_extra(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("loom.etl.io.sources._dynamodb_predicate._ddb_types", None)

        with pytest.raises(MissingExtraError) as excinfo:
            predicate_to_dynamo(col("status") == "active", NO_PARAMS)

        assert excinfo.value.extra == "aws"
        assert excinfo.value.missing == "boto3"

    def test_item_normalizer_without_boto3_names_the_aws_extra(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("loom.etl.io.sources._dynamodb_items._DESERIALIZER", None)

        with pytest.raises(MissingExtraError) as excinfo:
            normalize_dynamo_item({"id": {"S": "a"}})

        assert excinfo.value.extra == "aws"
