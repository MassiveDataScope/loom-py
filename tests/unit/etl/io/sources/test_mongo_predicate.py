"""Tests for materialize_filter and predicate_to_mongo."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl
import pytest

from loom.core.expr.nodes import (
    AndExpr,
    InExpr,
    NotExpr,
    OrExpr,
)
from loom.etl.declarative.expr import col
from loom.etl.declarative.source import SourceRef
from loom.etl.declarative.source._from import FromTemp
from loom.etl.io.sources._mongo_predicate import materialize_filter, predicate_to_mongo

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@dataclass
class StatusYearParams:
    status: str = "active"
    year: int = 2024


NO_PARAMS: Any = None


# ---------------------------------------------------------------------------
# predicate_to_mongo — basic operators
# ---------------------------------------------------------------------------


class TestEqExpr:
    @pytest.mark.parametrize(
        ("value", "column"),
        [
            pytest.param("active", "status", id="eq_string_literal"),
            pytest.param(2024, "year", id="eq_integer_literal"),
        ],
    )
    def test_eq_literal(self, value: object, column: str) -> None:
        node = col(column) == value
        assert predicate_to_mongo(node, NO_PARAMS) == {column: value}


class TestNeExpr:
    def test_ne(self) -> None:
        node = col("status") != "cancelled"
        assert predicate_to_mongo(node, NO_PARAMS) == {"status": {"$ne": "cancelled"}}


class TestComparisonExprs:
    def test_gt(self) -> None:
        assert predicate_to_mongo(col("amount") > 100, NO_PARAMS) == {"amount": {"$gt": 100}}

    def test_gte(self) -> None:
        assert predicate_to_mongo(col("amount") >= 100, NO_PARAMS) == {"amount": {"$gte": 100}}

    def test_lt(self) -> None:
        assert predicate_to_mongo(col("amount") < 100, NO_PARAMS) == {"amount": {"$lt": 100}}

    def test_lte(self) -> None:
        assert predicate_to_mongo(col("amount") <= 100, NO_PARAMS) == {"amount": {"$lte": 100}}


class TestInExpr:
    def test_in_tuple(self) -> None:
        node = col("status").isin(("active", "pending", "shipped"))
        result = predicate_to_mongo(node, NO_PARAMS)
        assert result == {"status": {"$in": ["active", "pending", "shipped"]}}

    def test_in_list(self) -> None:
        node = col("_id").isin(["id1", "id2"])
        assert predicate_to_mongo(node, NO_PARAMS) == {"_id": {"$in": ["id1", "id2"]}}

    def test_in_source_ref_raises_before_materialize(self) -> None:
        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        node = col("_id").isin(ref)
        with pytest.raises(TypeError, match="materialize_filter"):
            predicate_to_mongo(node, NO_PARAMS)


class TestAndExpr:
    def test_and_two_predicates(self) -> None:
        node = (col("status") == "active") & (col("year") == 2024)
        result = predicate_to_mongo(node, NO_PARAMS)
        assert result == {"$and": [{"status": "active"}, {"year": 2024}]}


class TestOrExpr:
    def test_or_two_predicates(self) -> None:
        node = (col("status") == "active") | (col("status") == "pending")
        result = predicate_to_mongo(node, NO_PARAMS)
        assert result == {"$or": [{"status": "active"}, {"status": "pending"}]}


class TestNotExpr:
    def test_not(self) -> None:
        node = ~(col("status") == "cancelled")
        result = predicate_to_mongo(node, NO_PARAMS)
        assert result == {"$nor": [{"status": "cancelled"}]}


class TestUnsupportedNodeRaises:
    def test_raw_string_raises(self) -> None:
        with pytest.raises(TypeError, match="unsupported predicate node type"):
            predicate_to_mongo("not_a_node", NO_PARAMS)


# ---------------------------------------------------------------------------
# predicate_to_mongo — param resolution
# ---------------------------------------------------------------------------


class TestParamResolution:
    def test_eq_with_param(self) -> None:
        from loom.etl.declarative.expr import params as P

        node = col("status") == P.status
        result = predicate_to_mongo(node, StatusYearParams(status="shipped"))
        assert result == {"status": "shipped"}

    def test_gte_with_param(self) -> None:
        from loom.etl.declarative.expr import params as P

        node = col("year") >= P.year
        result = predicate_to_mongo(node, StatusYearParams(year=2023))
        assert result == {"year": {"$gte": 2023}}


# ---------------------------------------------------------------------------
# materialize_filter
# ---------------------------------------------------------------------------


class TestMaterializeFilter:
    def test_no_source_ref_returns_same_node(self) -> None:
        node = col("status") == "active"
        result = materialize_filter(node, {})
        assert result is node

    def test_in_expr_with_source_ref_resolved(self) -> None:
        df = pl.DataFrame({"order_id": ["oid1", "oid2", "oid3"]})
        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        node = col("_id").isin(ref)
        result = materialize_filter(node, {"order_ids": df})
        assert isinstance(result, InExpr)
        assert result.values == ("oid1", "oid2", "oid3")

    def test_materialized_node_renders_to_mongo(self) -> None:
        df = pl.DataFrame({"order_id": ["a", "b"]})
        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        node = col("_id").isin(ref)
        materialized = materialize_filter(node, {"order_ids": df})
        result = predicate_to_mongo(materialized, NO_PARAMS)
        assert result == {"_id": {"$in": ["a", "b"]}}

    def test_and_expr_recurses_into_both_sides(self) -> None:
        df = pl.DataFrame({"order_id": ["x"]})
        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        node = (col("status") == "active") & col("_id").isin(ref)
        result = materialize_filter(node, {"order_ids": df})
        assert isinstance(result, AndExpr)
        assert isinstance(result.right, InExpr)
        assert result.right.values == ("x",)

    def test_or_expr_recurses_into_both_sides(self) -> None:
        df = pl.DataFrame({"order_id": ["x"]})
        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        node = (col("status") == "cancelled") | col("_id").isin(ref)
        result = materialize_filter(node, {"order_ids": df})
        assert isinstance(result, OrExpr)
        assert isinstance(result.right, InExpr)
        assert result.right.values == ("x",)

    def test_not_expr_recurses(self) -> None:
        df = pl.DataFrame({"order_id": ["z"]})
        ref = SourceRef(FromTemp("order_ids"), col="order_id")
        node = ~col("_id").isin(ref)
        result = materialize_filter(node, {"order_ids": df})
        assert isinstance(result, NotExpr)
        assert isinstance(result.operand, InExpr)
        assert result.operand.values == ("z",)
