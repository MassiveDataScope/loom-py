"""Tests for predicate node construction and composition."""

from __future__ import annotations

from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._predicate import (
    AndPred,
    EqPred,
    GePred,
    GtPred,
    InPred,
    LePred,
    LtPred,
    NePred,
    NotPred,
    OrPred,
)
from loom.etl.declarative.expr._refs import UnboundColumnRef, col


class TestPredicateNodes:
    def test_eq_pred_from_col_and_literal(self) -> None:
        pred = col("year") == 2024
        assert isinstance(pred, EqPred)
        assert isinstance(pred.left, UnboundColumnRef)
        assert pred.right == 2024

    def test_ne_pred(self) -> None:
        pred = col("status") != "deleted"
        assert isinstance(pred, NePred)

    def test_gt_pred(self) -> None:
        assert isinstance(col("amount") > 100, GtPred)

    def test_ge_pred(self) -> None:
        assert isinstance(col("amount") >= 100, GePred)

    def test_lt_pred(self) -> None:
        assert isinstance(col("amount") < 100, LtPred)

    def test_le_pred(self) -> None:
        assert isinstance(col("amount") <= 100, LePred)

    def test_isin_pred_with_literal_tuple(self) -> None:
        pred = col("country").isin(("ES", "FR"))
        assert isinstance(pred, InPred)
        assert pred.values == ("ES", "FR")

    def test_isin_pred_with_param_expr(self) -> None:
        expr = ParamExpr(("countries",))
        pred = col("country").isin(expr)
        assert isinstance(pred, InPred)
        assert pred.values is expr

    def test_and_composition(self) -> None:
        left = col("year") == 2024
        right = col("month") == 1
        combined = left & right
        assert isinstance(combined, AndPred)
        assert combined.left is left
        assert combined.right is right

    def test_or_composition(self) -> None:
        combined = (col("status") == "a") | (col("status") == "b")
        assert isinstance(combined, OrPred)

    def test_not_predicate(self) -> None:
        pred = ~(col("deleted") == True)  # noqa: E712
        assert isinstance(pred, NotPred)

    def test_chained_and_produces_nested_and(self) -> None:
        p = (col("a") == 1) & (col("b") == 2) & (col("c") == 3)
        assert isinstance(p, AndPred)
        assert isinstance(p.left, AndPred)

    def test_col_eq_param_expr(self) -> None:
        expr = ParamExpr(("run_date", "year"))
        pred = col("year") == expr
        assert isinstance(pred, EqPred)
        assert pred.right is expr

    def test_frozen_pred_is_hashable(self) -> None:
        pred = col("year") == 2024
        # frozen dataclass — must be hashable for use in sets/dicts
        assert isinstance(hash(pred), int)
        s: set[EqPred] = {pred}
        assert pred in s
