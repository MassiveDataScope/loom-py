"""Tests for TableRef, UnboundColumnRef and col()."""

from __future__ import annotations

import pytest

from loom.etl.declarative.expr._params import ParamExpr
from loom.etl.declarative.expr._predicate import EqPred, GtPred, InPred
from loom.etl.declarative.expr._refs import TableRef, UnboundColumnRef, col


class TestTableRefAndColumns:
    def test_table_ref_and_col_construction(self) -> None:
        table = TableRef("raw.orders")
        unbound = col("year")

        assert table.ref == "raw.orders"
        assert isinstance(unbound, UnboundColumnRef)
        assert unbound.name == "year"

    def test_table_ref_equality_hash_and_repr(self) -> None:
        orders = TableRef("raw.orders")
        same_orders = TableRef("raw.orders")
        assert orders == same_orders
        assert orders != TableRef("raw.customers")
        assert len({TableRef("raw.orders"), TableRef("raw.orders"), TableRef("raw.customers")}) == 2
        assert repr(TableRef("raw.orders")) == "TableRef('raw.orders')"


class TestPredicatesAndHashes:
    @pytest.mark.parametrize(
        "expr,expected_type",
        [
            (col("year") == 2024, EqPred),
            (col("year") == ParamExpr(("run_date", "year")), EqPred),
            (col("amount") > 0, GtPred),
            (col("country").isin(("ES", "FR")), InPred),
        ],
    )
    def test_predicate_builders(self, expr: object, expected_type: type[object]) -> None:
        assert isinstance(expr, expected_type)

    def test_eq_predicate_payloads(self) -> None:
        literal_pred = col("year") == 2024

        assert isinstance(literal_pred, EqPred)
        assert literal_pred.right == 2024

    def test_hash_and_repr_contract(self) -> None:
        """Equality cannot be asserted directly: ``__eq__`` builds a DSL predicate."""
        year, same_year = col("year"), col("year")
        assert hash(year) == hash(same_year)
        assert hash(year) != hash(col("month"))
        assert repr(col("year")) == "col('year')"
