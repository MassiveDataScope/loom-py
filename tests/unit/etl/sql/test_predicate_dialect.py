"""Tests for shared predicate fold + PredicateDialect contract."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import pytest

from loom.etl.declarative.expr._params import params
from loom.etl.declarative.expr._predicate import PredicateNode
from loom.etl.declarative.expr._predicate_dialect import PredicateDialect, fold_predicate
from loom.etl.declarative.expr._refs import col


@dataclass(frozen=True)
class RunDateCountriesFrozenParams:
    run_date: date
    countries: tuple[str, ...]


def _params() -> RunDateCountriesFrozenParams:
    return RunDateCountriesFrozenParams(run_date=date(2024, 3, 1), countries=("ES", "FR"))


class _TraceDialect(PredicateDialect[str]):
    def column(self, name: str) -> str:
        return f"C[{name}]"

    def literal(self, value: Any) -> str:
        return f"L[{value!r}]"

    def eq(self, left: str, right: str) -> str:
        return f"EQ({left},{right})"

    def ne(self, left: str, right: str) -> str:
        return f"NE({left},{right})"

    def gt(self, left: str, right: str) -> str:
        return f"GT({left},{right})"

    def ge(self, left: str, right: str) -> str:
        return f"GE({left},{right})"

    def lt(self, left: str, right: str) -> str:
        return f"LT({left},{right})"

    def le(self, left: str, right: str) -> str:
        return f"LE({left},{right})"

    def in_(self, ref: str, values: tuple[Any, ...]) -> str:
        return f"IN({ref},{values!r})"

    def and_(self, left: str, right: str) -> str:
        return f"AND({left},{right})"

    def or_(self, left: str, right: str) -> str:
        return f"OR({left},{right})"

    def not_(self, operand: str) -> str:
        return f"NOT({operand})"


_DIALECT = _TraceDialect()


class TestFoldPredicate:
    @pytest.mark.parametrize(
        ("node", "expected"),
        [
            (col("a") == 1, "EQ(C[a],L[1])"),
            (col("a") != 1, "NE(C[a],L[1])"),
            (col("a") > 1, "GT(C[a],L[1])"),
            (col("a") >= 1, "GE(C[a],L[1])"),
            (col("a") < 1, "LT(C[a],L[1])"),
            (col("a") <= 1, "LE(C[a],L[1])"),
            (col("a").isin((1, 2)), "IN(C[a],(1, 2))"),
        ],
    )
    def test_fold_predicate_binary_and_in(self, node: PredicateNode, expected: str) -> None:
        assert fold_predicate(node, _params(), _DIALECT) == expected

    def test_fold_predicate_composition_and_param_resolution(self) -> None:
        node = (
            (col("year") == params.run_date.year) & (col("country").isin(params.countries))
        ) | ~(
            col("is_deleted") == True  # noqa: E712
        )
        result = fold_predicate(node, _params(), _DIALECT)

        assert "EQ(C[year],L[2024])" in result
        assert "IN(C[country],('ES', 'FR'))" in result
        assert "NOT(EQ(C[is_deleted],L[True]))" in result
        assert result.startswith("OR(")

    def test_fold_predicate_unsupported_node_raises(self) -> None:
        params = _params()
        with pytest.raises(TypeError, match="unsupported node type"):
            fold_predicate(object(), params, _DIALECT)  # type: ignore[arg-type]
