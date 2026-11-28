"""Predicate renderers for backend execution.

This module keeps one predicate AST (`io/source/_predicate.py`) and offers
backend render targets:

- SQL string (`predicate_to_sql`)
- Polars expression (`predicate_to_polars`)
"""

from __future__ import annotations

from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from loom.etl.declarative.expr._predicate import PredicateNode
from loom.etl.declarative.expr._predicate_dialect import PredicateDialect, fold_predicate

if TYPE_CHECKING:
    import polars as pl


def sql_literal(value: Any) -> str:
    """Render a Python scalar as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        return f"'{value.isoformat()}'"
    if isinstance(value, date):
        return f"'{value.isoformat()}'"
    if isinstance(value, str):
        return f"'{value.replace(chr(39), chr(39) * 2)}'"
    return str(value)


class _SqlPredicateDialect(PredicateDialect[str]):
    """Render predicate nodes as ANSI SQL fragments."""

    def column(self, name: str) -> str:
        return name

    def literal(self, value: Any) -> str:
        return sql_literal(value)

    def eq(self, left: str, right: str) -> str:
        return f"{left} = {right}"

    def ne(self, left: str, right: str) -> str:
        return f"{left} != {right}"

    def gt(self, left: str, right: str) -> str:
        return f"{left} > {right}"

    def ge(self, left: str, right: str) -> str:
        return f"{left} >= {right}"

    def lt(self, left: str, right: str) -> str:
        return f"{left} < {right}"

    def le(self, left: str, right: str) -> str:
        return f"{left} <= {right}"

    def in_(self, ref: str, values: tuple[Any, ...]) -> str:
        formatted = ", ".join(self._in_literal(v) for v in values)
        return f"{ref} IN ({formatted})"

    def and_(self, left: str, right: str) -> str:
        return f"({left}) AND ({right})"

    def or_(self, left: str, right: str) -> str:
        return f"({left}) OR ({right})"

    def not_(self, operand: str) -> str:
        return f"NOT ({operand})"

    def _in_literal(self, value: Any) -> str:
        return sql_literal(value)


class _PolarsPredicateDialect(PredicateDialect["pl.Expr"]):
    """Render predicate nodes as Polars expressions."""

    def __init__(self) -> None:
        import polars as pl

        self._pl = pl

    def column(self, name: str) -> pl.Expr:
        return self._pl.col(name)

    def literal(self, value: Any) -> pl.Expr:
        return self._pl.lit(value)

    def eq(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return left == right

    def ne(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return left != right

    def gt(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return left > right

    def ge(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return left >= right

    def lt(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return left < right

    def le(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return left <= right

    def in_(self, ref: pl.Expr, values: tuple[Any, ...]) -> pl.Expr:
        return ref.is_in(list(values))

    def and_(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return left & right

    def or_(self, left: pl.Expr, right: pl.Expr) -> pl.Expr:
        return left | right

    def not_(self, operand: pl.Expr) -> pl.Expr:
        return ~operand


_SQL_PREDICATE_DIALECT = _SqlPredicateDialect()
_POLARS_PREDICATE_DIALECT = _PolarsPredicateDialect()


def predicate_to_sql(node: PredicateNode, params_instance: Any) -> str:
    """Serialise *node* to ANSI SQL predicate string."""
    return fold_predicate(node, params_instance, _SQL_PREDICATE_DIALECT)


def predicate_to_polars(node: PredicateNode, params_instance: Any) -> pl.Expr:
    """Convert *node* to ``polars.Expr`` for ``LazyFrame.filter``."""
    return fold_predicate(node, params_instance, _POLARS_PREDICATE_DIALECT)
