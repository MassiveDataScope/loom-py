"""Predicate AST → Polars expression converter.

Translates a :data:`~loom.etl._predicate.PredicateNode` tree into a
``polars.Expr`` so it can be passed to ``LazyFrame.filter()``.

Polars' lazy optimizer then pushes the filter into the PyArrow dataset
scanner, which performs:

* **Partition pruning** — entire partition directories whose column values
  do not match the predicate are skipped before any I/O.
* **Row-group pruning** — Parquet row groups whose column statistics
  (min/max) exclude the predicate are skipped within each file.
* **Row-level filtering** — remaining rows are filtered in-memory.

The combined effect means a ``WHERE year = 2024`` predicate on a table
partitioned by ``year`` scans only the relevant partition files — no full
table scan in memory.

See https://delta-io.github.io/delta-rs/api/delta_table/#deltalake.DeltaTable.to_pyarrow_dataset
See https://docs.pola.rs/api/python/stable/reference/lazyframe/api/polars.LazyFrame.filter.html
"""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl.declarative.expr._predicate import PredicateNode
from loom.etl.declarative.expr._predicate_dialect import PredicateDialect, fold_predicate


class _PolarsPredicateDialect(PredicateDialect[pl.Expr]):
    """Render predicate nodes as Polars expressions."""

    def column(self, name: str) -> pl.Expr:
        return pl.col(name)

    def literal(self, value: Any) -> pl.Expr:
        return pl.lit(value)

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


_POLARS_PREDICATE_DIALECT = _PolarsPredicateDialect()


def predicate_to_polars(node: PredicateNode, params_instance: Any) -> pl.Expr:
    """Convert a :data:`~loom.etl._predicate.PredicateNode` to a ``polars.Expr``.

    :class:`~loom.etl._proxy.ParamExpr` leaf nodes are resolved against
    *params_instance* at call time.

    Args:
        node:            Root predicate node from the column / param DSL.
        params_instance: Concrete params used to resolve
                         :class:`~loom.etl._proxy.ParamExpr` leaves.

    Returns:
        Polars expression suitable for :meth:`polars.LazyFrame.filter`.

    Raises:
        TypeError: When an unsupported node type is encountered.

    Example::

        from loom.etl import col, params
        from loom.etl.engines.polars._predicate import predicate_to_polars

        node = (col("year") == params.run_date.year) & (col("month") == params.run_date.month)
        expr = predicate_to_polars(node, DailyParams(run_date=date(2024, 1, 15)))
        # → (pl.col("year") == 2024) & (pl.col("month") == 1)
    """
    return fold_predicate(node, params_instance, _POLARS_PREDICATE_DIALECT)
