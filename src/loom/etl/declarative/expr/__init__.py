"""Declarative DSL primitives used by ETL source/target builders.

This package owns the symbolic language used at class-definition time:

* table/column references (:class:`TableRef`, :func:`col`)
* params expressions (:data:`params`, :class:`ParamExpr`)
* predicate AST nodes and operators
"""

from __future__ import annotations

from ._params import ParamExpr, params, resolve_param_expr
from ._predicate import (
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
    PredicateNode,
)
from ._predicate_dialect import PredicateDialect, fold_predicate
from ._refs import TableRef, UnboundColumnRef, col

__all__ = [
    "PredicateNode",
    "EqPred",
    "NePred",
    "GtPred",
    "GePred",
    "LtPred",
    "LePred",
    "InPred",
    "AndPred",
    "OrPred",
    "NotPred",
    "PredicateDialect",
    "fold_predicate",
    "TableRef",
    "UnboundColumnRef",
    "col",
    "ParamExpr",
    "params",
    "resolve_param_expr",
]
