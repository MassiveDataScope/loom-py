"""Shared predicate fold with pluggable render dialects."""

from __future__ import annotations

from functools import singledispatch
from typing import Any, Protocol, TypeVar

from loom.core.errors import InvalidType
from loom.etl.declarative.expr._params import ParamExpr, resolve_param_expr
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
    PredicateNode,
)
from loom.etl.declarative.expr._refs import UnboundColumnRef

T = TypeVar("T")


class PredicateDialect(Protocol[T]):
    """Backend renderer for predicate fold output type ``T``."""

    def column(self, name: str) -> T: ...

    def literal(self, value: Any) -> T: ...

    def eq(self, left: T, right: T) -> T: ...

    def ne(self, left: T, right: T) -> T: ...

    def gt(self, left: T, right: T) -> T: ...

    def ge(self, left: T, right: T) -> T: ...

    def lt(self, left: T, right: T) -> T: ...

    def le(self, left: T, right: T) -> T: ...

    def in_(self, ref: T, values: tuple[Any, ...]) -> T: ...

    def and_(self, left: T, right: T) -> T: ...

    def or_(self, left: T, right: T) -> T: ...

    def not_(self, operand: T) -> T: ...


def fold_predicate(node: PredicateNode, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    """Fold a predicate AST using a backend-specific dialect.

    With ``params_instance=None`` the params are not resolved: a params reference
    reaches the dialect as the :class:`ParamExpr` itself, and a params list as ``()``.
    """
    return _fold(node, params_instance, dialect)


class _ColumnNames:
    """Dialect whose fold result is the column names a predicate refers to."""

    def column(self, name: str) -> tuple[str, ...]:
        return (name,)

    def literal(self, value: Any) -> tuple[str, ...]:
        return ()

    def eq(self, left: tuple[str, ...], right: tuple[str, ...]) -> tuple[str, ...]:
        return left + right

    ne = gt = ge = lt = le = and_ = or_ = eq

    def in_(self, ref: tuple[str, ...], values: tuple[Any, ...]) -> tuple[str, ...]:
        return ref

    def not_(self, operand: tuple[str, ...]) -> tuple[str, ...]:
        return operand


def columns_in(*predicates: PredicateNode) -> tuple[str, ...]:
    """The columns the predicates refer to, in order of appearance, once each."""
    names = (name for node in predicates for name in fold_predicate(node, None, _ColumnNames()))
    return tuple(dict.fromkeys(names))


@singledispatch
def _fold(node: object, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    raise InvalidType(f"fold_predicate: unsupported node type {type(node)!r}")


def _term(value: Any, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    if isinstance(value, UnboundColumnRef):
        return dialect.column(value.name)
    if isinstance(value, ParamExpr) and params_instance is not None:
        return dialect.literal(resolve_param_expr(value, params_instance))
    return dialect.literal(value)


def _in_values(values: Any, params_instance: Any) -> tuple[Any, ...]:
    if not isinstance(values, ParamExpr):
        return tuple(values)
    return () if params_instance is None else tuple(resolve_param_expr(values, params_instance))


def _binary(
    op: str,
    left: Any,
    right: Any,
    params_instance: Any,
    dialect: PredicateDialect[T],
) -> T:
    lhs = _term(left, params_instance, dialect)
    rhs = _term(right, params_instance, dialect)
    if op == "eq":
        return dialect.eq(lhs, rhs)
    if op == "ne":
        return dialect.ne(lhs, rhs)
    if op == "gt":
        return dialect.gt(lhs, rhs)
    if op == "ge":
        return dialect.ge(lhs, rhs)
    if op == "lt":
        return dialect.lt(lhs, rhs)
    if op == "le":
        return dialect.le(lhs, rhs)
    raise InvalidType("fold_predicate: unsupported binary operator")


@_fold.register(EqPred)
def _(node: EqPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("eq", node.left, node.right, params_instance, dialect)


@_fold.register(NePred)
def _(node: NePred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("ne", node.left, node.right, params_instance, dialect)


@_fold.register(GtPred)
def _(node: GtPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("gt", node.left, node.right, params_instance, dialect)


@_fold.register(GePred)
def _(node: GePred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("ge", node.left, node.right, params_instance, dialect)


@_fold.register(LtPred)
def _(node: LtPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("lt", node.left, node.right, params_instance, dialect)


@_fold.register(LePred)
def _(node: LePred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return _binary("le", node.left, node.right, params_instance, dialect)


@_fold.register(InPred)
def _(node: InPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    values = _in_values(node.values, params_instance)
    ref = _term(node.ref, params_instance, dialect)
    return dialect.in_(ref, values)


@_fold.register(AndPred)
def _(node: AndPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return dialect.and_(
        _fold(node.left, params_instance, dialect),
        _fold(node.right, params_instance, dialect),
    )


@_fold.register(OrPred)
def _(node: OrPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return dialect.or_(
        _fold(node.left, params_instance, dialect),
        _fold(node.right, params_instance, dialect),
    )


@_fold.register(NotPred)
def _(node: NotPred, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    return dialect.not_(_fold(node.operand, params_instance, dialect))


__all__ = ["PredicateDialect", "columns_in", "fold_predicate"]
