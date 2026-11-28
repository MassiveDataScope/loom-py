"""Shared predicate fold with pluggable render dialects."""

from __future__ import annotations

from collections.abc import Iterator
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
    """Fold a predicate AST with a backend dialect, resolving params from *params_instance*."""
    return _fold(node, params_instance, dialect)


def columns_in(*predicates: PredicateNode) -> tuple[str, ...]:
    """The columns the predicates refer to, in order of appearance, once each."""
    return tuple(dict.fromkeys(name for node in predicates for name in _columns(node)))


def _columns(node: PredicateNode) -> Iterator[str]:
    match node:
        case AndPred(left=left, right=right) | OrPred(left=left, right=right):
            yield from _columns(left)
            yield from _columns(right)
        case NotPred(operand=operand):
            yield from _columns(operand)
        case InPred(ref=ref):
            yield from _column_name(ref)
        case (
            EqPred(left=left, right=right)
            | NePred(left=left, right=right)
            | GtPred(left=left, right=right)
            | GePred(left=left, right=right)
            | LtPred(left=left, right=right)
            | LePred(left=left, right=right)
        ):
            yield from _column_name(left)
            yield from _column_name(right)
        case _:
            raise InvalidType(f"columns_in: unsupported node type {type(node)!r}")


def _column_name(term: Any) -> Iterator[str]:
    if isinstance(term, UnboundColumnRef):
        yield term.name


@singledispatch
def _fold(node: object, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    raise InvalidType(f"fold_predicate: unsupported node type {type(node)!r}")


def _term(value: Any, params_instance: Any, dialect: PredicateDialect[T]) -> T:
    if isinstance(value, UnboundColumnRef):
        return dialect.column(value.name)
    if isinstance(value, ParamExpr):
        return dialect.literal(resolve_param_expr(value, params_instance))
    return dialect.literal(value)


def _in_values(values: Any, params_instance: Any) -> tuple[Any, ...]:
    if isinstance(values, ParamExpr):
        return tuple(resolve_param_expr(values, params_instance))
    return tuple(values)


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
