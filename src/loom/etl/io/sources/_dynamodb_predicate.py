"""DynamoDB predicate utilities.

Public function:

- :func:`predicate_to_dynamo` — converts a fully-materialised
  :class:`~loom.core.expr.nodes.ExprNode` to a DynamoDB
  :class:`DynamoFilterExpression` (``FilterExpression`` string plus aliased
  attribute names and pre-serialized attribute values).

``SourceRef`` resolution is source-agnostic and shared with MongoDB:
:func:`materialize_filter` is re-exported from
:mod:`loom.etl.io.sources._mongo_predicate`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from functools import singledispatch
from typing import Any

from loom.core.expr.nodes import (
    AndExpr,
    EqExpr,
    GeExpr,
    GtExpr,
    InExpr,
    LeExpr,
    LtExpr,
    NeExpr,
    NotExpr,
    OrExpr,
)
from loom.etl.declarative.expr._params import ParamExpr, resolve_param_expr
from loom.etl.declarative.expr._refs import UnboundColumnRef
from loom.etl.declarative.source._from_mongo import SourceRef
from loom.etl.io.sources._mongo_predicate import materialize_filter

try:
    from boto3.dynamodb import types as _ddb_types  # type: ignore[import-untyped]
except ImportError:  # pragma: no cover
    _ddb_types = None


@dataclass(frozen=True)
class DynamoFilterExpression:
    """Compiled DynamoDB filter — expression string plus aliased names/values.

    Attribute names are always aliased (``#n0``, ``#n1``, ...) and values are
    always placeholders (``:v0``, ``:v1``, ...) so DynamoDB reserved words
    never break the expression.

    Args:
        expression:       ``FilterExpression`` string, e.g. ``"#n0 = :v0"``.
        attribute_names:  ``ExpressionAttributeNames`` mapping alias → name.
        attribute_values: ``ExpressionAttributeValues`` mapping placeholder →
                          DynamoDB AttributeValue dict (already serialized).
    """

    expression: str
    attribute_names: dict[str, str]
    attribute_values: dict[str, dict[str, Any]]


def predicate_to_dynamo(node: Any, params_instance: Any) -> DynamoFilterExpression:
    """Convert a fully-materialised predicate node to a DynamoDB filter.

    ``SourceRef`` nodes must have been resolved by :func:`materialize_filter`
    before calling this function — any remaining ``SourceRef`` will raise
    ``TypeError``.

    Args:
        node:             A :class:`~loom.core.expr.nodes.ExprNode` predicate.
        params_instance:  ETL params instance for resolving
                          :class:`~loom.etl.declarative.expr.ParamExpr` leaves.

    Returns:
        A :class:`DynamoFilterExpression` ready to pass to ``client.scan()``.

    Raises:
        TypeError:    On unsupported node types or unresolved ``SourceRef``.
        RuntimeError: When boto3 is not installed.
    """
    builder = _ExpressionBuilder(params_instance)
    expression = _to_dynamo(node, builder)
    return DynamoFilterExpression(
        expression=expression,
        attribute_names=builder.names,
        attribute_values=builder.values,
    )


def _to_dynamo_value(value: Any) -> Any:
    """Coerce Python values to types accepted by boto3's ``TypeSerializer``."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _to_dynamo_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_to_dynamo_value(item) for item in value]
    return value


class _ExpressionBuilder:
    """Threads monotonic name/value alias counters through translation."""

    __slots__ = ("names", "values", "params", "_serializer")

    def __init__(self, params: Any) -> None:
        if _ddb_types is None:
            raise RuntimeError(
                "predicate_to_dynamo requires boto3. "
                "Install it with: pip install 'loom-kernel[aws]'"
            )
        self.names: dict[str, str] = {}
        self.values: dict[str, dict[str, Any]] = {}
        self.params = params
        self._serializer = _ddb_types.TypeSerializer()

    def name(self, ref: Any) -> str:
        alias = f"#n{len(self.names)}"
        self.names[alias] = _col(ref)
        return alias

    def value(self, raw: Any) -> str:
        resolved = resolve_param_expr(raw, self.params) if isinstance(raw, ParamExpr) else raw
        placeholder = f":v{len(self.values)}"
        self.values[placeholder] = self._serializer.serialize(_to_dynamo_value(resolved))
        return placeholder


def _col(ref: Any) -> str:
    if isinstance(ref, UnboundColumnRef):
        return ref.name
    if isinstance(ref, str):
        return ref
    raise TypeError(f"predicate_to_dynamo: expected column reference, got {type(ref)!r}")


@singledispatch
def _to_dynamo(node: object, builder: _ExpressionBuilder) -> str:
    raise TypeError(
        f"predicate_to_dynamo: unsupported predicate node type {type(node)!r}. "
        "Call materialize_filter() before predicate_to_dynamo() if the predicate "
        "contains SourceRef nodes."
    )


def _comparison(node: Any, op: str, builder: _ExpressionBuilder) -> str:
    return f"{builder.name(node.left)} {op} {builder.value(node.right)}"


@_to_dynamo.register(EqExpr)
def _(node: EqExpr, builder: _ExpressionBuilder) -> str:
    return _comparison(node, "=", builder)


@_to_dynamo.register(NeExpr)
def _(node: NeExpr, builder: _ExpressionBuilder) -> str:
    return _comparison(node, "<>", builder)


@_to_dynamo.register(GtExpr)
def _(node: GtExpr, builder: _ExpressionBuilder) -> str:
    return _comparison(node, ">", builder)


@_to_dynamo.register(GeExpr)
def _(node: GeExpr, builder: _ExpressionBuilder) -> str:
    return _comparison(node, ">=", builder)


@_to_dynamo.register(LtExpr)
def _(node: LtExpr, builder: _ExpressionBuilder) -> str:
    return _comparison(node, "<", builder)


@_to_dynamo.register(LeExpr)
def _(node: LeExpr, builder: _ExpressionBuilder) -> str:
    return _comparison(node, "<=", builder)


@_to_dynamo.register(InExpr)
def _(node: InExpr, builder: _ExpressionBuilder) -> str:
    raw = node.values
    if isinstance(raw, SourceRef):
        raise TypeError(
            "predicate_to_dynamo: InExpr.values is a SourceRef — call "
            "materialize_filter() before predicate_to_dynamo()."
        )
    values = (
        list(resolve_param_expr(raw, builder.params)) if isinstance(raw, ParamExpr) else list(raw)
    )
    if not values:
        raise ValueError("predicate_to_dynamo: IN requires at least one value.")
    placeholders = ", ".join(builder.value(v) for v in values)
    return f"{builder.name(node.ref)} IN ({placeholders})"


@_to_dynamo.register(AndExpr)
def _(node: AndExpr, builder: _ExpressionBuilder) -> str:
    return f"({_to_dynamo(node.left, builder)}) AND ({_to_dynamo(node.right, builder)})"


@_to_dynamo.register(OrExpr)
def _(node: OrExpr, builder: _ExpressionBuilder) -> str:
    return f"({_to_dynamo(node.left, builder)}) OR ({_to_dynamo(node.right, builder)})"


@_to_dynamo.register(NotExpr)
def _(node: NotExpr, builder: _ExpressionBuilder) -> str:
    return f"NOT ({_to_dynamo(node.operand, builder)})"


__all__ = ["DynamoFilterExpression", "materialize_filter", "predicate_to_dynamo"]
