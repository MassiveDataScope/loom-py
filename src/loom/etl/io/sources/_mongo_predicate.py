"""MongoDB predicate utilities.

Two public functions:

- :func:`materialize_filter` — resolves :class:`SourceRef` nodes in a
  predicate tree to concrete value lists.  Called by the executor before
  passing the spec to :class:`MongoSourceReader`.

- :func:`predicate_to_mongo` — converts a fully-materialised
  :class:`~loom.core.expr.nodes.ExprNode` to a pymongo filter dict.
"""

from __future__ import annotations

from functools import singledispatch
from typing import Any

from loom.core.errors import InvalidType, UnknownKey
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


def _is_source_ref(value: object) -> bool:
    return isinstance(value, SourceRef)


def materialize_filter(
    node: Any,
    resolved_frames: dict[str, Any],
) -> Any:
    """Replace ``SourceRef`` nodes in *node* with concrete value tuples.

    Walks the predicate tree and, for every ``InExpr`` whose ``values`` is a
    :class:`SourceRef`, extracts the referenced column from the already-resolved
    DataFrame and replaces the ``SourceRef`` with a plain ``tuple`` of values.

    Args:
        node:             A :class:`~loom.core.expr.nodes.ExprNode` predicate.
        resolved_frames:  Mapping of source alias → already-read ``pl.DataFrame``.

    Returns:
        A new predicate node (or the original if no ``SourceRef`` was found).
    """
    return _materialize(node, resolved_frames)


@singledispatch
def _materialize(node: object, frames: dict[str, Any]) -> Any:
    return node


@_materialize.register(InExpr)
def _(node: InExpr, frames: dict[str, Any]) -> InExpr:
    if not _is_source_ref(node.values):
        return node
    ref = node.values
    source = ref.source
    alias = getattr(source, "alias", None) or getattr(
        getattr(source, "_to_spec", lambda a: None)("__mongo_ref__"),
        "temp_name",
        None,
    )
    df = None
    if alias and alias in frames:
        df = frames[alias]
    else:
        for frame_df in frames.values():
            df = frame_df
            break
    if df is None:
        raise UnknownKey(
            f"SourceRef.source could not be resolved — no matching frame found. "
            f"Available aliases: {list(frames.keys())}"
        )
    values = tuple(df[ref.col].to_list())
    return InExpr(ref=node.ref, values=values)


@_materialize.register(AndExpr)
def _(node: AndExpr, frames: dict[str, Any]) -> AndExpr:
    return AndExpr(
        left=_materialize(node.left, frames),
        right=_materialize(node.right, frames),
    )


@_materialize.register(OrExpr)
def _(node: OrExpr, frames: dict[str, Any]) -> OrExpr:
    return OrExpr(
        left=_materialize(node.left, frames),
        right=_materialize(node.right, frames),
    )


@_materialize.register(NotExpr)
def _(node: NotExpr, frames: dict[str, Any]) -> NotExpr:
    return NotExpr(operand=_materialize(node.operand, frames))


def predicate_to_mongo(node: Any, params_instance: Any) -> dict[str, Any]:
    """Convert a fully-materialised predicate node to a pymongo filter dict.

    ``SourceRef`` nodes must have been resolved by :func:`materialize_filter`
    before calling this function — any remaining ``SourceRef`` will raise
    ``TypeError``.

    Args:
        node:             A :class:`~loom.core.expr.nodes.ExprNode` predicate.
        params_instance:  ETL params instance for resolving
                          :class:`~loom.etl.declarative.expr.ParamExpr` leaves.

    Returns:
        A dict suitable for passing to ``collection.find(filter_dict)``.
    """
    return _to_mongo(node, params_instance)


@singledispatch
def _to_mongo(node: object, params: Any) -> dict[str, Any]:
    raise InvalidType(
        f"predicate_to_mongo: unsupported predicate node type {type(node)!r}. "
        "Call materialize_filter() before predicate_to_mongo() if the predicate "
        "contains SourceRef nodes."
    )


def _col(ref: Any) -> str:
    if isinstance(ref, UnboundColumnRef):
        return ref.name
    if isinstance(ref, str):
        return ref
    raise InvalidType(f"predicate_to_mongo: expected column reference, got {type(ref)!r}")


def _val(value: Any, params: Any) -> Any:
    if isinstance(value, ParamExpr):
        return resolve_param_expr(value, params)
    return value


@_to_mongo.register(EqExpr)
def _(node: EqExpr, params: Any) -> dict[str, Any]:
    return {_col(node.left): _val(node.right, params)}


@_to_mongo.register(NeExpr)
def _(node: NeExpr, params: Any) -> dict[str, Any]:
    return {_col(node.left): {"$ne": _val(node.right, params)}}


@_to_mongo.register(GtExpr)
def _(node: GtExpr, params: Any) -> dict[str, Any]:
    return {_col(node.left): {"$gt": _val(node.right, params)}}


@_to_mongo.register(GeExpr)
def _(node: GeExpr, params: Any) -> dict[str, Any]:
    return {_col(node.left): {"$gte": _val(node.right, params)}}


@_to_mongo.register(LtExpr)
def _(node: LtExpr, params: Any) -> dict[str, Any]:
    return {_col(node.left): {"$lt": _val(node.right, params)}}


@_to_mongo.register(LeExpr)
def _(node: LeExpr, params: Any) -> dict[str, Any]:
    return {_col(node.left): {"$lte": _val(node.right, params)}}


@_to_mongo.register(InExpr)
def _(node: InExpr, params: Any) -> dict[str, Any]:
    raw = node.values
    if _is_source_ref(raw):
        raise InvalidType(
            "predicate_to_mongo: InExpr.values is a SourceRef — call "
            "materialize_filter() before predicate_to_mongo()."
        )
    values = list(resolve_param_expr(raw, params)) if isinstance(raw, ParamExpr) else list(raw)
    return {_col(node.ref): {"$in": values}}


@_to_mongo.register(AndExpr)
def _(node: AndExpr, params: Any) -> dict[str, Any]:
    return {"$and": [_to_mongo(node.left, params), _to_mongo(node.right, params)]}


@_to_mongo.register(OrExpr)
def _(node: OrExpr, params: Any) -> dict[str, Any]:
    return {"$or": [_to_mongo(node.left, params), _to_mongo(node.right, params)]}


@_to_mongo.register(NotExpr)
def _(node: NotExpr, params: Any) -> dict[str, Any]:
    return {"$nor": [_to_mongo(node.operand, params)]}


__all__ = ["materialize_filter", "predicate_to_mongo"]
