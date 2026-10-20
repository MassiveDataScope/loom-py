"""Bytewax node handler dispatcher for streaming DSL steps."""

from __future__ import annotations

from collections.abc import Callable
from types import MappingProxyType
from typing import Any, TypeAlias

from loom.streaming.bytewax.handlers._shared import (
    _BuildContextProtocol,
)
from loom.streaming.bytewax.handlers.boundary import _apply_into_topic
from loom.streaming.bytewax.handlers.routing import (
    _apply_broadcast,
    _apply_expand_routes,
    _apply_fork,
    _apply_router,
)
from loom.streaming.bytewax.handlers.scopes import _apply_with, _apply_with_async
from loom.streaming.bytewax.handlers.shapes import (
    _apply_collect_batch,
    _apply_drain,
    _apply_for_each,
)
from loom.streaming.bytewax.handlers.steps import (
    _apply_batch_expand_step,
    _apply_batch_step,
    _apply_expand_step,
    _apply_explode,
    _apply_record_step,
)
from loom.streaming.bytewax.handlers.storage import _apply_into_sink
from loom.streaming.core._exceptions import UnsupportedNodeError
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._decompose import Explode
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._fork import Fork
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach
from loom.streaming.nodes._sink import IntoSink
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep
from loom.streaming.nodes._with import With, WithAsync

Stream: TypeAlias = Any

NodeHandler: TypeAlias = Callable[[Stream, object, int, _BuildContextProtocol], Stream]


def _wire_process(
    stream: Stream,
    nodes: tuple[object, ...],
    ctx: _BuildContextProtocol,
    *,
    path_prefix: tuple[int, ...] = (),
    start: int = 0,
) -> Stream:
    """Wire one process subtree under a path prefix, from the node at index *start*.

    Node indices, and so compiled paths, stay those of the full *nodes*.
    """
    for idx, node in enumerate(nodes[start:], start):
        with ctx.enter_path(path_prefix + (idx,)):
            stream = _wire_node(stream, node, idx, ctx)
    return stream


def _wire_node(stream: Stream, node: object, idx: int, ctx: _BuildContextProtocol) -> Stream:
    """Dispatch one DSL node to its Bytewax handler."""
    for handler_type, handler in _NODE_HANDLERS.items():
        if isinstance(node, handler_type):
            return handler(stream, node, idx, ctx)
    raise UnsupportedNodeError(f"No adapter handler for {type(node).__name__}")


_NODE_HANDLERS: MappingProxyType[type[object], NodeHandler] = MappingProxyType(
    {
        RecordStep: _apply_record_step,
        BatchStep: _apply_batch_step,
        ExpandStep: _apply_expand_step,
        BatchExpandStep: _apply_batch_expand_step,
        Explode: _apply_explode,
        With: _apply_with,
        WithAsync: _apply_with_async,
        CollectBatch: _apply_collect_batch,
        ForEach: _apply_for_each,
        Fork: _apply_fork,
        Router: _apply_router,
        Broadcast: _apply_broadcast,
        ExpandRoutes: _apply_expand_routes,
        Drain: _apply_drain,
        IntoTopic: _apply_into_topic,
        IntoSink: _apply_into_sink,
    }
)
