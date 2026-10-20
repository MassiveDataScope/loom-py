"""Branch enumeration for the nodes that split a stream into several paths.

A branching node's branches are addressed by position: the compiler keys a
compiled sink by the branch index, the adapter wires the branch stream under
the same index, and the validator reports errors against the branch label.
:func:`iter_branches` is the single place those indices and labels come from.
An index is derived from insertion order: stable for a given source declaration,
renumbered when a route is inserted in the middle of a mapping.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, Final

from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._broadcast import Broadcast
from loom.streaming.nodes._expand_routes import ExpandRoutes
from loom.streaming.nodes._expr_eval import PredicateSpec
from loom.streaming.nodes._fork import Fork, ForkKind
from loom.streaming.nodes._router import Router
from loom.streaming.nodes._shape import Drain
from loom.streaming.nodes._sink import IntoSink


@dataclass(frozen=True)
class Branch:
    """One branch of a branching node.

    Args:
        index: Position of the branch inside its node. It is the path segment
            the compiler and every backend adapter must use for this branch.
        label: Human-readable branch identity used in compilation errors.
        nodes: Process nodes that make up the branch.
        key: Dispatch value that selects the branch — the route key of a keyed
            node, or the payload type of an ``ExpandRoutes`` route. ``None``
            for predicate and fallback branches.
        when: Predicate that selects the branch. ``None`` unless the branch
            comes from an ordered predicate route.
        output: Terminal topic the branch owns, declared on the branch itself
            rather than as one of its ``nodes``.
        is_default: Whether this is the fallback branch, reached when no other
            branch claims the message.
    """

    index: int
    label: str
    nodes: tuple[object, ...]
    key: object | None = None
    when: PredicateSpec[Any] | None = None
    output: IntoTopic[Any] | None = None
    is_default: bool = False


BRANCHING_NODE_TYPES: Final = (Fork, Router, Broadcast, ExpandRoutes)

PATH_END_NODE_TYPES: Final = (IntoTopic, IntoSink, Drain, Broadcast)
"""Node types that end the path of every message they receive; validation keeps them last."""
"""Node types whose branches occupy their own path segment."""

_DEFAULT_LABEL: Final = "default"


def is_branching_node(node: object) -> bool:
    """Return whether *node* splits its stream into indexed branches."""
    return isinstance(node, BRANCHING_NODE_TYPES)


def iter_branches(node: object) -> Iterator[Branch]:
    """Yield the branches of *node* in path-index order, fallback branch last.

    Yields nothing for a node that does not branch.
    """
    if isinstance(node, Fork):
        yield from _fork_branches(node)
    elif isinstance(node, Router):
        yield from _router_branches(node)
    elif isinstance(node, Broadcast):
        yield from _broadcast_branches(node)
    elif isinstance(node, ExpandRoutes):
        yield from _expand_routes_branches(node)


def _fork_branches(fork: Fork[Any]) -> Iterator[Branch]:
    index = 0
    if fork.kind is ForkKind.KEYED:
        for key, process in fork.routes.items():
            yield Branch(index=index, label=repr(key), nodes=process.nodes, key=key)
            index += 1
    else:
        for ordinal, route in enumerate(fork.predicate_routes):
            yield Branch(
                index=index,
                label=f"predicate[{ordinal}]",
                nodes=route.process.nodes,
                when=route.when,
            )
            index += 1
    if fork.default is not None:
        yield _default_branch(index, fork.default.nodes)


def _router_branches(router: Router[Any, Any]) -> Iterator[Branch]:
    index = 0
    for key, process in router.routes.items():
        yield Branch(index=index, label=repr(key), nodes=process.nodes, key=key)
        index += 1
    for ordinal, route in enumerate(router.predicate_routes):
        yield Branch(
            index=index,
            label=f"predicate[{ordinal}]",
            nodes=route.process.nodes,
            when=route.when,
        )
        index += 1
    if router.default is not None:
        yield _default_branch(index, router.default.nodes)


def _broadcast_branches(broadcast: Broadcast[Any]) -> Iterator[Branch]:
    for index, route in enumerate(broadcast.routes):
        yield Branch(
            index=index,
            label=str(index),
            nodes=route.process.nodes,
            output=route.output,
        )


def _expand_routes_branches(node: ExpandRoutes[Any]) -> Iterator[Branch]:
    index = 0
    for payload_type, process in node.routes.items():
        yield Branch(
            index=index,
            label=payload_type.__name__,
            nodes=process.nodes,
            key=payload_type,
        )
        index += 1
    if node.default is not None:
        yield _default_branch(index, node.default.nodes)


def _default_branch(index: int, nodes: tuple[object, ...]) -> Branch:
    return Branch(index=index, label=_DEFAULT_LABEL, nodes=nodes, is_default=True)


__all__ = [
    "BRANCHING_NODE_TYPES",
    "PATH_END_NODE_TYPES",
    "Branch",
    "is_branching_node",
    "iter_branches",
]
