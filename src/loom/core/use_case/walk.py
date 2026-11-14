"""Every execution declaration a root reaches through the dependency graph.

A use case may declare a marker in its ``execute()`` or reach one through
what it is built with: a port whose adapter or factory declares
``Mcp('gateway')``, or a use case another execution kind calls. The startup
phases that grant, verify and close over those declarations read them here,
in one walk per root over an analysed graph, and split them by kind
themselves; the executor never walks.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from loom.core.di.graph import ClassNode, DependencyGraph, Node, Path
from loom.core.di.introspection import ExecutionDependency
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.plan import ExecutionBinding


@dataclass(frozen=True, slots=True)
class Reached:
    """One declaration a root reaches.

    Attributes:
        binding: The declaring parameter: its name, kind, declaration and annotation.
        where: The parameter name for one the root declares itself, else the
            path from the root to the declaring node then the name, as the
            graph's refusals word it: ``"Market -> queries: Queries: gateway"``.
    """

    binding: ExecutionBinding
    where: str


def reached(
    roots: Iterable[Node], graph: DependencyGraph, compiler: UseCaseCompiler
) -> dict[Node, tuple[Reached, ...]]:
    """Every declaration each of *roots* reaches, by root, in their order.

    Each root is walked once, breadth first, along every edge of
    :meth:`~loom.core.di.graph.DependencyGraph.edges`, call edges included,
    so a called use case adds its declarations to its caller's. Each node
    is visited once per root, under the first (shortest) path to it, so a
    declaration appears once per parameter and path. A node declares the
    execution kinds of its constructor or factory parameters, then those of
    its plan's ``execute()``. A binding with no edges, such as an instance or
    a zero-argument factory, ends the walk there. A root reaching nothing
    maps to ``()``.
    """
    return {root: tuple(_walk(root, graph, compiler)) for root in roots}


def _walk(root: Node, graph: DependencyGraph, compiler: UseCaseCompiler) -> Iterator[Reached]:
    visited = {root}
    pending = deque([(root, Path(root))])
    while pending:
        node, path = pending.popleft()
        declared = _declared(node, graph, compiler)
        if declared:
            prefix = "" if node == root else f"{path}: "
            yield from (Reached(binding, prefix + binding.name) for binding in declared)
        for edge in graph.edges(node):
            if edge.target not in visited:
                visited.add(edge.target)
                pending.append((edge.target, path.to(edge)))


def _declared(
    node: Node, graph: DependencyGraph, compiler: UseCaseCompiler
) -> list[ExecutionBinding]:
    """The execution-kind parameters of *node*: its constructor's, then its plan's ``execute()``."""
    declared = [
        ExecutionBinding(name, dependency.kind, dependency.declaration, dependency.annotation)
        for name, dependency in graph.constructor(node)
        if isinstance(dependency, ExecutionDependency)
    ]
    plan = compiler.get_plan(node.cls) if isinstance(node, ClassNode) else None
    if plan is not None:
        declared.extend(plan.execution_bindings)
    return declared


__all__ = ["Reached", "reached"]
