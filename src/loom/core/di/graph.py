"""The dependency graph of a container, and its structural analysis.

A node is a container binding (:class:`KeyNode`) or a class loom builds
itself, such as a use case, a job or a callback (:class:`ClassNode`). An
edge is one constructor parameter; a *call edge* reaches a class another
execution runs, so a singleton above it holds nothing of that execution.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Collection, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, TypeGuard

from loom.core.di.container import LoomContainer, ResolutionError, _Binding, _describe_key
from loom.core.di.errors import DependencyUnresolvable
from loom.core.di.introspection import (
    Constructor,
    ExecutionDependency,
    KeyDependency,
    RefusedDependency,
    RepoDependency,
    UnresolvableDependency,
    constructor_dependencies,
)
from loom.core.di.scope import Scope


@dataclass(frozen=True, slots=True)
class KeyNode:
    """The binding under ``key``."""

    key: object

    def __str__(self) -> str:
        return _describe_key(self.key)


@dataclass(frozen=True, slots=True)
class ClassNode:
    """A class loom builds itself."""

    cls: type[Any]

    def __str__(self) -> str:
        return self.cls.__qualname__


Node = KeyNode | ClassNode


@dataclass(frozen=True, slots=True)
class Edge:
    """The constructor parameter ``name`` of one node, reaching ``target``."""

    target: Node
    name: str
    call_edge: bool = False


@dataclass(frozen=True, slots=True)
class Path:
    """The edges walked from ``root`` to the node under analysis, which ends it."""

    root: Node
    edges: tuple[Edge, ...] = ()

    def to(self, edge: Edge) -> Path:
        """This path followed by *edge*."""
        return Path(self.root, (*self.edges, edge))

    def __str__(self) -> str:
        steps = (f" -> {edge.name}: {edge.target}" for edge in self.edges)
        return f"{self.root}{''.join(steps)}"


class DependencyGraphError(ResolutionError):
    """The dependency graph is refused; the message holds the path from a root."""


class DependencyGraph:
    """The nodes a container's bindings and loom's own classes form, read on demand.

    Args:
        container: The container whose bindings, repository mappings,
            execution kinds and key classifier the graph reads.
    """

    __slots__ = ("_constructors", "_container", "_edges")

    def __init__(self, container: LoomContainer) -> None:
        self._container = container
        self._constructors: dict[Node, Constructor] = {}
        self._edges: dict[Node, tuple[Edge, ...]] = {}

    def edges(self, node: Node) -> tuple[Edge, ...]:
        """The one source of edges, derived once per node; an unmapped repository gives none.

        :func:`refuse_unmapped_repository` refuses the nodes whose unmapped repository
        nothing else reports.
        """
        edges = self._edges.get(node)
        if edges is None:
            edges = self._edges[node] = tuple(self._derive(node))
        return edges

    def constructor(self, node: Node) -> Constructor:
        """The dependencies of the constructor loom calls for *node*; none for a provider."""
        constructor = self._constructors.get(node)
        if constructor is None:
            constructor = self._constructors[node] = self._introspect(node)
        return constructor

    def scope(self, node: Node) -> Scope | None:
        """The scope of *node*'s binding; ``None`` for a class or an unbound key."""
        binding = self._binding(node)
        return None if binding is None else binding.scope

    def repository(self, model: type[Any]) -> KeyNode | None:
        """The binding *model*'s repository is mapped to; ``None`` when no mapping is."""
        key = self._container._repo_bindings.get(model)
        return None if key is None else KeyNode(key)

    def _binding(self, node: Node) -> _Binding | None:
        match node:
            case KeyNode(key):
                return self._container._bindings.get(key)
            case ClassNode():
                return None

    def _introspect(self, node: Node) -> Constructor:
        """None for a provider, or a class with a custom ``__new__`` or metaclass ``__call__``."""
        match node:
            case ClassNode(cls):
                built: object = cls
            case KeyNode():
                binding = self._binding(node)
                built = None if binding is None else binding.provider
        if not _built_by_type(built):
            return ()
        return constructor_dependencies(built, self._container.kinds, self._container._classify_key)

    def _derive(self, node: Node) -> Iterator[Edge]:
        for name, dependency in self.constructor(node):
            match dependency:
                case KeyDependency(key):
                    yield Edge(KeyNode(key), name)
                case RepoDependency(model) if (repository := self.repository(model)) is not None:
                    yield Edge(repository, name)
                case ExecutionDependency(kind=kind, annotation=annotation):
                    yield from (Edge(ClassNode(t), name, True) for t in kind.targets(annotation))


def _built_by_type(built: object) -> TypeGuard[type[Any]]:
    """Whether calling *built* surely runs its ``__init__``: a class with no custom construction.

    A metaclass ``__call__`` or a ``__new__`` may return anything, so either one
    leaves the class opaque.
    """
    return (
        inspect.isclass(built)
        and type(built).__call__ is type.__call__
        and built.__new__ is object.__new__
    )


@dataclass(frozen=True, slots=True)
class GraphAnalysis:
    """Snapshot taken at the end of registration: the roots, what they reach, the warnings."""

    roots: tuple[Node, ...]
    reachable: frozenset[Node]
    warnings: tuple[DependencyUnresolvable, ...]
    graph: DependencyGraph


Rule = Callable[[Node, Path, DependencyGraph], str | None]
"""One structural refusal: why the node ending the path is refused, or ``None``.

A rule may read the path only through whether a singleton holds its end and
which nodes are on it, the two things :func:`analyse` memoises a check by. A
check that needs more of the path (refusal 8) is a walk of its own; one that
needs other facts (refusal 7) is a second :func:`analyse` with its own rules.
"""

_PER_EXECUTION = (Scope.REQUEST, Scope.TRANSACTION)


def _held_by_singleton(path: Path, graph: DependencyGraph) -> bool:
    """Whether an ``APPLICATION`` binding holds the end of *path*; a call edge stops the search."""
    for edge in reversed(path.edges):
        if graph.scope(edge.target) is Scope.APPLICATION:
            return True
        if edge.call_edge:
            return False
    return False


def _refuse_missing_binding(node: Node, path: Path, graph: DependencyGraph) -> str | None:
    """Refusal 1: a key reached from a root has no binding; a class never misses one."""
    match node:
        case KeyNode() if graph.scope(node) is None:
            return "no binding is registered for it"
    return None


def _refuse_cycle(node: Node, path: Path, graph: DependencyGraph) -> str | None:
    """Refusal 2: the construction since the last call edge reaches a node it already holds.

    A call edge runs its target in another execution, so a loop through one builds.
    """
    nodes = (path.root, *(edge.target for edge in path.edges))
    start = max((i + 1 for i, edge in enumerate(path.edges) if edge.call_edge), default=0)
    return "dependency cycle" if node in nodes[start:-1] else None


def _refuse_captive(node: Node, path: Path, graph: DependencyGraph) -> str | None:
    """Refusal 3: a singleton holds a binding built per execution."""
    if graph.scope(node) in _PER_EXECUTION and _held_by_singleton(path, graph):
        return "an APPLICATION binding cannot hold a per-execution one"
    return None


def _refuse_unsafe_kind(node: Node, path: Path, graph: DependencyGraph) -> str | None:
    """Refusal 4: a singleton holds a kind whose value belongs to one execution."""
    for name, dependency in graph.constructor(node):
        match dependency:
            case ExecutionDependency(kind=kind) if not kind.singleton_safe:
                if _held_by_singleton(path, graph):
                    return f"{name}: an APPLICATION binding cannot hold a {kind.name!r} value"
    return None


def _refuse_declaration(node: Node, path: Path, graph: DependencyGraph) -> str | None:
    """Refusal 5: a constructor declares what only ``execute()`` may."""
    for name, dependency in graph.constructor(node):
        match dependency:
            case RefusedDependency(reason):
                return f"{name}: {reason}"
    return None


def _refuse_unresolvable_root(node: Node, path: Path, graph: DependencyGraph) -> str | None:
    """Refusal 6: a required parameter of a root names nothing the container serves."""
    if path.edges:
        return None
    return next(_unresolvable(node, graph), None)


def refuse_unmapped_repository(nodes: Collection[Node]) -> Rule:
    """Refusal 1 for a repository: a constructor of one of *nodes* asks for an unmapped model.

    Only *nodes* are checked, so a caller that reports an unmapped repository
    of its own classes later keeps its error.
    """

    def refuse(node: Node, path: Path, graph: DependencyGraph) -> str | None:
        if node not in nodes:
            return None
        unmapped = (
            f"{name}: no repository mapping is registered for {dependency.model.__qualname__}"
            for name, dependency in graph.constructor(node)
            if isinstance(dependency, RepoDependency) and graph.repository(dependency.model) is None
        )
        return next(unmapped, None)

    return refuse


LENIENT_REFUSALS: tuple[Rule, ...] = (
    _refuse_cycle,
    _refuse_captive,
    _refuse_unsafe_kind,
    _refuse_declaration,
    _refuse_unresolvable_root,
)
"""The refusals a lenient composition keeps: all but the missing binding."""

REFUSALS: tuple[Rule, ...] = (_refuse_missing_binding, *LENIENT_REFUSALS)
"""The structural refusals, in the order they win."""


def analyse(
    graph: DependencyGraph, roots: Iterable[Node], rules: tuple[Rule, ...] = REFUSALS
) -> GraphAnalysis:
    """Walk *graph* from each of *roots* and run *rules* on every node reached.

    A node is checked once per way of being reached: as a root or not, held
    by a singleton or not. What follows a call edge depends on nothing before
    it, so a loop back to a call target still being walked stops there.
    Bindings no root reaches are not analysed. An
    unresolvable parameter of a binding (not a root) is a warning, since the
    binding may never be built.

    Raises:
        DependencyGraphError: The first refusal, with its path.
    """
    roots = tuple(dict.fromkeys(roots))
    reachable: dict[Node, None] = {}
    checked: set[tuple[Node, bool, bool]] = set()
    entered: set[tuple[Node, bool, bool]] = set()

    def visit(node: Node, path: Path) -> None:
        state = (node, _held_by_singleton(path, graph), not path.edges)
        if state in checked:
            return
        for rule in rules:
            reason = rule(node, path, graph)
            if reason is not None:
                raise DependencyGraphError(f"{path}: {reason}")
        if state in entered and path.edges[-1].call_edge:
            return  # a loop through this call: its target's frame, still open, checks what follows
        entered.add(state)
        reachable[node] = None
        for edge in graph.edges(node):
            visit(edge.target, path.to(edge))
        checked.add(state)

    for root in roots:
        visit(root, Path(root))
    warnings = tuple(
        DependencyUnresolvable(f"{node} -> {reason}")
        for node in reachable
        if node not in roots
        for reason in _unresolvable(node, graph)
    )
    return GraphAnalysis(
        roots=roots, reachable=frozenset(reachable), warnings=warnings, graph=graph
    )


def _unresolvable(node: Node, graph: DependencyGraph) -> Iterator[str]:
    """Why each required parameter of *node* names nothing the container serves."""
    for name, dependency in graph.constructor(node):
        match dependency:
            case UnresolvableDependency(None):
                yield f"{name} has no annotation"
            case UnresolvableDependency(annotation):
                yield f"{name}: {annotation!r} names nothing the container can serve"
