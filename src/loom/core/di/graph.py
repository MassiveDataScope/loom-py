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
from typing import TYPE_CHECKING, Any, TypeGuard

from loom.core.di.errors import DependencyUnresolvable, ResolutionError, describe_key
from loom.core.di.introspection import (
    Constructor,
    ExecutionDependency,
    KeyClassifier,
    KeyDependency,
    RefusedDependency,
    RepoDependency,
    UnresolvableDependency,
    constructor_dependencies,
    plain_key,
)
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionKinds, Kind

if TYPE_CHECKING:
    from loom.core.di.container import LoomContainer, _Binding


@dataclass(frozen=True, slots=True)
class KeyNode:
    """The binding under ``key``."""

    key: object

    def __str__(self) -> str:
        return describe_key(self.key)


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


def _nothing_implicit(cls: type[Any]) -> Constructor:
    return ()


class DependencyGraph:
    """The nodes a container's bindings and loom's own classes form, read on demand.

    What it reads is memoised per node; the container forgets a key's entries
    when the key is registered again (:meth:`forget`).

    Args:
        container: The container whose bindings, repository mappings and
            execution kinds the graph reads.
        classify_key: How a constructor annotation becomes a key; defaults
            to :func:`~loom.core.di.introspection.plain_key`.
        implicit: The dependencies loom injects into a class it builds
            beyond those its signature declares, such as a use case's main
            repository; none by default.
    """

    __slots__ = ("_classify_key", "_constructors", "_container", "_edges", "_implicit")

    def __init__(
        self,
        container: LoomContainer,
        *,
        classify_key: KeyClassifier = plain_key,
        implicit: Callable[[type[Any]], Constructor] = _nothing_implicit,
    ) -> None:
        self._container = container
        self._classify_key = classify_key
        self._implicit = implicit
        self._constructors: dict[Node, Constructor] = {}
        self._edges: dict[Node, tuple[Edge, ...]] = {}

    def forget(self, key: object) -> None:
        """Drop what was read for the binding under *key*: it was registered again."""
        node = KeyNode(key)
        self._constructors.pop(node, None)
        self._edges.pop(node, None)

    def forget_edges(self) -> None:
        """Drop every memoised edge: a repository mapping changed, and edges follow mappings."""
        self._edges.clear()

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

    def unresolvable(self, node: Node) -> Iterator[str]:
        """Why each required parameter of *node* names nothing the container serves."""
        for name, dependency in self.constructor(node):
            match dependency:
                case UnresolvableDependency(None):
                    yield f"{name} has no annotation"
                case UnresolvableDependency(annotation):
                    yield f"{name}: {annotation!r} names nothing the container can serve"

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
        """None for a provider, or a class with a custom ``__new__`` or metaclass ``__call__``.

        A class loom builds itself adds what :attr:`_implicit` injects.
        """
        match node:
            case ClassNode(cls):
                built: object = cls
                implicit = self._implicit(cls)
            case KeyNode():
                binding = self._binding(node)
                built = None if binding is None else binding.provider
                implicit = ()
        if not _built_by_type(built):
            return implicit
        kinds = self._container.kinds
        return (*constructor_dependencies(built, kinds, self._classify_key), *implicit)

    def _derive(self, node: Node) -> Iterator[Edge]:
        for name, dependency in self.constructor(node):
            match dependency:
                case KeyDependency(key):
                    yield Edge(KeyNode(key), name)
                case RepoDependency(model, contract):
                    repository = self.repository(model)
                    if repository is not None:
                        yield Edge(repository, name)
                    if contract is not None:
                        yield Edge(KeyNode(contract), name)
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
    """Whether an ``APPLICATION`` binding holds the end of *path*; a call edge stops the search.

    The root counts too: a singleton rooting the walk holds what it reaches.
    """
    for edge in reversed(path.edges):
        if graph.scope(edge.target) is Scope.APPLICATION:
            return True
        if edge.call_edge:
            return False
    return graph.scope(path.root) is Scope.APPLICATION


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
    return next(graph.unresolvable(node), None)


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


def refuse_unbound(kinds: ExecutionKinds, skip: Collection[Kind] = ()) -> Rule:
    """Refusal 7: a singleton declares a kind of *kinds* no resolver is bound for.

    Only ``APPLICATION`` bindings: seal builds them, while a per-execution
    adapter fails at its first build, as an ``execute()`` marker does. A kind
    in *skip* is left to that first build too.
    """

    def refuse(node: Node, path: Path, graph: DependencyGraph) -> str | None:
        if graph.scope(node) is not Scope.APPLICATION:
            return None
        unbound = (
            f"{name}: no resolver is bound for {dependency.kind.name!r}"
            for name, dependency in graph.constructor(node)
            if isinstance(dependency, ExecutionDependency)
            and dependency.kind not in skip
            and not kinds.bound(dependency.kind)
        )
        return next(unbound, None)

    return refuse


def _refuse_ungranted_kind(node: Node, path: Path, graph: DependencyGraph) -> str | None:
    """A binding no root reaches declares a kind: nothing granted what it would resolve."""
    for name, dependency in graph.constructor(node):
        match dependency:
            case ExecutionDependency(kind=kind):
                return f"{name}: declares a {kind.name!r} dependency no root reaches"
    return None


_CONSTRUCTION_REFUSALS: tuple[Rule, ...] = (
    _refuse_cycle,
    _refuse_captive,
    _refuse_unsafe_kind,
    _refuse_declaration,
)

LENIENT_REFUSALS: tuple[Rule, ...] = (*_CONSTRUCTION_REFUSALS, _refuse_unresolvable_root)
"""The refusals a lenient composition keeps: all but the missing binding."""

UNREACHED_REFUSALS: tuple[Rule, ...] = (*_CONSTRUCTION_REFUSALS, _refuse_ungranted_kind)
"""Why seal leaves unbuilt a singleton no root reaches (C-B2): refusals 2-5, or a kind.

A missing binding or an unresolvable parameter is not among them: building
the singleton fails startup, as ``validate()`` does.
"""

BINDING_REFUSALS: tuple[Rule, ...] = (_refuse_missing_binding, _refuse_cycle)
"""Refusals 1 and 2: a key with no binding, a main repository's capability included, or a cycle."""

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
        for reason in graph.unresolvable(node)
    )
    return GraphAnalysis(
        roots=roots, reachable=frozenset(reachable), warnings=warnings, graph=graph
    )
