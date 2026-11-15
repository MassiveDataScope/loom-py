"""The dependency graph of a container, and its structural analysis.

A node is a container binding (:class:`KeyNode`) or a class loom builds
itself, such as a use case, a job or a callback (:class:`ClassNode`). An
edge is one parameter of the constructor or factory function loom calls for
a node; a *call edge* reaches a class another
execution runs, so a singleton above it holds nothing of that execution.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from loom.core.di.errors import DependencyUnresolvable, ResolutionError, describe_key
from loom.core.di.introspection import (
    KeyDependency,
    RefusedDependency,
    RepoDependency,
    SignatureReader,
    UnresolvableDependency,
)
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionBinding, ExecutionKinds, Kind, UnboundKind

if TYPE_CHECKING:
    from loom.core.di.container import _Binding
    from loom.core.di.introspection import Constructor


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
class DependencyPath:
    """The edges walked from ``root`` to the node under analysis, which ends it."""

    root: Node
    edges: tuple[Edge, ...] = ()

    @property
    def end(self) -> Node:
        """The node this path ends at: the last edge's target, else the root."""
        return self.edges[-1].target if self.edges else self.root

    def to(self, edge: Edge) -> DependencyPath:
        """This path followed by *edge*."""
        return DependencyPath(self.root, (*self.edges, edge))

    def unbound(self, key: object) -> ResolutionError:
        """Return the error for *key*, which ends this path and has no binding.

        Args:
            key: The key nothing is bound under.

        Returns:
            A :class:`ResolutionError` for a key resolved directly; a
            :class:`DependencyGraphError` naming this path for one a binding needs.
        """
        if not self.edges:
            return ResolutionError(f"No binding registered for: {describe_key(key)}")
        return DependencyGraphError(f"{self}: no binding is registered for it")

    def revisits(self) -> bool:
        """Whether the end of this path is held earlier, since its last call edge.

        A call edge runs its target in another execution, so a loop through one builds.
        """
        nodes = (self.root, *(edge.target for edge in self.edges))
        start = max((i + 1 for i, edge in enumerate(self.edges) if edge.call_edge), default=0)
        return self.end in nodes[start:-1]

    def __str__(self) -> str:
        steps = (f" -> {edge.name}: {edge.target}" for edge in self.edges)
        return f"{self.root}{''.join(steps)}"


class DependencyGraphError(ResolutionError):
    """The dependency graph is refused; the message holds the path from a root."""


@dataclass(frozen=True, slots=True)
class GraphAnalysis:
    """Snapshot taken at the end of registration: the roots, what they reach, the warnings."""

    roots: tuple[Node, ...]
    reachable: frozenset[Node]
    warnings: tuple[DependencyUnresolvable, ...]
    graph: DependencyGraph


Refusal = Callable[[Node, DependencyPath, "DependencyGraph"], str | None]
"""One structural refusal: why the node ending the path is refused, or ``None``.

A refusal reads the path only through whether a singleton holds its end and
which nodes are on it.
"""


def _refuse_missing_binding(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
    """A key reached from a root has no binding; a class never misses one."""
    match node:
        case KeyNode() if graph.scope(node) is None:
            return "no binding is registered for it"
    return None


def _refuse_cycle(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
    """The construction since the last call edge reaches a node it already holds."""
    return "dependency cycle" if path.revisits() else None


def _refuse_captive(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
    """A singleton holds a binding built per execution."""
    scope = graph.scope(node)
    if scope is not None and scope.per_execution and graph.held_by_singleton(path):
        return "an APPLICATION binding cannot hold a per-execution one"
    return None


def _refuse_unsafe_kind(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
    """A singleton holds a kind whose value belongs to one execution."""
    for name, dependency in graph.constructor(node):
        match dependency:
            case ExecutionBinding(kind=kind) if not kind.singleton_safe:
                if graph.held_by_singleton(path):
                    return f"{name}: an APPLICATION binding cannot hold a {kind.name!r} value"
    return None


def _refuse_declaration(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
    """A constructor declares what only ``execute()`` may."""
    for name, dependency in graph.constructor(node):
        match dependency:
            case RefusedDependency(reason):
                return f"{name}: {reason}"
    return None


def _refuse_unresolvable(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
    """A required parameter of a class loom builds names nothing the container serves."""
    return next(graph.unresolvable(node), None)


def _refuse_unresolvable_root(
    node: Node, path: DependencyPath, graph: DependencyGraph
) -> str | None:
    """A required parameter names nothing the container serves, at a root only."""
    return None if path.edges else _refuse_unresolvable(node, path, graph)


def refuse_unmapped_repository(nodes: Collection[Node]) -> Refusal:
    """Refuse a constructor of one of *nodes* that asks for an unmapped model's repository.

    Only *nodes* are checked, so a caller that reports an unmapped repository
    of its own classes later keeps its error.

    Args:
        nodes: The nodes checked.

    Returns:
        The refusal.
    """

    def refuse(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
        if node not in nodes:
            return None
        unmapped = (
            f"{name}: no repository mapping is registered for {dependency.model.__qualname__}"
            for name, dependency in graph.constructor(node)
            if isinstance(dependency, RepoDependency) and graph.repository(dependency.model) is None
        )
        return next(unmapped, None)

    return refuse


def refuse_unbound(kinds: ExecutionKinds, skip: Collection[Kind] = ()) -> Refusal:
    """Refuse a singleton that declares a kind of *kinds* no resolver is bound for.

    Only ``APPLICATION`` bindings: seal builds them, while a per-execution
    adapter fails at its first build, as an ``execute()`` marker does.

    Args:
        kinds: The execution kinds whose resolvers are checked.
        skip: Kinds left to the first build too.

    Returns:
        The refusal.
    """

    def refuse(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
        if graph.scope(node) is not Scope.APPLICATION:
            return None
        unbound = (
            f"{name}: {UnboundKind(dependency.kind)}"
            for name, dependency in graph.constructor(node)
            if isinstance(dependency, ExecutionBinding)
            and dependency.kind not in skip
            and not kinds.is_bound(dependency.kind)
        )
        return next(unbound, None)

    return refuse


def _refuse_ungranted_kind(node: Node, path: DependencyPath, graph: DependencyGraph) -> str | None:
    """A binding no root reaches declares a kind: nothing granted what it would resolve."""
    for name, dependency in graph.constructor(node):
        match dependency:
            case ExecutionBinding(kind=kind):
                return f"{name}: declares a {kind.name!r} dependency no root reaches"
    return None


_CONSTRUCTION_REFUSALS: tuple[Refusal, ...] = (
    _refuse_cycle,
    _refuse_captive,
    _refuse_unsafe_kind,
    _refuse_declaration,
)

LENIENT_REFUSALS: tuple[Refusal, ...] = (*_CONSTRUCTION_REFUSALS, _refuse_unresolvable_root)
"""What a lenient composition refuses: no missing binding, unresolvable parameters at roots."""

UNREACHED_REFUSALS: tuple[Refusal, ...] = (*_CONSTRUCTION_REFUSALS, _refuse_ungranted_kind)
"""Why seal leaves unbuilt a singleton no root reaches: a construction refusal, or a kind."""

BINDING_REFUSALS: tuple[Refusal, ...] = (_refuse_missing_binding, _refuse_cycle)
"""A key with no binding, a main repository's capability included, or a cycle."""

REFUSALS: tuple[Refusal, ...] = (
    _refuse_missing_binding,
    *_CONSTRUCTION_REFUSALS,
    _refuse_unresolvable,
)
"""The structural refusals, in the order they win; unresolvable parameters on every class."""


class DependencyGraph:
    """The nodes a container's bindings and loom's own classes form, read on demand.

    What it reads is memoised per node; the container invalidates a key's
    entries when the key is registered again (:meth:`invalidate`).

    Args:
        bindings: The container's bindings by key.
        repo_bindings: The container's repository keys by model.
        reader: How a constructor's signature becomes its dependencies.
    """

    __slots__ = ("_bindings", "_constructors", "_edges", "_repo_bindings", "reader")

    def __init__(
        self,
        bindings: Mapping[object, _Binding],
        repo_bindings: Mapping[type[Any], object],
        reader: SignatureReader,
    ) -> None:
        self._bindings = bindings
        self._repo_bindings = repo_bindings
        self.reader = reader
        self._constructors: dict[Node, Constructor] = {}
        self._edges: dict[Node, tuple[Edge, ...]] = {}

    def invalidate(self, key: object) -> None:
        """Drop what was read for the binding under *key*: it was registered again."""
        node = KeyNode(key)
        self._constructors.pop(node, None)
        self._edges.pop(node, None)

    def invalidate_edges(self) -> None:
        """Drop every memoised edge: a repository mapping changed, and edges follow mappings."""
        self._edges.clear()

    def edges(self, node: Node) -> tuple[Edge, ...]:
        """The one source of edges, derived once per node; an unmapped repository gives none.

        :func:`refuse_unmapped_repository` refuses the nodes whose unmapped repository
        nothing else reports.
        """
        edges = self._edges.get(node)
        if edges is None:
            edges = self._edges[node] = tuple(self._edges_of(node))
        return edges

    def constructor(self, node: Node) -> Constructor:
        """The dependencies of the constructor or factory function loom calls for *node*.

        None for any other provider, such as a ``lambda`` or a bound method.
        """
        constructor = self._constructors.get(node)
        if constructor is None:
            match node:
                case ClassNode(cls):
                    constructor = self.reader.built_by_loom(cls)
                case KeyNode(key):
                    binding = self.binding(key)
                    provider = None if binding is None else binding.provider
                    constructor = self.reader.dependencies(provider)
            self._constructors[node] = constructor
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
        match node:
            case KeyNode(key):
                binding = self.binding(key)
                return None if binding is None else binding.scope
        return None

    def binding(self, key: object) -> _Binding | None:
        """The binding under *key*; ``None`` when nothing is bound."""
        return self._bindings.get(key)

    def repository(self, model: type[Any]) -> object | None:
        """The key *model*'s repository is mapped to; ``None`` when no mapping is."""
        return self._repo_bindings.get(model)

    def held_by_singleton(self, path: DependencyPath) -> bool:
        """Whether an ``APPLICATION`` binding holds the end of *path*; a call edge stops the search.

        The root counts too: a singleton rooting the walk holds what it reaches.
        """
        for edge in reversed(path.edges):
            if self.scope(edge.target) is Scope.APPLICATION:
                return True
            if edge.call_edge:
                return False
        return self.scope(path.root) is Scope.APPLICATION

    def analyse(
        self, roots: Iterable[Node], refusals: tuple[Refusal, ...] = REFUSALS
    ) -> GraphAnalysis:
        """Walk the graph from each of *roots* and run *refusals* on every node reached.

        A node is checked once per way of being reached: as a root or not, held
        by a singleton or not. What follows a call edge depends on nothing before
        it, so a loop back to a call target still being walked stops there.
        Bindings no root reaches are not analysed. Under refusals that refuse an
        unresolvable parameter at roots only (:data:`LENIENT_REFUSALS`), one of a
        reached binding is a warning instead.

        Args:
            roots: The nodes the walk starts from.
            refusals: The refusals run on each node reached.

        Returns:
            The roots, what they reach and the warnings.

        Raises:
            DependencyGraphError: The first refusal, with its path.
        """
        roots = tuple(dict.fromkeys(roots))
        reachable: dict[Node, None] = {}
        checked: set[tuple[Node, bool, bool]] = set()
        entered: set[tuple[Node, bool, bool]] = set()

        def visit(node: Node, path: DependencyPath) -> None:
            state = (node, self.held_by_singleton(path), not path.edges)
            if state in checked:
                return
            for refusal in refusals:
                reason = refusal(node, path, self)
                if reason is not None:
                    raise DependencyGraphError(f"{path}: {reason}")
            if state in entered and path.edges[-1].call_edge:
                return
            entered.add(state)
            reachable[node] = None
            for edge in self.edges(node):
                visit(edge.target, path.to(edge))
            checked.add(state)

        for root in roots:
            visit(root, DependencyPath(root))
        warnings = tuple(
            DependencyUnresolvable(f"{node} -> {reason}")
            for node in reachable
            if node not in roots
            for reason in self.unresolvable(node)
        )
        return GraphAnalysis(
            roots=roots, reachable=frozenset(reachable), warnings=warnings, graph=self
        )

    def _edges_of(self, node: Node) -> Iterator[Edge]:
        for name, dependency in self.constructor(node):
            match dependency:
                case KeyDependency(key):
                    yield Edge(KeyNode(key), name)
                case RepoDependency(model, contract):
                    repository = self.repository(model)
                    if repository is not None:
                        yield Edge(KeyNode(repository), name)
                    if contract is not None:
                        yield Edge(KeyNode(contract), name)
                case ExecutionBinding(kind=kind, annotation=annotation):
                    calls = kind.called_classes(annotation)
                    yield from (Edge(ClassNode(cls), name, True) for cls in calls)
