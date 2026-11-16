"""Compile a container binding into the supplier that serves it.

A binding compiles once, on its first resolve or when the container is
sealed: its constructor is read from the container's dependency graph, each
dependency becomes the supplier of the binding it names, and the scope wraps
the result in one branch. An ``APPLICATION`` binding is built once, outside
any execution, then served as a constant; a ``REQUEST`` or ``TRANSACTION``
one is built once per execution and kept in its cache, or built fresh
outside any execution. A class loom builds itself, such as a use case
or a job, compiles the same way into its *plan*, with no scope.

Concurrency:
    :data:`STRUCTURE_LOCK`, one reentrant lock for the whole process, guards
    structure only: compiling (the compiled suppliers, the graph's memo) and
    changing a binding, which clears every compiled supplier. No provider
    runs under it.
    Building a singleton takes its binding's own reentrant lock, a once-cell:
    one thread builds while the others wait for that singleton alone, and a
    provider resolving another singleton on its own thread recurses. Locks
    are taken in one order, a binding's then :data:`STRUCTURE_LOCK`, so a
    provider may wait on another thread that resolves from the container.
    A cycle of providers split across two threads waits forever. Serving a built singleton takes no
    lock; a per-execution binding is built once per execution
    (:func:`~loom.core.execution.per_execution`).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from itertools import repeat
from typing import TYPE_CHECKING, Any, TypeVar

from loom.core.di.errors import ResolutionError
from loom.core.di.graph import (
    ClassNode,
    DependencyGraph,
    DependencyGraphError,
    DependencyPath,
    Edge,
    KeyNode,
    Node,
)
from loom.core.di.introspection import (
    Dependency,
    KeyDependency,
    RefusedDependency,
    RepoDependency,
)
from loom.core.execution import ExecutionBinding, ExecutionKinds, UnboundKind, per_execution

if TYPE_CHECKING:
    from loom.core.di.container import _Binding

T = TypeVar("T")

Supplier = Callable[[], object]
"""What the container calls to serve a binding."""

STRUCTURE_LOCK = threading.RLock()
"""Taken to compile a binding or change one; never while a provider runs."""

UNBUILT: Any = object()
"""The instance of a singleton not built yet."""


class SupplierCompiler:
    """Compiles each binding of a dependency graph into the supplier serving it.

    Writes only under :data:`STRUCTURE_LOCK`. The keys whose compile is still
    open detect a construction cycle.

    Args:
        graph: The dependency graph whose bindings and constructors are compiled.
        kinds: The execution kinds an execution dependency resolves through.
        served: Where the supplier compiled for each key is kept.
        plans: Where the plan compiled for each class loom builds is kept.
    """

    __slots__ = ("_graph", "_kinds", "_open", "_plans", "_served")

    def __init__(
        self,
        graph: DependencyGraph,
        kinds: ExecutionKinds,
        served: dict[object, Supplier],
        plans: dict[type[Any], Callable[[], Any]],
    ) -> None:
        self._graph = graph
        self._kinds = kinds
        self._served = served
        self._plans = plans
        self._open: set[object] = set()

    def serving(self, key: object) -> Supplier:
        """Return the supplier serving *key*, compiling it and every binding it needs first.

        Raises:
            ResolutionError: When nothing is bound under *key*.
            DependencyGraphError: When a binding it needs is missing, unmapped,
                refused or part of a construction cycle, with the path.
        """
        with STRUCTURE_LOCK:
            return self._compile(DependencyPath(KeyNode(key)), key)

    def plan(self, cls: type[T]) -> Callable[[], T]:
        """Return the plan of *cls*, a class loom builds, compiled once as its own provider.

        Raises:
            DependencyGraphError: When a dependency is missing, unmapped,
                refused or part of a construction cycle, with the path.
        """
        with STRUCTURE_LOCK:
            plan: Callable[[], Any] | None = self._plans.get(cls)
            if plan is None:
                node = ClassNode(cls)
                plan = self._plans[cls] = self._constructor_call(DependencyPath(node), node, cls)
            return plan

    def _compile(self, path: DependencyPath, key: object) -> Supplier:
        """The supplier of *key*, which ends *path*."""
        supplier = self._served.get(key)
        if supplier is not None:
            return supplier
        if key in self._open:
            raise DependencyGraphError(f"{path}: dependency cycle")
        binding = self._graph.binding(key)
        if binding is None:
            raise path.unbound(key)
        if binding.refused is not None:
            raise DependencyGraphError(binding.refused)
        self._open.add(key)
        try:
            if binding.target is not None:
                supplier = self._compile(
                    path.to(Edge(KeyNode(binding.target), "target")), binding.target
                )
            else:
                make = self._constructor_call(path, KeyNode(key), binding.provider)
                supplier = self._scoped(key, binding, make)
        finally:
            self._open.discard(key)
        self._served[key] = supplier
        return supplier

    def _scoped(self, key: object, binding: _Binding, make: Supplier) -> Supplier:
        """Serve *binding* by its scope: per execution, as a constant, or built once.

        An unbuilt singleton's supplier is replaced by a constant once built, under
        its key and under every alias of it; a dependent compiled earlier keeps it,
        and it reads the built instance.
        """
        if binding.scope.per_execution:
            return per_execution(binding, make)
        instance = binding.instance
        if instance is not UNBUILT:
            return repeat(instance).__next__
        served = self._served

        def singleton() -> object:
            instance = binding.instance
            if instance is not UNBUILT:
                return instance
            instance = binding.build(make)
            constant = repeat(instance).__next__
            with STRUCTURE_LOCK:
                for served_key in [k for k, v in served.items() if v is singleton]:
                    served[served_key] = constant
            return instance

        return singleton

    def _constructor_call(
        self, path: DependencyPath, node: Node, provider: Callable[..., Any]
    ) -> Supplier:
        """*node*'s *provider*, given its constructor's arguments when it has any."""
        arguments = tuple(
            (name, argument)
            for name, dependency in self._graph.constructor(node)
            if (argument := self._argument(path, name, dependency)) is not None
        )
        return _call_with_arguments(provider, arguments) if arguments else provider

    def _argument(self, path: DependencyPath, name: str, dependency: Dependency) -> Supplier | None:
        """The supplier of one parameter; ``None`` when Python supplies its default."""
        match dependency:
            case KeyDependency(key):
                return self._compile(path.to(Edge(KeyNode(key), name)), key)
            case RepoDependency(model):
                key = self._graph.repository(model)
                if key is None:
                    raise DependencyGraphError(
                        f"{path}: {name}: no repository mapping is registered for "
                        f"{model.__qualname__}"
                    )
                return self._compile(path.to(Edge(KeyNode(key), name)), key)
            case ExecutionBinding():
                return self._resolve_per_build(dependency, f"{path}: {name}")
            case RefusedDependency(reason):
                raise DependencyGraphError(f"{path}: {name}: {reason}")
        return None

    def _resolve_per_build(self, binding: ExecutionBinding, where: str) -> Supplier:
        """The supplier resolving *binding* through its kind's resolver on each build.

        Args:
            binding: The execution parameter to resolve.
            where: The path and parameter an unbound kind's error names.

        Returns:
            The supplier of the parameter's value; it raises
            :class:`~loom.core.di.errors.ResolutionError` when no resolver is
            bound for the kind.
        """
        kinds = self._kinds
        kind, declaration, annotation = binding.kind, binding.declaration, binding.annotation

        def resolve() -> object:
            try:
                return kinds.resolve(kind, declaration, annotation)
            except UnboundKind as exc:
                raise ResolutionError(f"{where}: {exc}") from None

        return resolve


def _call_with_arguments(
    provider: Callable[..., object], arguments: tuple[tuple[str, Supplier], ...]
) -> Supplier:
    def build() -> object:
        return provider(**{name: argument() for name, argument in arguments})

    return build
