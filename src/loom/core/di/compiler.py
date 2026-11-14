"""Compile a container binding into the closure that serves it.

A binding compiles once, on its first resolve or when the container is
sealed: its constructor is read from the container's dependency graph, each
dependency becomes the closure of the binding it names, and the scope wraps
the result in one branch. An ``APPLICATION`` binding is built once, outside
any execution, then served as a constant; a ``REQUEST`` or ``TRANSACTION``
one is built once per execution and kept in its cache, or built fresh
outside any execution. A class loom builds itself, such as a use case
or a job, compiles the same way into its *plan*, with no scope.

Concurrency:
    :data:`COLD_PATH`, one reentrant lock for the whole process, guards
    structure only: compiling (the compiled closures, the graph's memo) and
    changing a binding, which clears every compiled closure. No provider runs
    under it.
    Building a singleton takes its binding's own reentrant lock, a once-cell:
    one thread builds while the others wait for that singleton alone, and a
    provider resolving another singleton on its own thread recurses. Locks
    are taken in one order, a binding's then :data:`COLD_PATH`, so a
    provider may wait on another thread that resolves from the container.
    A cycle of providers split across two threads waits forever instead of
    recursing; it is a bug either way. Serving a built singleton takes no
    lock; a per-execution binding is built once per execution
    (:func:`~loom.core.execution.per_execution`).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from itertools import repeat
from typing import TYPE_CHECKING, Any

from loom.core.di.errors import ResolutionError, describe_key
from loom.core.di.graph import ClassNode, DependencyGraphError, Edge, KeyNode, Node, Path
from loom.core.di.introspection import (
    Dependency,
    ExecutionDependency,
    KeyDependency,
    RefusedDependency,
    RepoDependency,
)
from loom.core.di.scope import Scope
from loom.core.execution import ExecutionKinds, NoExecution, per_execution

if TYPE_CHECKING:
    from loom.core.di.container import LoomContainer, _Binding

Closure = Callable[[], object]
"""What the container calls to serve a binding."""

COLD_PATH = threading.RLock()
"""Taken to compile a binding or change one; never while a provider runs."""

UNBUILT: Any = object()
"""The instance of a singleton not built yet."""


def compile_key(container: LoomContainer, key: object) -> Closure:
    """Return the closure serving *key*, compiling it and every binding it needs first.

    Raises:
        ResolutionError: When nothing is bound under *key*.
        DependencyGraphError: When a binding it needs is missing, unmapped,
            refused or part of a construction cycle, with the path.
    """
    with COLD_PATH:
        return _Compilation(container).compile(Path(KeyNode(key)), key)


def compile_class(container: LoomContainer, cls: type[Any]) -> Callable[[], Any]:
    """Return the plan of *cls*, compiled once with *cls* as its own provider; see ``compiled``."""
    node = ClassNode(cls)
    with COLD_PATH:
        plan = container._plans.get(cls)
        if plan is None:
            plan = container._plans[cls] = _Compilation(container)._maker(Path(node), node, cls)
        return plan


class _Compilation:
    """One compile, from one key: the keys whose compile is still open detect a cycle."""

    __slots__ = ("_container", "_open")

    def __init__(self, container: LoomContainer) -> None:
        self._container = container
        self._open: set[object] = set()

    def compile(self, path: Path, key: object) -> Closure:
        """The closure of *key*, which ends *path*."""
        container = self._container
        closure = container._compiled.get(key)
        if closure is not None:
            return closure
        if key in self._open:
            raise DependencyGraphError(f"{path}: dependency cycle")
        binding = container._bindings.get(key)
        if binding is None:
            raise _unbound(path, key)
        if binding.refused is not None:
            raise DependencyGraphError(binding.refused)
        self._open.add(key)
        try:
            make = self._maker(path, KeyNode(key), binding.provider)
        finally:
            self._open.discard(key)
        compiled = container._compiled
        closure = compiled[key] = _scoped(compiled, key, binding, make)
        return closure

    def _maker(self, path: Path, node: Node, provider: Callable[..., object]) -> Closure:
        """*node*'s *provider*, given its constructor's arguments when it has any."""
        constructor = self._container.graph.constructor(node)
        arguments = tuple(
            (name, argument)
            for name, dependency in constructor
            if (argument := self._argument(path, name, dependency)) is not None
        )
        return _calling(provider, arguments) if arguments else provider

    def _argument(self, path: Path, name: str, dependency: Dependency) -> Closure | None:
        """The closure of one parameter; ``None`` leaves it to Python, as ``Impl()`` did."""
        match dependency:
            case KeyDependency(key):
                return self._dependency(path, name, key)
            case RepoDependency(model):
                return self._dependency(path, name, self._mapped(path, name, model))
            case ExecutionDependency():
                return _resolving(self._container.kinds, dependency, f"{path}: {name}")
            case RefusedDependency(reason):
                raise DependencyGraphError(f"{path}: {name}: {reason}")
        return None

    def _dependency(self, path: Path, name: str, key: object) -> Closure:
        """The closure of *key*, reached from the end of *path* through parameter *name*."""
        return self.compile(path.to(Edge(KeyNode(key), name)), key)

    def _mapped(self, path: Path, name: str, model: type[Any]) -> object:
        """The key *model*'s repository is mapped to."""
        key = self._container._repo_bindings.get(model)
        if key is None:
            raise DependencyGraphError(
                f"{path}: {name}: no repository mapping is registered for {model.__qualname__}"
            )
        return key


def _unbound(path: Path, key: object) -> ResolutionError:
    """Today's error for a missing key resolved directly; the path for one a binding needs."""
    if not path.edges:
        return ResolutionError(f"No binding registered for: {describe_key(key)}")
    return DependencyGraphError(f"{path}: no binding is registered for it")


def _calling(
    provider: Callable[..., object], arguments: tuple[tuple[str, Closure], ...]
) -> Closure:
    def build() -> object:
        return provider(**{name: argument() for name, argument in arguments})

    return build


def _resolving(kinds: ExecutionKinds, dependency: ExecutionDependency, where: str) -> Closure:
    """Resolve *dependency* through its kind's resolver on each build, bound by then or not."""
    kind, declaration, annotation = dependency.kind, dependency.declaration, dependency.annotation

    def resolve() -> object:
        try:
            return kinds.resolve(kind, declaration, annotation)
        except KeyError:
            if kinds.bound(kind):
                raise
            raise ResolutionError(f"{where}: no resolver is bound for {kind.name!r}") from None

    return resolve


def _scoped(
    compiled: dict[object, Closure], key: object, binding: _Binding, make: Closure
) -> Closure:
    if binding.scope is Scope.APPLICATION:
        instance = binding.instance
        if instance is not UNBUILT:
            return repeat(instance).__next__
        return _singleton(compiled, key, binding, make)
    return per_execution(binding, make)


def _singleton(
    compiled: dict[object, Closure], key: object, binding: _Binding, make: Closure
) -> Closure:
    """Build once under the binding's lock and no execution; then serve the instance as is.

    Once built, the container serves *key* through a constant; a dependent
    compiled earlier keeps this closure, which reads the built instance.
    """

    def singleton() -> object:
        instance = binding.instance
        if instance is not UNBUILT:
            return instance
        with binding.lock:
            instance = binding.instance
            if instance is UNBUILT:
                with NoExecution():
                    instance = make()
                binding.instance = instance
                with COLD_PATH:
                    if compiled.get(key) is singleton:
                        compiled[key] = repeat(instance).__next__
        return instance

    return singleton
