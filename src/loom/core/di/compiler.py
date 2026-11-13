"""Compile a container binding into the closure that serves it.

A binding compiles once, on its first resolve or when the container is
sealed: its constructor is read from the container's dependency graph, each
dependency becomes the closure of the binding it names, and the scope wraps
the result in one branch. An ``APPLICATION`` binding is built once, outside
any execution, then served as a constant; a ``REQUEST`` or ``TRANSACTION``
one is built once per execution and kept in its cache, or built fresh
outside any execution (D4).

Concurrency:
    :data:`COLD_PATH`, one reentrant lock for the whole process, guards
    structure only: compiling (the compiled closures, the dependents, the
    graph's memo) and changing a binding. No provider runs under it.
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
from dataclasses import dataclass
from itertools import repeat
from typing import TYPE_CHECKING, Any

from loom.core.di.errors import ResolutionError, describe_key
from loom.core.di.graph import DependencyGraphError, Edge, KeyNode, Path
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
"""The instance of a singleton not built yet, and a miss in an execution's cache."""


@dataclass(frozen=True, slots=True)
class RepoMapping:
    """The repository mapping of ``model``: a binding may depend on it as on a key."""

    model: type[Any]


def compile_key(container: LoomContainer, key: object) -> Closure:
    """Return the closure serving *key*, compiling it and every binding it needs first.

    Raises:
        ResolutionError: When nothing is bound under *key*.
        DependencyGraphError: When a binding it needs is missing, unmapped,
            refused or part of a construction cycle, with the path.
    """
    with COLD_PATH:
        return _Compilation(container).compile(Path(KeyNode(key)), key)


def refusing(message: str) -> Closure:
    """A closure that raises :class:`DependencyGraphError` with *message* whenever called."""

    def refuse() -> object:
        raise DependencyGraphError(message)

    return refuse


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
        self._open.add(key)
        try:
            make = self._maker(path, key, binding)
        finally:
            self._open.discard(key)
        compiled = container._compiled
        closure = compiled[key] = _scoped(compiled, key, binding, make)
        return closure

    def _maker(self, path: Path, key: object, binding: _Binding) -> Closure:
        """The binding's provider, given its constructor's arguments when it has any."""
        constructor = self._container.graph.constructor(KeyNode(key))
        arguments = tuple(
            (name, argument)
            for name, dependency in constructor
            if (argument := self._argument(path, key, name, dependency)) is not None
        )
        return _calling(binding.provider, arguments) if arguments else binding.provider

    def _argument(
        self, path: Path, dependent: object, name: str, dependency: Dependency
    ) -> Closure | None:
        """The closure of one parameter; ``None`` leaves it to Python, as ``Impl()`` did."""
        match dependency:
            case KeyDependency(key):
                return self._dependency(path, dependent, name, key)
            case RepoDependency(model):
                mapped = self._mapped(path, dependent, name, model)
                return self._dependency(path, dependent, name, mapped)
            case ExecutionDependency():
                return _resolving(self._container.kinds, dependency, f"{path}: {name}")
            case RefusedDependency(reason):
                raise DependencyGraphError(f"{path}: {name}: {reason}")
        return None

    def _dependency(self, path: Path, dependent: object, name: str, key: object) -> Closure:
        """The closure of *key*, recorded as a dependency of *dependent*, which ends *path*."""
        self._container._depends(dependent, on=key)
        return self.compile(path.to(Edge(KeyNode(key), name)), key)

    def _mapped(self, path: Path, dependent: object, name: str, model: type[Any]) -> object:
        """The key *model*'s repository is mapped to, recorded as a dependency of *dependent*."""
        self._container._depends(dependent, on=RepoMapping(model))
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
        if instance is not UNBUILT:  # compiled again after a dependency changed
            return repeat(instance).__next__
        return _singleton(compiled, key, binding, make)
    # The binding record keys the execution's cache: a key registered again gets
    # a new record, so an execution never serves an instance of a replaced binding.
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
