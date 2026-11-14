"""What a constructor or a factory asks for, read from its signature once per binding.

Each parameter becomes one dependency: an execution dependency when its
default or its annotation belongs to an execution kind; a refused declaration
when its default is one ``kinds`` refuses; nothing when it has any other
default, which Python then supplies; otherwise whatever the key classifier
makes of its annotation, or an unresolvable parameter.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import FunctionType
from typing import TYPE_CHECKING, Any

from loom.core.di.errors import ResolutionError
from loom.core.execution import ExecutionKinds, Kind
from loom.core.model.introspection import resolve_type_hints

if TYPE_CHECKING:
    from loom.core.di.compiler import Supplier


@dataclass(frozen=True, slots=True)
class KeyDependency:
    """A parameter the container serves from the binding under ``key``."""

    key: object


@dataclass(frozen=True, slots=True)
class RepoDependency:
    """A parameter the container serves from the repository mapped to ``model``.

    ``contract``, when set, is a key that repository must also be bound
    under, such as the standard capability a use case declares for it.
    """

    model: type[Any]
    contract: object = None


@dataclass(frozen=True, slots=True)
class ExecutionDependency:
    """A parameter of an execution kind, resolved per execution from ``declaration``."""

    kind: Kind
    declaration: object
    annotation: object

    def supplier(self, kinds: ExecutionKinds, where: str) -> Supplier:
        """Return what resolves this dependency through its kind's resolver on each build.

        Args:
            kinds: The execution kinds whose resolver serves it, bound by then or not.
            where: The path and parameter an unbound kind's error names.

        Returns:
            The supplier of the dependency's value.

        Raises:
            ResolutionError: From the supplier, when no resolver is bound for the kind.
        """
        kind, declaration, annotation = self.kind, self.declaration, self.annotation

        def resolve() -> object:
            try:
                return kinds.resolve(kind, declaration, annotation)
            except KeyError:
                if kinds.is_bound(kind):
                    raise
                raise ResolutionError(f"{where}: no resolver is bound for {kind.name!r}") from None

        return resolve


@dataclass(frozen=True, slots=True)
class RefusedDependency:
    """A parameter whose default no constructor may declare, and why."""

    reason: str


@dataclass(frozen=True, slots=True)
class UnresolvableDependency:
    """A required parameter whose annotation names nothing the container can serve."""

    annotation: object


Dependency = (
    KeyDependency
    | RepoDependency
    | ExecutionDependency
    | RefusedDependency
    | UnresolvableDependency
)

Constructor = tuple[tuple[str, Dependency], ...]
"""The ``(parameter name, dependency)`` pairs of a constructor, in signature order."""

KeyClassifier = Callable[[object], KeyDependency | RepoDependency | None]
"""How a constructor annotation becomes a container key, ``None`` for none."""

_VARIADIC = (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)


def plain_key(annotation: object) -> KeyDependency | None:
    """The default classifier: a class annotation is its own key."""
    return KeyDependency(annotation) if isinstance(annotation, type) else None


def no_implicit_dependencies(cls: type[Any]) -> Constructor:
    return ()


@dataclass(frozen=True, slots=True)
class SignatureReader:
    """Reads the dependencies of a constructor or a factory function from its signature.

    Args:
        kinds: The execution kinds a parameter's default or annotation may belong to.
        classify_key: How an annotation becomes a container key.
        implicit: The dependencies loom injects into a class it builds beyond
            those its signature declares, such as a use case's main repository.
    """

    kinds: ExecutionKinds
    classify_key: KeyClassifier = plain_key
    implicit: Callable[[type[Any]], Constructor] = no_implicit_dependencies

    def built_by_loom(self, cls: type[Any]) -> Constructor:
        """Return the dependencies of a class loom builds: its signature's, then *implicit*'s."""
        return (*self.dependencies(cls), *self.implicit(cls))

    def dependencies(self, built: object) -> Constructor:
        """Return the dependencies of a class's ``__init__`` or of a factory function.

        ``self`` and ``*args``/``**kwargs`` are skipped. Annotations are resolved
        through their module, falling back to the raw ones when they cannot be;
        a signature that cannot be read has no dependencies. Any other
        provider, and a class whose metaclass ``__call__`` or ``__new__`` may
        return anything, is opaque: it has none either.

        Args:
            built: What the container or loom calls to build the value.

        Returns:
            The ``(parameter name, dependency)`` pairs, in signature order.
        """
        if isinstance(built, FunctionType):
            init, first = built, 0
        elif (
            inspect.isclass(built)
            and type(built).__call__ is type.__call__
            and built.__new__ is object.__new__
        ):
            init, first = built.__init__, 1
        else:
            return ()
        parameters = _parameters(init, first)
        hints = _annotations(init, parameters)
        dependencies = (
            (p.name, self._dependency(p.default, hints.get(p.name))) for p in parameters
        )
        return tuple(
            (name, dependency) for name, dependency in dependencies if dependency is not None
        )

    def _dependency(self, default: object, annotation: object) -> Dependency | None:
        kind = self.kinds.kind_for(default, annotation)
        if kind is not None:
            return ExecutionDependency(kind, default, annotation)
        reason = self.kinds.refusal(default)
        if reason is not None:
            return RefusedDependency(reason)
        if default is not inspect.Parameter.empty:
            return None
        key = self.classify_key(annotation)
        return UnresolvableDependency(annotation) if key is None else key


def _annotations(
    init: Callable[..., object], parameters: tuple[inspect.Parameter, ...]
) -> Mapping[str, object]:
    """The annotations of *init* resolved through its module, else the raw ones."""
    raw = {p.name: p.annotation for p in parameters if p.annotation is not p.empty}
    return resolve_type_hints(init) or raw


def _parameters(init: Callable[..., object], first: int) -> tuple[inspect.Parameter, ...]:
    """The parameters of *init* from *first* on (1 skips ``self``), variadic ones dropped.

    None when the signature cannot be read.
    """
    try:
        parameters = tuple(inspect.signature(init).parameters.values())[first:]
    except (TypeError, ValueError):
        return ()
    return tuple(p for p in parameters if p.kind not in _VARIADIC)
