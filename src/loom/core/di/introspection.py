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
from typing import Any

from loom.core.execution import ExecutionKinds, Kind
from loom.core.model.introspection import resolve_type_hints


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


def constructor_dependencies(
    built: type[Any] | FunctionType, kinds: ExecutionKinds, classify_key: KeyClassifier
) -> Constructor:
    """Return the dependencies of a class's ``__init__`` or of a factory function.

    ``self`` and ``*args``/``**kwargs`` are skipped. Annotations are resolved
    through their module, falling back to the raw ones when they cannot be;
    a signature that cannot be read has no dependencies, and is called with
    no arguments.
    """
    init, first = (built.__init__, 1) if isinstance(built, type) else (built, 0)
    parameters = _parameters(init, first)
    hints = _annotations(init, parameters)
    dependencies = (
        (p.name, _dependency(p.default, hints.get(p.name), kinds, classify_key)) for p in parameters
    )
    return tuple((name, dependency) for name, dependency in dependencies if dependency is not None)


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


def _dependency(
    default: object, annotation: object, kinds: ExecutionKinds, classify_key: KeyClassifier
) -> Dependency | None:
    kind = kinds.kind_for(default, annotation)
    if kind is not None:
        return ExecutionDependency(kind, default, annotation)
    reason = kinds.refusal(default)
    if reason is not None:
        return RefusedDependency(reason)
    if default is not inspect.Parameter.empty:
        return None
    key = classify_key(annotation)
    return UnresolvableDependency(annotation) if key is None else key
