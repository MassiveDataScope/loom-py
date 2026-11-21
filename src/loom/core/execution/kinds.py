"""Execution kinds: which declarations resolve per execution, and who resolves them.

A *kind* names one family of execution dependencies (an agent handle, the
caller, ...). Packages above this one declare which parameter defaults or
annotation origins belong to a kind, refuse the ones that must never reach a
constructor, and register one resolver per kind. :class:`ExecutionKinds` holds that
data and names no concrete kind, so the container and the executor share it
without importing the packages that declare kinds.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar, get_origin, runtime_checkable

from loom.core.errors.container import ContainerSealed

_K = TypeVar("_K")
_V = TypeVar("_V")


def _calls_nothing(annotation: object) -> tuple[type, ...]:
    return ()


@dataclass(frozen=True, eq=False, slots=True)
class Kind:
    """One family of execution dependencies; two kinds are equal only if identical.

    Args:
        name: Shown in error messages.
        singleton_safe: ``False`` when a singleton must not depend on this kind,
            because the value belongs to one execution (the caller does).
        called_classes: Returns the classes an annotation of this kind calls,
            as plain classes; ``()`` unless the kind calls another use case.
        spelling: The marker declaring a parameter of this kind, as written
            in a signature; empty when no marker declares it.

    Example::

        ACME = Kind("acme", singleton_safe=False)
    """

    name: str
    singleton_safe: bool = True
    called_classes: Callable[[object], tuple[type, ...]] = _calls_nothing
    spelling: str = ""


class UnboundKind(LookupError):
    """Raised when a kind with no resolver bound is resolved.

    Args:
        kind: The kind that has no resolver.
    """

    def __init__(self, kind: Kind) -> None:
        super().__init__(f"no resolver is bound for {kind.name!r}")
        self.kind = kind


@dataclass(frozen=True, slots=True)
class ExecutionBinding:
    """One parameter of an execution kind, resolved per execution from its declaration.

    Args:
        name: Parameter name as declared in the signature.
        kind: The kind whose resolver builds the value.
        declaration: The parameter's default, the marker the resolver reads.
        annotation: Resolved annotation of the parameter.
    """

    name: str
    kind: Kind
    declaration: Any
    annotation: Any


@runtime_checkable
class ExecutionResolver(Protocol):
    """Returns the value of a parameter declaring ``declaration: annotation`` of its kind.

    It reads the execution identity, when it needs one, through
    :func:`~loom.core.execution.execution_identity`.
    """

    def resolve(self, declaration: object, annotation: object) -> object: ...


class ExecutionKinds:
    """Which declarations belong to which kind, and the one resolver bound per kind.

    Declarations are idempotent and refuse a conflicting one; a resolver is
    bound once.

    Example::

        kinds = ExecutionKinds()
        kinds.declare_type(AcmeMarker, ACME)
        kinds.register(ACME, AcmeResolver())
        kind = kinds.kind_for(AcmeMarker(), AcmeHandle)
        handle = kinds.resolve(kind, AcmeMarker(), AcmeHandle)
    """

    __slots__ = ("_by_origin", "_by_type", "_frozen", "_refused", "_resolvers")

    def __init__(self) -> None:
        self._frozen: str | None = None
        self._by_type: dict[type, Kind] = {}
        self._by_origin: dict[object, Kind] = {}
        self._refused: dict[type, str] = {}
        self._resolvers: dict[Kind, ExecutionResolver] = {}

    def freeze(self, reason: str) -> None:
        """Refuse every later declaration, refusal or binding with *reason*."""
        self._frozen = reason

    def _check_frozen(self) -> None:
        """Raise when this registry is frozen."""
        if self._frozen is not None:
            raise ContainerSealed(self._frozen)

    def declare_type(self, declaration_type: type, kind: Kind) -> None:
        """Make a parameter whose default is a ``declaration_type`` instance a ``kind``."""
        self._claim(self._by_type, declaration_type, kind)

    def declare_origin(self, origin: object, kind: Kind) -> None:
        """Make a parameter annotated ``origin[...]`` a ``kind``."""
        self._claim(self._by_origin, origin, kind)

    def refuse(self, declaration_type: type, reason: str) -> None:
        """Refuse a parameter whose default is a ``declaration_type`` instance."""
        self._claim(self._refused, declaration_type, reason)

    def kind_for(self, default: object, annotation: object) -> Kind | None:
        """Return the kind of a parameter, the default's type taking precedence.

        Returns:
            The kind, or ``None`` when the parameter is no execution dependency.
        """
        kind = self._by_type.get(type(default))
        if kind is not None:
            return kind
        return self._by_origin.get(get_origin(annotation))

    def refusal_for(self, default: object) -> str | None:
        """Return why a parameter with this default is refused, or ``None``."""
        return self._refused.get(type(default))

    def register(self, kind: Kind, resolver: ExecutionResolver) -> None:
        """Register the resolver of ``kind``.

        Raises:
            TypeError: When ``resolver`` has no ``resolve(declaration, annotation)``,
                such as a positional ``(name, identity)`` callable.
            RuntimeError: When ``kind`` already has one.
        """
        self._check_frozen()
        if not isinstance(resolver, ExecutionResolver):
            raise TypeError(
                f"The {kind.name!r} resolver must be an ExecutionResolver with "
                "resolve(declaration, annotation); positional callables such as "
                "(name, identity) are no longer accepted"
            )
        if kind in self._resolvers:
            raise RuntimeError(f"A resolver is already bound for the {kind.name!r} kind")
        self._resolvers[kind] = resolver

    def is_bound(self, kind: Kind) -> bool:
        """Return whether ``kind`` has a resolver."""
        return kind in self._resolvers

    def resolve(self, kind: Kind, declaration: object, annotation: object) -> object:
        """Resolve ``declaration: annotation`` through the resolver of ``kind``.

        Raises:
            UnboundKind: When ``kind`` has no resolver.
        """
        resolver = self._resolvers.get(kind)
        if resolver is None:
            raise UnboundKind(kind)
        return resolver.resolve(declaration, annotation)

    def _claim(self, table: dict[_K, _V], key: _K, value: _V) -> None:
        """Declare *key* as *value* in *table*, refusing a conflicting declaration."""
        self._check_frozen()
        current = table.setdefault(key, value)
        if current != value:
            raise ValueError(f"{key!r} is already declared as {current!r}, not {value!r}")
