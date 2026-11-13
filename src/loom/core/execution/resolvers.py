"""Execution kinds: which declarations resolve per execution, and who resolves them.

A *kind* names one family of execution dependencies (an agent handle, the
caller, ...). Packages above this one declare which parameter defaults or
annotation origins belong to a kind, refuse the ones that must never reach a
constructor, and bind one resolver per kind. :class:`ExecutionKinds` holds that
data and names no concrete kind, so the container and the executor share it
without importing the packages that declare kinds.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol, TypeVar, get_origin, runtime_checkable

_K = TypeVar("_K")
_V = TypeVar("_V")


def _no_targets(annotation: object) -> tuple[type, ...]:
    return ()


@dataclass(frozen=True, eq=False, slots=True)
class Kind:
    """One family of execution dependencies; two kinds are equal only if identical.

    Args:
        name: Shown in error messages.
        singleton_safe: ``False`` when a singleton must not depend on this kind,
            because the value belongs to one execution (the caller does).
        targets: Returns the classes an annotation of this kind calls, as
            plain classes; ``()`` unless the kind calls another use case.

    Example::

        ACME = Kind("acme", singleton_safe=False)
    """

    name: str
    singleton_safe: bool = True
    targets: Callable[[object], tuple[type, ...]] = _no_targets


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
        kinds.bind(ACME, AcmeResolver())
        kind = kinds.kind_for(AcmeMarker(), AcmeHandle)
        handle = kinds.resolve(kind, AcmeMarker(), AcmeHandle)
    """

    __slots__ = ("_by_origin", "_by_type", "_refused", "_resolvers")

    def __init__(self) -> None:
        self._by_type: dict[type, Kind] = {}
        self._by_origin: dict[object, Kind] = {}
        self._refused: dict[type, str] = {}
        self._resolvers: dict[Kind, ExecutionResolver] = {}

    def declare_type(self, declaration_type: type, kind: Kind) -> None:
        """Make a parameter whose default is a ``declaration_type`` instance a ``kind``."""
        _claim(self._by_type, declaration_type, kind)

    def declare_origin(self, origin: object, kind: Kind) -> None:
        """Make a parameter annotated ``origin[...]`` a ``kind``."""
        _claim(self._by_origin, origin, kind)

    def refuse(self, declaration_type: type, reason: str) -> None:
        """Refuse a parameter whose default is a ``declaration_type`` instance."""
        _claim(self._refused, declaration_type, reason)

    def kind_for(self, default: object, annotation: object) -> Kind | None:
        """Return the kind of a parameter, the default's type taking precedence.

        Returns:
            The kind, or ``None`` when the parameter is no execution dependency.
        """
        kind = self._by_type.get(type(default))
        if kind is not None:
            return kind
        return self._by_origin.get(get_origin(annotation))

    def refusal(self, default: object) -> str | None:
        """Return why a parameter with this default is refused, or ``None``."""
        return self._refused.get(type(default))

    def bind(self, kind: Kind, resolver: ExecutionResolver) -> None:
        """Bind the resolver of ``kind``.

        Raises:
            RuntimeError: When ``kind`` already has one.
        """
        if kind in self._resolvers:
            raise RuntimeError(f"A resolver is already bound for the {kind.name!r} kind")
        self._resolvers[kind] = resolver

    def bound(self, kind: Kind) -> bool:
        """Return whether ``kind`` has a resolver."""
        return kind in self._resolvers

    def resolve(self, kind: Kind, declaration: object, annotation: object) -> object:
        """Resolve ``declaration: annotation`` through the resolver of ``kind``.

        Raises:
            KeyError: When ``kind`` has no resolver (:meth:`bound` tells).
        """
        return self._resolvers[kind].resolve(declaration, annotation)


def _claim(table: dict[_K, _V], key: _K, value: _V) -> None:
    current = table.setdefault(key, value)
    if current != value:
        raise ValueError(f"{key!r} is already declared as {current!r}, not {value!r}")
