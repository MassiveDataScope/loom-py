"""Acme execution kinds the graph tests declare: a caller, an input marker and a call."""

from __future__ import annotations

from typing import Any, Generic, TypeVar, get_args

from loom.core.di.container import LoomContainer
from loom.core.di.graph import ClassNode, DependencyGraph, GraphAnalysis, analyse
from loom.core.execution import Kind

T = TypeVar("T")


class AcmeCaller:
    """A marker of :data:`CALLER`, a kind whose value belongs to one execution."""


class AcmeInput:
    """A marker no constructor may declare."""


class AcmeCall(Generic[T]):
    """``AcmeCall[Target]`` declares a call to ``Target``, a :data:`CALL` dependency."""


def _called(annotation: object) -> tuple[type, ...]:
    return tuple(arg for arg in get_args(annotation) if isinstance(arg, type))


CALLER = Kind("acme-caller", singleton_safe=False)
CALL = Kind("acme-call", targets=_called)


def container() -> LoomContainer:
    """A container declaring the Acme kinds and refusing :class:`AcmeInput`."""
    built = LoomContainer()
    built.kinds.declare_type(AcmeCaller, CALLER)
    built.kinds.declare_origin(AcmeCall, CALL)
    built.kinds.refuse(AcmeInput, "AcmeInput() belongs to execute()")
    return built


def analysed(built: LoomContainer, *roots: type[Any]) -> GraphAnalysis:
    """*built*'s graph analysed from *roots*."""
    return analyse(DependencyGraph(built), [ClassNode(root) for root in roots])
