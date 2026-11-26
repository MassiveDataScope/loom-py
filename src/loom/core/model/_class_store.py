"""Values derived from a class, kept on the class so they are collected with it."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from functools import wraps
from typing import Any, TypeVar, cast

ClassFunction = TypeVar("ClassFunction", bound=Callable[[type], Any])

_ABSENT: Any = object()


def stored_on_class(attribute: str) -> Callable[[ClassFunction], ClassFunction]:
    """Memoise a function of one class under *attribute* in that class's own ``__dict__``.

    A cache keyed by class keeps every class it has seen alive; the value stored on
    the class lives and dies with it, so classes built at run time can be collected.
    A parent's stored value is never read for a subclass. A class that refuses new
    attributes, as a builtin, recomputes the value on each call.
    """

    def memoise(compute: ClassFunction) -> ClassFunction:
        @wraps(compute)
        def stored(cls: type) -> Any:
            value = cls.__dict__.get(attribute, _ABSENT)
            if value is _ABSENT:
                value = compute(cls)
                with suppress(TypeError):
                    setattr(cls, attribute, value)
            return value

        return cast(ClassFunction, stored)

    return memoise
