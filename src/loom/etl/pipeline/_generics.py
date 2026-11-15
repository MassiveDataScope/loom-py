"""Generic parameter extraction utility for ETL base classes.

Internal module — consumed by :mod:`loom.etl.pipeline._step` and
:mod:`loom.etl.pipeline._pipeline`.
"""

from __future__ import annotations

import typing
from typing import Any, cast


def _extract_generic_arg(cls: type[Any], origin: type[Any]) -> type[Any] | None:
    """Return the first generic argument of *cls* where ``__origin__`` is *origin*.

    Iterates over ``__orig_bases__`` to find ``origin[T]`` and returns ``T``.
    Returns ``None`` when no matching parameterised base is found.

    Args:
        cls:    Class whose ``__orig_bases__`` to inspect.
        origin: The generic base class to match against (e.g. ``ETLStep``).

    Returns:
        The first type argument, or ``None`` if not found.
    """
    for base in getattr(cls, "__orig_bases__", ()):
        if getattr(base, "__origin__", None) is origin:
            args = typing.get_args(base)
            if args:
                return cast(type[Any], args[0])
    return None
