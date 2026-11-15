"""Errors and warnings of the dependency injection container, and how they name a key."""

from __future__ import annotations

from typing import get_args, get_origin

from loom.core.errors.container import ContainerSealed as ContainerSealed
from loom.core.errors.container import ResolutionError as ResolutionError


class DependencyWarning(UserWarning):
    """A binding the container serves may fail when it is built; the message says why."""


class DependencyUnresolvable(DependencyWarning):
    """A binding a root reaches declares a required parameter the container cannot serve.

    Only a warning: the binding fails when it is built, and it may never be.
    """


def describe_key(key: object) -> str:
    """Return a stable human-readable name for a DI binding key, ``Origin[Arg]`` for an alias."""
    if isinstance(key, type):
        return key.__qualname__
    origin = get_origin(key)
    if isinstance(origin, type):
        args = ", ".join(describe_key(arg) for arg in get_args(key))
        return f"{origin.__qualname__}[{args}]"
    return repr(key)
