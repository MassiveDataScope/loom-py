"""Errors and warnings of the dependency injection container, and how they name a key."""

from __future__ import annotations

from typing import get_args, get_origin


class ResolutionError(Exception):
    """Raised when a dependency cannot be resolved from the container.

    Args:
        message: Human-readable description of the resolution failure.

    Example::

        raise ResolutionError("No binding registered for: UserRepository")
    """


ResolutionError.__module__ = "loom.core.di.container"


class ContainerSealed(ResolutionError):
    """A binding was registered into a container after the application sealed it.

    A sealed container is complete: its singletons are built and its graph
    analysed, so nothing may change it. Register every binding in a module
    passed to the application; to reconfigure, build a new container.
    """


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
