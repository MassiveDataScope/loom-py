"""Warnings the dependency injection container emits."""

from __future__ import annotations


class ContainerSealedWarning(DeprecationWarning):
    """A binding was registered into a container after the application sealed it.

    The registration still replaces that key's binding, dropping only that
    key's built instance; it becomes an error in the next major version.
    Register through a module passed to the application instead.
    """


class DependencyUnresolvable(UserWarning):
    """A binding a root reaches declares a required parameter the container cannot serve.

    Only a warning: the binding fails when it is built, and it may never be.
    """
