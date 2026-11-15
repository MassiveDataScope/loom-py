"""Errors shared by execution kinds and dependency injection."""


class ResolutionError(Exception):
    """Raised when a dependency cannot be resolved from the container."""


ResolutionError.__module__ = "loom.core.di.container"


class ContainerSealed(ResolutionError):
    """Raised when a sealed container or its execution kinds are changed."""


ContainerSealed.__module__ = "loom.core.di.errors"
