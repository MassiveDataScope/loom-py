"""Dependency injection scopes."""

from enum import Enum


class Scope(Enum):
    """Lifetime scope for a registered dependency.

    Attributes:
        APPLICATION: Singleton — created once, outside any execution, and
            reused for the entire process lifetime.  Suitable for stateless
            services, connection pools, and configuration objects.
        REQUEST: Per-execution — built once per execution, shared by
            everything that execution resolves (and by the tasks it creates,
            even one that outlives it); a nested execution gets its own.
            Outside any execution, each
            :meth:`~loom.core.di.container.LoomContainer.resolve` creates a
            fresh one.  Suitable for repositories and unit-of-work objects.
        TRANSACTION: Reserved for transactional scope.  Behaves like
            ``REQUEST``.
    """

    APPLICATION = "application"
    REQUEST = "request"
    TRANSACTION = "transaction"
