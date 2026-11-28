"""Dependency injection scopes."""

from enum import Enum


class Scope(Enum):
    """Lifetime scope for a registered dependency.

    Attributes:
        APPLICATION: Singleton — created once, outside any execution, and
            reused for the entire process lifetime.  Suitable for stateless
            services, connection pools, and configuration objects.
        REQUEST: Per-execution — built once per execution, shared by
            everything that execution resolves (and by the tasks it creates
            while it runs; one still running after it ended gets
            :class:`~loom.core.execution.ExecutionClosed`); a nested execution
            gets its own. Outside any execution, each
            :meth:`~loom.core.di.container.LoomContainer.resolve` creates a
            fresh one.  Suitable for repositories and unit-of-work objects.
        TRANSACTION: Reserved for transactional scope.  Behaves like
            ``REQUEST``.
    """

    APPLICATION = "application"
    REQUEST = "request"
    TRANSACTION = "transaction"

    @property
    def is_per_execution(self) -> bool:
        """Whether a binding of this scope is built once per execution, not once per process."""
        return self is not Scope.APPLICATION
