"""Unit of Work port — transport and backend agnostic.

Defines the :class:`UnitOfWork` and :class:`UnitOfWorkFactory` protocols
that decouple the application layer from the underlying session management
(SQLAlchemy, etc.).

:class:`~loom.core.engine.executor.UseCaseExecutor` opens one invocation
scope (:mod:`loom.core.stores.uow.scope`) per top-level execution and ends it
through the ordered-commits coordinator (:mod:`loom.core.stores.uow.coordinator`).
Nested executions in the task that owns the scope join it.
"""

from loom.core.stores.uow.abc import UnitOfWork, UnitOfWorkFactory

__all__ = [
    "UnitOfWork",
    "UnitOfWorkFactory",
]
