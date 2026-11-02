"""Unit of Work port — transport and backend agnostic.

Defines the :class:`UnitOfWork` and :class:`UnitOfWorkFactory` protocols
that decouple the application layer from the underlying session management
(SQLAlchemy, etc.).

:class:`~loom.core.engine.executor.RuntimeExecutor` opens one invocation
scope (:mod:`loom.core.uow.scope`) per top-level execution and ends it
through the ordered-commits coordinator (:mod:`loom.core.uow.coordinator`).
Nested executions in the task that owns the scope join it.
"""

from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory

__all__ = ["UnitOfWork", "UnitOfWorkFactory"]
