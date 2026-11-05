"""UnitOfWork and UnitOfWorkFactory protocols.

These protocols define the transaction boundary contract that
:class:`~loom.core.engine.executor.RuntimeExecutor` uses to manage commit
and rollback around each UseCase execution.

No backend-specific types are imported here — implementations may back these
protocols with SQLAlchemy async sessions, in-memory stores, or any other
persistence technology.

Usage::

    class SQLAlchemyUoW:
        transactional: ClassVar[bool] = True

        async def begin(self) -> None: ...
        async def commit(self) -> None: ...
        async def rollback(self) -> None: ...
        async def __aenter__(self) -> "SQLAlchemyUoW": ...
        async def __aexit__(self, *args: object) -> None: ...

    class SQLAlchemyUoWFactory:
        def create(self) -> SQLAlchemyUoW: ...
"""

from __future__ import annotations

from typing import ClassVar, Protocol, runtime_checkable


@runtime_checkable
class UnitOfWork(Protocol):
    """Async transaction boundary protocol.

    Implementations open a session in :meth:`begin`, persist changes in
    :meth:`commit`, and discard them in :meth:`rollback`.  The context
    manager interface delegates to :meth:`begin` / :meth:`commit` /
    :meth:`rollback` automatically.

    :class:`~loom.core.engine.executor.RuntimeExecutor` drives the unit of
    work through :meth:`__aenter__` / :meth:`__aexit__` only; the explicit
    methods stay for adapters and hand-driven use.

    Example::

        async with uow:
            repo.save(entity)
        # commit was called on exit
    """

    transactional: ClassVar[bool]
    """Whether ``commit`` and ``rollback`` are real.

    ``True`` when writes made inside the unit of work become visible only
    at ``commit`` and are discarded by ``rollback``; ``False`` for adapters
    whose writes autocommit (no-op units of work).
    """

    async def begin(self) -> None:
        """Open the underlying session or connection.

        Called automatically by :meth:`__aenter__`.  Must be idempotent
        — implementations may guard against double-begin.
        """
        ...

    async def commit(self) -> None:
        """Persist all changes made within this unit of work.

        Raises:
            RuntimeError: If :meth:`begin` has not been called.
        """
        ...

    async def rollback(self) -> None:
        """Discard all changes made within this unit of work.

        Raises:
            RuntimeError: If :meth:`begin` has not been called.
        """
        ...

    async def __aenter__(self) -> UnitOfWork:
        """Enter the async context manager, calling :meth:`begin`.

        Returns:
            This :class:`UnitOfWork` instance.
        """
        ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Exit the context manager.

        Calls :meth:`commit` when no exception occurred, otherwise calls
        :meth:`rollback`.  Always closes the underlying session, also when
        ``commit`` raises and when the exception is a cancellation: the
        adapter shields its driver I/O so rollback and close complete.
        """
        ...


@runtime_checkable
class UnitOfWorkFactory(Protocol):
    """Factory that creates fresh :class:`UnitOfWork` instances.

    One factory is typically registered per application and injected into
    :class:`~loom.core.engine.executor.RuntimeExecutor` at startup.

    Example::

        factory = SQLAlchemyUnitOfWorkFactory(session_manager)
        executor = RuntimeExecutor(compiler, uow_factory=factory)
    """

    def create(self) -> UnitOfWork:
        """Return a new, not-yet-begun :class:`UnitOfWork` instance.

        Returns:
            A fresh :class:`UnitOfWork` ready for :meth:`~UnitOfWork.begin`.
        """
        ...
