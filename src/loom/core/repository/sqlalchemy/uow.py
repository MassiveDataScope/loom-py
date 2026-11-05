"""SQLAlchemy implementation of the UnitOfWork protocol.

Wraps a :class:`~loom.core.repository.sqlalchemy.session_manager.SessionManager`
to provide a single, atomic transaction scope per UseCase execution.

:meth:`SQLAlchemyUnitOfWork.begin` opens an ``AsyncSession`` from the
``SessionManager``. The unit of work is a participant of an invocation scope
(:mod:`loom.core.uow.scope`): repositories, ``@transactional`` and
:attr:`~loom.core.repository.sqlalchemy.session_manager.SessionManager.active_session`
find its :attr:`~SQLAlchemyUnitOfWork.session` through the scope, never
through a context variable of their own.

On :meth:`commit` the session is committed.  On :meth:`rollback` all pending
changes are discarded.  The explicit methods leave the session open: closing
it is the job of
:meth:`SQLAlchemyUnitOfWork.close`, which the ordered-commits coordinator
calls phase by phase. A unit of work entered by hand (``async with uow:``)
runs as the only participant of an invocation of its own.

Usage::

    factory = SQLAlchemyUnitOfWorkFactory(session_manager)
    executor = RuntimeExecutor(compiler, uow_factory=factory)
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.logger import get_logger
from loom.core.repository.sqlalchemy.session_manager import SessionManager, bare_url
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import CommitUncertain, PhysicalStore
from loom.core.uow.scope import EntersAlone

_log = get_logger(__name__).bind(component="uow")


class SQLAlchemyUnitOfWork(EntersAlone):
    """Async SQLAlchemy Unit of Work backed by ``SessionManager``.

    Manages a single ``AsyncSession`` lifetime: opens it in :meth:`begin`,
    commits or rolls back, and exposes it as :attr:`session`, which the
    repositories of its store join through the invocation scope without
    needing an explicit session argument.

    Args:
        session_manager: Session factory used to create the underlying
            ``AsyncSession``.

    Example::

        uow = SQLAlchemyUnitOfWork(session_manager)
        async with uow:
            await repo.create(entity)
        # committed
    """

    transactional: ClassVar[bool] = True
    phased: ClassVar[bool] = True

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager
        self._store = session_manager.store
        self._session: AsyncSession | None = None
        self._session_cm: Any = None  # context manager from session_manager.session()

    async def begin(self) -> None:
        """Open a new ``AsyncSession``.

        Raises:
            RuntimeError: If :meth:`begin` has already been called without
                a matching :meth:`commit` or :meth:`rollback`.
        """
        if self._session is not None:
            raise RuntimeError("SQLAlchemyUnitOfWork.begin() called twice without commit/rollback")

        self._session_cm = self._session_manager.session()
        self._session = await self._session_cm.__aenter__()
        _log.debug("UoWBegin")

    @property
    def owner(self) -> SessionManager:
        """The session manager every session of this unit of work comes from."""
        return self._session_manager

    @property
    def session(self) -> AsyncSession | None:
        """The open session, between :meth:`begin` and :meth:`close`."""
        return self._session

    @property
    def physical(self) -> PhysicalStore:
        """The database this unit of work writes to, named by its URL without credentials."""
        return PhysicalStore(
            backend="sqlalchemy", target=bare_url(self._session_manager.engine.url)
        )

    async def flush(self) -> None:
        """Send pending changes to the database without committing them.

        Raises:
            RuntimeError: If :meth:`begin` has not been called.
        """
        if self._session is None:
            raise RuntimeError("SQLAlchemyUnitOfWork.flush() called before begin()")
        await self._session.flush()

    async def commit(self) -> None:
        """Flush and commit all pending changes to the database.

        The session stays open; closing it is the job of :meth:`close`.

        Raises:
            RuntimeError: If :meth:`begin` has not been called.
            CommitUncertain: If the connection failed during ``COMMIT``
                (``OperationalError``, ``InterfaceError``, an invalidated
                connection, or an ``OSError`` such as a timeout): the commit
                may have been applied. The connection is invalidated first,
                so it never returns to the pool.
        """
        if self._session is None:
            raise RuntimeError("SQLAlchemyUnitOfWork.commit() called before begin()")
        try:
            await self._session.commit()
        except (DBAPIError, OSError) as exc:
            if isinstance(exc, OSError | OperationalError | InterfaceError) or (
                exc.connection_invalidated
            ):
                await self._invalidate(self._session)
                raise CommitUncertain(exc) from exc
            raise
        _log.debug("UoWCommit")

    @staticmethod
    async def _invalidate(session: AsyncSession) -> None:
        try:
            await session.invalidate()
        except Exception:
            _log.exception("UoWInvalidateFailed")

    async def rollback(self) -> None:
        """Discard all pending changes.

        The session stays open; closing it is the job of :meth:`close`.

        Raises:
            RuntimeError: If :meth:`begin` has not been called.
        """
        if self._session is None:
            raise RuntimeError("SQLAlchemyUnitOfWork.rollback() called before begin()")
        await self._session.rollback()
        _log.debug("UoWRollback")

    async def close(self) -> None:
        """Close the session, if any, under ``asyncio.shield``: a cancellation lets it finish."""
        await asyncio.shield(self._exit_session())

    async def _exit_session(self) -> None:
        """Exit the session context manager; the session is gone afterwards.

        A transaction still open, its rollback timed out before it began, is
        invalidated first: its connection is closed, not reset and pooled.
        """
        session_cm, session = self._session_cm, self._session
        self._session_cm, self._session = None, None
        if session is not None and session.in_transaction():
            await self._invalidate(session)
        if session_cm is not None:
            await session_cm.__aexit__(None, None, None)


class SQLAlchemyUnitOfWorkFactory:
    """Factory that creates fresh :class:`SQLAlchemyUnitOfWork` instances.

    Register one instance in the DI container and inject it into
    :class:`~loom.core.engine.executor.RuntimeExecutor` at startup.

    Args:
        session_manager: Shared session manager used by all created UoWs.

    Example::

        factory = SQLAlchemyUnitOfWorkFactory(session_manager)
        executor = RuntimeExecutor(compiler, uow_factory=factory)
    """

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager

    def create(self) -> UnitOfWork:
        """Return a fresh, not-yet-begun :class:`UnitOfWork`.

        Returns:
            A new :class:`UnitOfWork` backed by the shared
            session manager.
        """
        return SQLAlchemyUnitOfWork(self._session_manager)
