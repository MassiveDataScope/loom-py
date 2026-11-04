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
it and restoring the ContextVars is the job of
:meth:`SQLAlchemyUnitOfWork.close`, which the ordered-commits coordinator
calls phase by phase. A unit of work entered by hand (``async with uow:``)
runs as the only participant of an invocation of its own.

Usage::

    factory = SQLAlchemyUnitOfWorkFactory(session_manager)
    executor = RuntimeExecutor(compiler, uow_factory=factory)
"""

from __future__ import annotations

import asyncio
import contextvars
from typing import Any, ClassVar

from sqlalchemy.exc import DBAPIError, InterfaceError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.logger import get_logger
from loom.core.repository.sqlalchemy.session_manager import SessionManager, bare_url
from loom.core.repository.sqlalchemy.transactional import (
    reset_active_mutations,
    set_active_mutations,
)
from loom.core.transaction import close_atomic_transaction, open_atomic_transaction
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
        self._mutations_token: contextvars.Token[Any] | None = None
        self._transaction_token: contextvars.Token[bool] | None = None
        self._session_cm: Any = None  # context manager from session_manager.session()

    async def begin(self) -> None:
        """Open a new ``AsyncSession`` and publish a fresh mutation list and the atomic signal.

        Raises:
            RuntimeError: If :meth:`begin` has already been called without
                a matching :meth:`commit` or :meth:`rollback`.
        """
        if self._session is not None:
            raise RuntimeError("SQLAlchemyUnitOfWork.begin() called twice without commit/rollback")

        self._session_cm = self._session_manager.session()
        session: AsyncSession = await self._session_cm.__aenter__()
        try:
            self._bind(session)
        except BaseException:
            # The session context manager is already open: close it before
            # the failure leaves, or the connection leaks for good.
            await self._exit_session()
            self._reset_context()
            raise
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

    def _bind(self, session: AsyncSession) -> None:
        """Hold ``session`` and publish a fresh mutation list to the current context."""
        self._session = session
        _, self._mutations_token = set_active_mutations()
        self._transaction_token = open_atomic_transaction()

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
        """Close the session and reset the ContextVars :meth:`begin` published.

        The session closes under ``asyncio.shield``, so a cancellation arriving
        meanwhile lets it finish; the tokens are reset whatever happens.
        Closing a unit of work that holds no session only resets the tokens.
        """
        try:
            await asyncio.shield(self._exit_session())
        finally:
            self._reset_context()

    async def _exit_session(self) -> None:
        """Exit the session context manager; the session is gone afterwards."""
        session_cm, self._session_cm, self._session = self._session_cm, None, None
        if session_cm is not None:
            await session_cm.__aexit__(None, None, None)

    def _reset_context(self) -> None:
        # Closed first: see loom.core.transaction for why this is the token
        # that must be the one to leak if a reset here raises.
        if self._transaction_token is not None:
            close_atomic_transaction(self._transaction_token)
            self._transaction_token = None
        if self._mutations_token is not None:
            reset_active_mutations(self._mutations_token)
            self._mutations_token = None


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
