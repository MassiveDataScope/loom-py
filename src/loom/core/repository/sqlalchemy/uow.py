"""SQLAlchemy implementation of the UnitOfWork protocol.

Wraps a :class:`~loom.core.repository.sqlalchemy.session_manager.SessionManager`
to provide a single, atomic transaction scope per UseCase execution.

When :meth:`SQLAlchemyUnitOfWork.begin` is called it:

1. Opens an ``AsyncSession`` from the ``SessionManager``.
2. Sets the ``_active_session`` ContextVar so that:
   - :class:`~loom.core.repository.sqlalchemy.repository.RepositorySQLAlchemy`
     ``_session_scope`` automatically joins the transaction.
   - Existing ``@transactional`` decorators detect the active session and
     skip opening a redundant nested transaction.

On :meth:`commit` the session is flushed and committed.  On :meth:`rollback`
all pending changes are discarded.  Closing the session and restoring the
ContextVar is the job of :meth:`SQLAlchemyUnitOfWork.__aexit__`, which the
executor drives; the explicit methods leave the session open.

Usage::

    factory = SQLAlchemyUnitOfWorkFactory(session_manager)
    executor = RuntimeExecutor(compiler, uow_factory=factory)
"""

from __future__ import annotations

import asyncio
import contextvars
from typing import Any, ClassVar

from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.logger import get_logger
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import (
    reset_active_mutations,
    reset_active_session,
    set_active_mutations,
    set_active_session,
)
from loom.core.transaction import close_atomic_transaction, open_atomic_transaction
from loom.core.uow.abc import UnitOfWork

_log = get_logger(__name__).bind(component="uow")


class SQLAlchemyUnitOfWork:
    """Async SQLAlchemy Unit of Work backed by ``SessionManager``.

    Manages a single ``AsyncSession`` lifetime: opens it in :meth:`begin`,
    commits or rolls back on exit, and exposes the session through the
    ``_active_session`` ContextVar so repositories and ``@transactional``
    decorators automatically participate in the same transaction without
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

    def __init__(self, session_manager: SessionManager) -> None:
        self._session_manager = session_manager
        self._session: AsyncSession | None = None
        self._session_token: contextvars.Token[Any] | None = None
        self._mutations_token: contextvars.Token[Any] | None = None
        self._transaction_token: contextvars.Token[bool] | None = None
        self._session_cm: Any = None  # context manager from session_manager.session()

    async def begin(self) -> None:
        """Open a new ``AsyncSession`` and bind it to the current context.

        Sets the ``_active_session`` ContextVar so repositories and
        ``@transactional`` decorators can discover and reuse the session.

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

    def _bind(self, session: AsyncSession) -> None:
        """Publish ``session`` and a fresh mutation list to the current context."""
        self._session = session
        self._session_token = set_active_session(session, owner=self._session_manager)
        _, self._mutations_token = set_active_mutations()
        self._transaction_token = open_atomic_transaction()

    async def commit(self) -> None:
        """Flush and commit all pending changes to the database.

        The session stays open; closing it is the job of :meth:`__aexit__`.

        Raises:
            RuntimeError: If :meth:`begin` has not been called.
        """
        if self._session is None:
            raise RuntimeError("SQLAlchemyUnitOfWork.commit() called before begin()")
        await self._session.commit()
        _log.debug("UoWCommit")

    async def rollback(self) -> None:
        """Discard all pending changes.

        The session stays open; closing it is the job of :meth:`__aexit__`.

        Raises:
            RuntimeError: If :meth:`begin` has not been called.
        """
        if self._session is None:
            raise RuntimeError("SQLAlchemyUnitOfWork.rollback() called before begin()")
        await self._session.rollback()
        _log.debug("UoWRollback")

    async def __aenter__(self) -> SQLAlchemyUnitOfWork:
        """Begin the unit of work.

        Returns:
            This instance.
        """
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Commit on clean exit, roll back otherwise, always close the session.

        A failing ``commit`` is rolled back before the session closes.  The
        driver I/O of rollback and close runs under ``asyncio.shield`` so a
        cancellation arriving meanwhile lets it finish; the ``ContextVar``
        tokens are reset here, in the caller's context.

        Args:
            exc_type: Exception type, or ``None`` on clean exit.
            exc_val: Exception instance, or ``None``.
            exc_tb: Traceback, or ``None``.
        """
        try:
            if exc_type is None:
                await self._commit_then_close()
            else:
                await asyncio.shield(self._rollback_then_close())
        finally:
            self._reset_context()

    async def _commit_then_close(self) -> None:
        try:
            await self.commit()
        except BaseException:
            await asyncio.shield(self._rollback_then_close())
            raise
        await asyncio.shield(self._exit_session_logging_failure())

    async def _rollback_then_close(self) -> None:
        try:
            await self.rollback()
        except Exception:
            _log.exception("UoWRollbackFailed")
        finally:
            await self._exit_session_logging_failure()

    async def _exit_session_logging_failure(self) -> None:
        """Close on the failure path: a close error must not replace the business one."""
        try:
            await self._exit_session()
        except Exception:
            _log.exception("UoWCloseFailed")

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
        if self._session_token is not None:
            reset_active_session(self._session_token)
            self._session_token = None
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
