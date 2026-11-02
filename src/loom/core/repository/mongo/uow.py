"""Unit of work for the Mongo backend.

MongoDB transactions need a replica set and a client session, so they are
opt-in (``stores.<name>.transactions: true``):

- :class:`MongoUnitOfWork` starts a client session and a transaction in
  ``begin``, commits or aborts it, and ends the session afterwards. The
  session is published through a ``ContextVar`` that :func:`active_session`
  reads, so a :class:`~loom.core.repository.mongo.repository.RepositoryMongo`
  built with ``session_provider=active_session`` joins the transaction of the
  use case it runs in. The token is reset on exit, as the SQLAlchemy unit of
  work does. It declares the coordinator's ``phased`` marker: ``begin``,
  ``flush``, ``commit``, ``rollback`` and ``close`` are separate calls.
- :class:`NoOpMongoUnitOfWork` (default) opens nothing and touches no
  ``ContextVar``, mirroring
  :class:`~loom.core.repository.dynamodb.uow.DynamoUnitOfWork`: each driver
  call is autocommitted and the repository receives no session.

``pymongo`` is imported at module level: this module is only reached through
the Mongo backend, and the persistence registry turns an ``ImportError`` at
entry-point load into a ``ConfigError`` naming the extra.
"""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Callable
from typing import Any, ClassVar

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession

from loom.core.logger import get_logger
from loom.core.transaction import close_atomic_transaction, open_atomic_transaction
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import PhysicalStore
from loom.core.uow.scope import refuse_inside_open_scope

_log = get_logger(__name__).bind(component="uow")

_active_session: contextvars.ContextVar[AsyncClientSession | None] = contextvars.ContextVar(
    "loom_mongo_active_session", default=None
)


def active_session() -> AsyncClientSession | None:
    """Return the client session of the enclosing unit of work, or ``None``.

    Pass it as ``session_provider`` to
    :class:`~loom.core.repository.mongo.repository.RepositoryMongo`.
    """
    return _active_session.get()


class NoOpMongoUnitOfWork:
    """Unit of work for ``transactions: false``: no session, every write autocommits.

    Leaves :func:`active_session` untouched, so one nested inside a
    :class:`MongoUnitOfWork` neither hides nor releases that session.
    """

    transactional: ClassVar[bool] = False

    async def begin(self) -> None:
        """No-op: no session is opened."""

    async def commit(self) -> None:
        """No-op: each driver call is already committed."""

    async def rollback(self) -> None:
        """No-op: autocommitted writes cannot be discarded here."""

    async def __aenter__(self) -> NoOpMongoUnitOfWork:
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if exc_type is None:
            await self.commit()
        else:
            await self.rollback()


class MongoUnitOfWork:
    """Unit of work for ``transactions: true``: one client session and one transaction.

    Args:
        client: Client the session is started on; needs a replica set.

    Example::

        async with MongoUnitOfWork(client):
            await repo.create(entity)
        # committed
    """

    transactional: ClassVar[bool] = True
    phased: ClassVar[bool] = True

    def __init__(self, client: AsyncMongoClient[Any]) -> None:
        self._client = client
        self._session: AsyncClientSession | None = None
        self._token: contextvars.Token[AsyncClientSession | None] | None = None
        self._transaction_token: contextvars.Token[bool] | None = None
        self._commit_attempted = False

    @property
    def owner(self) -> AsyncMongoClient[Any]:
        """The client every session of this unit of work is started on."""
        return self._client

    @property
    def physical(self) -> PhysicalStore:
        """The cluster this unit of work writes to, named by the hosts the client knows now.

        Read from the client's current topology description, which needs no
        connection: the seed addresses until the client has discovered the
        cluster, its discovered members after.
        """
        servers = self._client.topology_description.server_descriptions()
        target = ",".join(sorted(f"{host}:{port}" for host, port in servers))
        return PhysicalStore(backend="mongo", target=target)

    async def begin(self) -> None:
        """Start a session and a transaction, publishing the session to :func:`active_session`.

        The session is ended when ``start_transaction`` fails, so nothing
        leaks on a server that cannot open one.

        Raises:
            RuntimeError: If called twice without a ``commit`` or ``rollback``.
        """
        if self._token is not None:
            raise RuntimeError("MongoUnitOfWork.begin() called twice without commit/rollback")
        session = self._client.start_session()
        try:
            await session.start_transaction()
        except Exception:
            await session.end_session()
            raise
        self._session = session
        self._commit_attempted = False
        self._token = _active_session.set(session)
        self._transaction_token = open_atomic_transaction()
        _log.debug("UoWBegin")

    async def flush(self) -> None:
        """Nothing to flush: every write already reached the server inside the transaction.

        Raises:
            RuntimeError: If ``begin`` has not been called.
        """
        self._require_session("flush")

    async def commit(self) -> None:
        """Commit the transaction.

        Raises:
            RuntimeError: If ``begin`` has not been called.
        """
        session = self._require_session("commit")
        self._commit_attempted = True
        await session.commit_transaction()
        _log.debug("UoWCommit")

    async def rollback(self) -> None:
        """Abort the transaction, unless a commit was already attempted.

        After a commit, failed or not, pymongo has marked the transaction
        committed and refuses an abort, so nothing is sent: the unit of work
        does not abort after a failed commit, as :meth:`__aexit__` never did.

        Raises:
            RuntimeError: If ``begin`` has not been called.
        """
        session = self._require_session("rollback")
        if self._commit_attempted:
            return
        await session.abort_transaction()
        _log.debug("UoWRollback")

    async def close(self) -> None:
        """End the session and reset the ContextVars :meth:`begin` published.

        The session ends under ``asyncio.shield``, so a cancellation arriving
        meanwhile lets it finish; the tokens are reset whatever happens. A
        failure while ending is logged, not raised.
        """
        try:
            await asyncio.shield(self._end_session())
        finally:
            self._reset_context()

    async def __aenter__(self) -> MongoUnitOfWork:
        """Begin the unit of work and return it.

        Raises:
            UnitOfWorkInsideScope: Inside an open invocation scope, where
                this transaction would commit on its own, outside the invocation.
        """
        refuse_inside_open_scope(self)
        await self.begin()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Commit on clean exit, abort on exception, always end the session.

        A failed ``commit`` ends the session without a second abort: pymongo
        marks the transaction committed-or-aborted itself.  The driver I/O of
        abort and ``end_session`` runs under ``asyncio.shield`` so a
        cancellation arriving meanwhile lets it finish; the ``ContextVar``
        token is reset here, in the caller's context.
        """
        try:
            if exc_type is None:
                await self._commit_then_close()
            else:
                await asyncio.shield(self._abort_then_close())
        finally:
            self._reset_context()

    async def _commit_then_close(self) -> None:
        try:
            await self.commit()
        finally:
            await asyncio.shield(self._end_session())

    async def _abort_then_close(self) -> None:
        try:
            await self.rollback()
        except Exception:
            _log.exception("UoWRollbackFailed")
        finally:
            await self._end_session()

    async def _end_session(self) -> None:
        """End the session; the unit of work holds none afterwards.

        A failure while ending is logged, not raised: it would otherwise
        replace the exception the use case is already propagating.
        """
        session, self._session = self._session, None
        if session is None:
            return
        try:
            await session.end_session()
        except Exception:
            _log.exception("UoWCloseFailed")

    def _reset_context(self) -> None:
        # Closed first: see loom.core.transaction for why this is the token
        # that must be the one to leak if a reset here raises.
        if self._transaction_token is not None:
            close_atomic_transaction(self._transaction_token)
            self._transaction_token = None
        if self._token is not None:
            _active_session.reset(self._token)
            self._token = None

    def _require_session(self, operation: str) -> AsyncClientSession:
        if self._session is None:
            raise RuntimeError(f"MongoUnitOfWork.{operation}() called before begin()")
        return self._session


class MongoUnitOfWorkFactory:
    """Factory that creates fresh Mongo units of work.

    Build it through :meth:`without_transactions` or :meth:`transactional`;
    register one instance and inject it into
    :class:`~loom.core.engine.executor.RuntimeExecutor` at startup.

    Args:
        new_unit_of_work: Returns a fresh, not-yet-begun unit of work.
    """

    def __init__(self, new_unit_of_work: Callable[[], UnitOfWork]) -> None:
        self._new_unit_of_work = new_unit_of_work

    @classmethod
    def without_transactions(cls) -> MongoUnitOfWorkFactory:
        """Return a factory whose units of work open no session."""
        return cls(NoOpMongoUnitOfWork)

    @classmethod
    def transactional(cls, client: AsyncMongoClient[Any]) -> MongoUnitOfWorkFactory:
        """Return a factory whose units of work run one transaction on ``client``.

        Args:
            client: Client sessions are started on; needs a replica set.
        """
        return cls(lambda: MongoUnitOfWork(client))

    def create(self) -> UnitOfWork:
        """Return a fresh, not-yet-begun :class:`UnitOfWork`."""
        return self._new_unit_of_work()


__all__ = [
    "MongoUnitOfWork",
    "MongoUnitOfWorkFactory",
    "NoOpMongoUnitOfWork",
    "active_session",
]
