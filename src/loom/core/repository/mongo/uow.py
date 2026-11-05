"""Unit of work for the Mongo backend.

MongoDB transactions need a replica set and a client session, so they are
opt-in (``stores.<name>.transactions: true``):

- :class:`MongoUnitOfWork` starts a client session and a transaction in
  ``begin``, commits or aborts it, and ends the session afterwards. It is a
  participant of the invocation scope (:mod:`loom.core.uow.scope`), and
  :class:`MongoSessions` finds its session there: a
  :class:`~loom.core.repository.mongo.repository.RepositoryMongo` built with
  ``session_provider=MongoSessions(client, store)`` joins the transaction of
  the use case it runs in. It declares the coordinator's ``phased`` marker:
  ``begin``, ``flush``, ``commit``, ``rollback`` and ``close`` are separate
  calls.
- :class:`NoOpMongoUnitOfWork` (default) opens nothing: each driver call is
  autocommitted and the repository receives no session.

``pymongo`` is imported at module level: this module is only reached through
the Mongo backend, and the persistence registry turns an ``ImportError`` at
entry-point load into a ``ConfigError`` naming the extra.
"""

from __future__ import annotations

import asyncio
import contextvars
from collections.abc import Callable
from typing import Any, ClassVar, cast

from pymongo import AsyncMongoClient
from pymongo.asynchronous.client_session import AsyncClientSession
from pymongo.errors import PyMongoError

from loom.core.logger import get_logger
from loom.core.persistence.stores import StoreKey
from loom.core.transaction import close_atomic_transaction, open_atomic_transaction
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import CommitUncertain, PhysicalStore
from loom.core.uow.scope import EntersAlone, current_handle

_log = get_logger(__name__).bind(component="uow")


class MongoSessions:
    """The client sessions of one Mongo store, as the current invocation holds them.

    The backend passes it as the ``session_provider`` of every repository of
    the store, and :attr:`active_session` is the one public way to read the
    store's session. Both go through the invocation scope's checkpoint.

    Args:
        client: The store's client, the one its unit of work starts sessions on.
        store: Key of the store.
    """

    def __init__(self, client: AsyncMongoClient[Any], store: StoreKey) -> None:
        self._client = client
        self.store = store

    @property
    def active_session(self) -> AsyncClientSession | None:
        """The session this store's unit of work holds in the current invocation, if any.

        Like ``request.user``, its value depends on the context it is read
        from: ``None`` outside an invocation, when this store is not enlisted
        in it, or with ``transactions: false``.

        Raises:
            ScopeOwnershipError: Read from a task other than the one that owns
                the invocation (a ``gather`` child, a task that outlives its
                use case), from a thread, or once the invocation has ended,
                while this store is enlisted.
            ForeignSessionError: When this store is enlisted with another
                client's unit of work.
        """
        return self(False)

    def __call__(self, write: bool) -> AsyncClientSession | None:
        """Return the session a driver call joins, given whether it writes.

        Raises:
            ScopeOwnershipError: See :attr:`active_session`; a write is also
                refused outside the owning task when the store is not enlisted.
            ForeignSessionError: See :attr:`active_session`.
            UndeclaredStoreWrite: When a write targets a store the running
                execution did not declare.
        """
        return cast(
            "AsyncClientSession | None", current_handle(self.store, self._client, write=write)
        )


class NoOpMongoUnitOfWork:
    """Unit of work for ``transactions: false``: no session, every write autocommits.

    Holds no session, so the store's repositories always run without one.
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


class MongoUnitOfWork(EntersAlone):
    """Unit of work for ``transactions: true``: one client session and one transaction.

    Args:
        client: Client the session is started on; needs a replica set.
        store: Key of the store the unit of work belongs to.

    Example::

        async with MongoUnitOfWork(client, store=DEFAULT_STORE):
            await repo.create(entity)
        # committed
    """

    transactional: ClassVar[bool] = True
    phased: ClassVar[bool] = True

    def __init__(self, client: AsyncMongoClient[Any], store: StoreKey) -> None:
        self._client = client
        self._store = store
        self._session: AsyncClientSession | None = None
        self._transaction_token: contextvars.Token[bool] | None = None

    @property
    def owner(self) -> AsyncMongoClient[Any]:
        """The client every session of this unit of work is started on."""
        return self._client

    @property
    def session(self) -> AsyncClientSession | None:
        """The open session, between ``begin`` and ``close``."""
        return self._session

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
        """Start a session and a transaction, and publish the atomic signal.

        The session is ended when ``start_transaction`` fails, so nothing
        leaks on a server that cannot open one.

        Raises:
            RuntimeError: If called twice without a ``commit`` or ``rollback``.
        """
        if self._session is not None:
            raise RuntimeError("MongoUnitOfWork.begin() called twice without commit/rollback")
        session = self._client.start_session()
        try:
            await session.start_transaction()
        except Exception:
            await session.end_session()
            raise
        self._session = session
        self._transaction_token = open_atomic_transaction()
        _log.debug("UoWBegin")

    async def flush(self) -> None:  # NOSONAR S7503 - the PhasedUnitOfWork protocol awaits it
        """Nothing to flush: every write already reached the server inside the transaction.

        Raises:
            RuntimeError: If ``begin`` has not been called.
        """
        self._require_session("flush")

    async def commit(self) -> None:
        """Commit the transaction.

        Raises:
            RuntimeError: If ``begin`` has not been called.
            CommitUncertain: If the error carries the
                ``UnknownTransactionCommitResult`` label: the commit may
                have been applied.
        """
        session = self._require_session("commit")
        try:
            await session.commit_transaction()
        except PyMongoError as exc:
            if exc.has_error_label("UnknownTransactionCommitResult"):
                raise CommitUncertain(exc) from exc
            raise
        _log.debug("UoWCommit")

    async def rollback(self) -> None:
        """Abort the transaction, unless the session no longer holds one.

        After a commit, failed or not, pymongo has marked the transaction
        committed (``in_transaction`` is ``False``) and refuses an abort, so
        nothing is sent: the unit of work does not abort after a failed
        commit, as :meth:`__aexit__` never did.

        Raises:
            RuntimeError: If ``begin`` has not been called.
        """
        session = self._require_session("rollback")
        if not session.in_transaction:
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
    def transactional(
        cls, client: AsyncMongoClient[Any], store: StoreKey
    ) -> MongoUnitOfWorkFactory:
        """Return a factory whose units of work run one transaction on ``client``.

        Args:
            client: Client sessions are started on; needs a replica set.
            store: Key of the store the units of work belong to.
        """
        return cls(lambda: MongoUnitOfWork(client, store))

    def create(self) -> UnitOfWork:
        """Return a fresh, not-yet-begun :class:`UnitOfWork`."""
        return self._new_unit_of_work()


__all__ = [
    "MongoSessions",
    "MongoUnitOfWork",
    "MongoUnitOfWorkFactory",
    "NoOpMongoUnitOfWork",
]
