from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from functools import wraps
from typing import Any, ClassVar, Generic, TypeVar, cast

from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.logger import get_logger
from loom.core.repository.abc import (
    BulkCreatable,
    Countable,
    Creatable,
    CursorCodec,
    Deletable,
    IdT,
    Listable,
    OutputT,
    Readable,
    Updatable,
)
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.mixins import (
    SQLAlchemyBulkCreateMixin,
    SQLAlchemyCreateMixin,
    SQLAlchemyDeleteMixin,
    SQLAlchemyReadMixin,
    SQLAlchemyUpdateMixin,
)
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.transactional import get_active_session

R = TypeVar("R")


def with_session_scope(
    method: Callable[..., Awaitable[R]],
) -> Callable[..., Awaitable[R]]:
    """Inject repository-managed session into custom repository methods."""

    @wraps(method)
    async def wrapper(
        self: RepositorySQLAlchemy[Any, Any],
        *args: Any,
        session: AsyncSession | None = None,
        **kwargs: Any,
    ) -> R:
        async with self._session_scope(session) as scoped_session:
            return await method(self, scoped_session, *args, **kwargs)

    return cast(Callable[..., Awaitable[R]], wrapper)


class RepositorySQLAlchemy(  # type: ignore[misc]  # mypy/pyright can't resolve same-named methods across Mixin+Protocol bases; runtime behaviour is correct
    SQLAlchemyCreateMixin[OutputT, IdT],
    SQLAlchemyBulkCreateMixin[OutputT, IdT],
    SQLAlchemyReadMixin[OutputT, IdT],
    SQLAlchemyUpdateMixin[OutputT, IdT],
    SQLAlchemyDeleteMixin[OutputT, IdT],
    Readable[OutputT],
    Creatable[OutputT],
    BulkCreatable[OutputT],
    Updatable[OutputT],
    Deletable[OutputT],
    Listable[OutputT],
    Countable[OutputT],
    Generic[OutputT, IdT],
):
    """Base SQLAlchemy repository with context-aware session management.

    Pass ``model`` (a Struct-based ``BaseModel``) to ``__init__``; the
    repository uses the compiled SA class for queries and returns the
    Struct directly.

    Args:
        session_manager: Shared session manager.
        model: Model the repository serves.
        cursor_codec: Codec that issues and verifies cursor tokens; the
            backend builds one from ``persistence.cursors`` and shares it.
            Cursors are bound to the row-level-security scope values of the
            caller when the session manager has them, to no scope otherwise.
            A codec passed here by hand skips the deployed-environment
            refusal of an opt-out that ``register_cursor_codec`` applies; a
            custom builder passes ``context.container.resolve(CursorCodec)``.
    """

    backend_name: ClassVar[str] = "sqlalchemy"

    def __init__(
        self,
        session_manager: SessionManager,
        model: type,
        *,
        cursor_codec: CursorCodec,
    ) -> None:
        self.session_manager = session_manager
        self.model = model
        self.cursor_codec = cursor_codec
        self._init_struct_model()
        self.log = get_logger(__name__).bind(repository=self.__class__.__name__)

    def has_caller_scoped_session(self) -> bool:
        """Whether a read would run inside a session bound to the caller's context.

        True inside a ``@transactional`` scope or a unit of work: the session
        was opened by the caller, dies when the caller unwinds, and holds
        writes only that caller can see.  False when the repository would open
        and close a session of its own for the call.

        Implements
        :class:`~loom.core.repository.abc.session_scope.SupportsCallerScopedSession`.
        :func:`loom.core.transaction.in_atomic_transaction` is the neutral
        marker this capability used to be a placeholder for, but it answers a
        different question — whether *any* atomic transaction is open — not
        whether *this session* is scoped to the caller, which is what keeps
        the repository cache out of the caller's transaction.  Keep using this
        method for that; it is not superseded.

        Returns:
            ``True`` when the active session is bound to this repository's engine.
        """
        session = get_active_session()
        return session is not None and self._bound_to_this_engine(session)

    def _bound_to_this_engine(self, session: AsyncSession) -> bool:
        """Whether *session* belongs to this repository's engine.

        A unit of work publishes one session for every repository. A repository
        whose engine is a different one must not join that transaction: the
        two databases do not commit together. SQLAlchemy 2.1 reports the async
        engine or connection. 2.0 keeps that async object on the session and
        the sync engine on ``sync_session``. A connection counts when its
        engine is either of those. A session with no bind is not reused.
        """
        engine = self.session_manager.engine
        bind: object = getattr(session, "bind", None)
        if bind is None:
            sync = getattr(session, "sync_session", None)
            bind = getattr(sync, "bind", None)
        if bind is engine or bind is engine.sync_engine:
            return True
        nested = getattr(bind, "engine", None)
        return nested is engine or nested is engine.sync_engine

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        """Handle post-commit mutation events (cache invalidation hook)."""
        self.log.debug("RepositoryTransactionCommitted", mutation_count=len(events))

    @asynccontextmanager
    async def _session_scope(
        self, session: AsyncSession | None = None
    ) -> AsyncIterator[AsyncSession]:
        """Reuse the active session when it is bound to this engine, or open one."""
        if session is not None:
            yield session
            return

        context_session = get_active_session()
        if context_session is not None and self._bound_to_this_engine(context_session):
            yield context_session
            return

        async with self.session_manager.session() as new_session:
            try:
                yield new_session
                await new_session.commit()
            except Exception:
                await new_session.rollback()
                self.log.exception("RepositorySessionRollback")
                raise
