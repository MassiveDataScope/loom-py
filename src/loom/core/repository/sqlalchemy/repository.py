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

R = TypeVar("R")


def with_session_scope(
    method: Callable[..., Awaitable[R]],
) -> Callable[..., Awaitable[R]]:
    """Inject repository-managed session into custom repository methods.

    The method joins its store's session when the store is enlisted in the
    invocation, as raw SQL does, and runs in a session of its own otherwise.
    It declares no write intent, so the scope treats it as a read: a write
    through it is not recorded for the commit order, and when its store is
    not enlisted it is not refused either. Inside an invocation, a custom
    write to a store that is not enlisted therefore commits per call, on its
    own, outside the invocation's unit of work, and survives a rollback of
    the invocation (T079 makes this an error, PR 6).
    """

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
        """Whether a read would run inside the session of its store's participant.

        True while the store is enlisted in the current invocation: the
        session was opened by the caller, dies when the caller unwinds, and
        holds writes only that caller can see.  False when the repository
        would open and close a session of its own for the call.

        Implements
        :class:`~loom.core.repository.abc.session_scope.SupportsCallerScopedSession`.
        :func:`loom.core.transaction.in_atomic_transaction` answers a
        different question — whether *any* atomic transaction is open — not
        whether *this* store's session is scoped to the caller, which is
        what keeps the repository cache out of the caller's transaction.

        Returns:
            ``True`` when the store's participant holds a session.

        Raises:
            ScopeOwnershipError: See :attr:`SessionManager.active_session`.
        """
        return self.session_manager.active_session is not None

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None:
        """Handle post-commit mutation events (cache invalidation hook)."""
        self.log.debug("RepositoryTransactionCommitted", mutation_count=len(events))

    @asynccontextmanager
    async def _session_scope(
        self, session: AsyncSession | None = None, *, write: bool = False
    ) -> AsyncIterator[AsyncSession]:
        """Run in *session*, in the session of the store's participant, or in one of its own.

        Args:
            session: A session the caller already holds.
            write: Whether the operation writes; a write is recorded on the
                participant for the commit order.

        Raises:
            ScopeOwnershipError: See :attr:`SessionManager.active_session`.
            ForeignSessionError: See :attr:`SessionManager.active_session`.
        """
        if session is not None:
            yield session
            return

        context_session = self.session_manager.joined_session(write=write)
        if context_session is not None:
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
