from __future__ import annotations

import contextvars
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from functools import partial, wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Concatenate,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
    runtime_checkable,
)

from sqlalchemy.ext.asyncio import AsyncSession

from loom.core.engine.post_commit import PostCommitChannel, bind_channel, reset_channel
from loom.core.errors.errors import ForeignSessionError
from loom.core.logger import get_logger
from loom.core.repository.mutation import MutationEvent
from loom.core.transaction import close_atomic_transaction, open_atomic_transaction

if TYPE_CHECKING:  # session_manager is the heavier module; it does not import this one
    from loom.core.repository.sqlalchemy.session_manager import SessionManager


@runtime_checkable
class SupportsPostCommit(Protocol):
    """Protocol for objects that react to committed transactions."""

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None: ...


T = TypeVar("T")
P = ParamSpec("P")


@dataclass(frozen=True, slots=True)
class _Bound:
    """The session bound to the context, and the session manager that opened it, if known."""

    session: AsyncSession
    owner: SessionManager | None


_active_session: contextvars.ContextVar[_Bound | None] = contextvars.ContextVar(
    "_active_session",
    default=None,
)
_mutations: contextvars.ContextVar[list[MutationEvent] | None] = contextvars.ContextVar(
    "_mutations",
    default=None,
)
_log = get_logger(__name__).bind(component="transactional")


def get_active_session() -> AsyncSession | None:
    """Return the transactional session bound to the current context, or ``None``.

    Returns:
        The active ``AsyncSession`` if inside a ``@transactional`` scope
        or inside a :class:`~loom.core.repository.sqlalchemy.uow.SQLAlchemyUnitOfWork`
        managed by :class:`~loom.core.engine.executor.RuntimeExecutor`.
    """
    bound = _active_session.get()
    return None if bound is None else bound.session


def session_to_join(session_manager: SessionManager) -> AsyncSession | None:
    """Return the active session a repository of *session_manager* joins, if any.

    Args:
        session_manager: The repository's own session manager.

    Returns:
        The active session, or ``None`` when none is bound. A session bound
        without an owner (:func:`set_active_session` called without
        ``owner``) is joined unchecked.

    Raises:
        ForeignSessionError: When another session manager opened it.
    """
    bound = _active_session.get()
    if bound is None:
        return None
    if bound.owner is not None and bound.owner is not session_manager:
        raise ForeignSessionError.between(_described(session_manager), _described(bound.owner))
    return bound.session


def _described(session_manager: SessionManager) -> str:
    """Name the store and connection of *session_manager* the way errors name them."""
    return ForeignSessionError.side(session_manager.store, repr(session_manager.engine.url))


def set_active_session(
    session: AsyncSession,
    *,
    owner: SessionManager | None = None,
) -> contextvars.Token[_Bound | None]:
    """Bind ``session`` to the current async context.

    Used by :class:`~loom.core.repository.sqlalchemy.uow.SQLAlchemyUnitOfWork`
    so that :func:`get_active_session` returns the UoW session, making
    repository ``_session_scope`` and :func:`transactional` seamlessly
    participate in the same transaction.

    Args:
        session: The ``AsyncSession`` to bind.
        owner: The session manager that opened it; a repository of another
            manager then refuses to join it (:func:`session_to_join`).

    Returns:
        A reset token that must be passed to :func:`reset_active_session`.
    """
    return _active_session.set(_Bound(session, owner))


def reset_active_session(token: contextvars.Token[_Bound | None]) -> None:
    """Restore the session ContextVar to its previous state.

    Args:
        token: The token returned by :func:`set_active_session`.
    """
    _active_session.reset(token)


MutationsToken = contextvars.Token[list[MutationEvent] | None]


def set_active_mutations() -> tuple[list[MutationEvent], MutationsToken]:
    """Initialise a fresh mutations list for the current context.

    Returns:
        A tuple of ``(mutations_list, reset_token)`` where the list collects
        :class:`~loom.core.repository.mutation.MutationEvent` objects and
        the token is passed to :func:`reset_active_mutations` on exit.
    """
    mutations: list[MutationEvent] = []
    token = _mutations.set(mutations)
    return mutations, token


def reset_active_mutations(
    token: MutationsToken,
) -> None:
    """Restore the mutations ContextVar to its previous state.

    Args:
        token: The token returned by :func:`set_active_mutations`.
    """
    _mutations.reset(token)


def record_mutation(event: MutationEvent) -> None:
    """Append a mutation event to the current transaction's pending list.

    If called outside a ``@transactional`` scope the event is silently discarded.

    Args:
        event: The mutation event to record.
    """
    events = _mutations.get()
    if events is None:
        return
    events.append(event)


def get_pending_mutations() -> tuple[MutationEvent, ...]:
    """Return all mutation events recorded in the current transaction scope.

    Returns:
        A tuple of ``MutationEvent`` instances, empty if none were recorded.
    """
    events = _mutations.get()
    if not events:
        return ()
    return tuple(events)


def transactional(
    method: Callable[Concatenate[Any, P], Awaitable[T]],
) -> Callable[Concatenate[Any, P], Awaitable[T]]:
    """Create a single transaction boundary for service/orchestrator use cases.

    When a session of the owner's own ``session_manager`` is already active
    (an outer ``@transactional`` call or a unit of work driven by the
    executor) the method joins it and nothing else happens: the owner of that
    session runs the post-commit hooks. A session another session manager
    opened is never joined.
    When the decorator opens the session itself it commits, then runs
    ``on_transaction_committed(pending)`` on the owner and on its
    :class:`SupportsPostCommit` attributes through the post-commit channel
    (:mod:`loom.core.engine.post_commit`) after the session is closed.  The
    decorator owns that channel whenever it owns the session: a channel
    bound by an outer context is left untouched and restored afterwards, so
    a committed transaction always drains its own actions.

    Args:
        method: Async method of an object exposing ``session_manager``.

    Returns:
        The wrapped method.

    Raises:
        TypeError: If applied to a repository method or the owner has no
            ``session_manager`` with a ``session()`` context manager.
        ForeignSessionError: If the active session belongs to another
            session manager.
        PostCommitError: If a hook failed after the commit.
    """

    @wraps(method)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> T:
        _reject_repository_owner(self)
        session_manager = _require_session_manager(self)
        if session_to_join(session_manager) is not None:
            _log.debug(
                "TransactionalSessionReused",
                owner=self.__class__.__name__,
                method=method.__name__,
            )
            return await method(self, *args, **kwargs)

        channel = PostCommitChannel()
        channel_token = bind_channel(channel)
        try:
            result = await _run_in_owned_session(
                self, method, session_manager, channel, args, kwargs
            )
        except BaseException:
            channel.discard()
            raise
        finally:
            reset_channel(channel_token)
        await channel.drain(committed=True)
        return result

    return cast(Callable[Concatenate[Any, P], Awaitable[T]], wrapper)


def _reject_repository_owner(owner: Any) -> None:
    # Local import: ``repository`` imports this module for ``get_active_session``.
    from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy

    if isinstance(owner, RepositorySQLAlchemy):
        raise TypeError(
            "@transactional is intended for service/orchestrator boundaries, "
            "not repository methods.",
        )


def _require_session_manager(owner: Any) -> Any:
    session_manager = getattr(owner, "session_manager", None)
    if session_manager is None or not callable(getattr(session_manager, "session", None)):
        raise TypeError(
            f"{owner.__class__.__name__} must have a 'session_manager' attribute "
            f"with a .session() context manager to use @transactional.",
        )
    return session_manager


async def _run_in_owned_session(
    owner: Any,
    method: Callable[..., Awaitable[T]],
    session_manager: Any,
    channel: PostCommitChannel,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> T:
    """Open the session, run ``method``, commit and enqueue the hooks on ``channel``."""
    async with session_manager.session() as session:
        session_token = set_active_session(session, owner=session_manager)
        mutations_token = _mutations.set([])
        transaction_token = open_atomic_transaction()
        try:
            result = await method(owner, *args, **kwargs)
            await session.commit()
            pending = get_pending_mutations()
            _log.info(
                "TransactionCommitted",
                owner=owner.__class__.__name__,
                method=method.__name__,
                mutation_count=len(pending),
            )
            _enqueue_post_commit_hooks(owner, pending, channel)
            return result
        except Exception:
            await session.rollback()
            _log.exception(
                "TransactionRolledBack",
                owner=owner.__class__.__name__,
                method=method.__name__,
            )
            raise
        finally:
            # Closed first: see loom.core.transaction for why this is the
            # token that must be the one to leak if a reset here raises.
            close_atomic_transaction(transaction_token)
            _active_session.reset(session_token)
            _mutations.reset(mutations_token)


def _enqueue_post_commit_hooks(
    owner: Any,
    pending: tuple[MutationEvent, ...],
    channel: PostCommitChannel,
) -> None:
    """Queue the owner's hook, then its dependencies' hooks, on ``channel``."""
    hooks: list[SupportsPostCommit] = []
    if isinstance(owner, SupportsPostCommit):
        hooks.append(owner)
    hooks.extend(_iter_post_commit_dependencies(owner))
    for hook in hooks:
        channel.enqueue(partial(hook.on_transaction_committed, pending))


def _iter_post_commit_dependencies(owner: Any) -> list[SupportsPostCommit]:
    dependencies: list[SupportsPostCommit] = []
    for value in vars(owner).values():
        if value is owner:
            continue
        if isinstance(value, SupportsPostCommit):
            dependencies.append(value)
    return dependencies
