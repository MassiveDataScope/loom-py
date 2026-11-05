from __future__ import annotations

import contextvars
from collections.abc import Awaitable, Callable
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

from loom.core.engine.post_commit import PostCommitChannel, bind_channel, reset_channel
from loom.core.errors.errors import ForeignSessionError
from loom.core.logger import get_logger
from loom.core.repository.mutation import MutationEvent
from loom.core.uow.scope import (
    InvocationScope,
    UndeclaredStoreWrite,
    current_scope,
    single_store,
)

if TYPE_CHECKING:  # session_manager is the heavier module; it does not import this one
    from loom.core.repository.sqlalchemy.session_manager import SessionManager


@runtime_checkable
class SupportsPostCommit(Protocol):
    """Protocol for objects that react to committed transactions."""

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None: ...


T = TypeVar("T")
P = ParamSpec("P")

_mutations: contextvars.ContextVar[list[MutationEvent] | None] = contextvars.ContextVar(
    "_mutations",
    default=None,
)
_log = get_logger(__name__).bind(component="transactional")

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

    Inside an open invocation scope (a use case the executor runs, an outer
    ``@transactional`` call) the method joins the participant of its owner's
    store and nothing else happens: the invocation commits it and runs the
    post-commit actions. Outside one, the method runs in an invocation of
    its owner's store alone, which commits on return and rolls back on any
    exception; it then runs ``on_transaction_committed(pending)`` on the
    owner and on its :class:`SupportsPostCommit` attributes through the
    post-commit channel (:mod:`loom.core.engine.post_commit`). The decorator
    owns that channel whenever it owns the invocation: a channel bound by an
    outer context is left untouched and restored afterwards, so a committed
    transaction always drains its own actions.

    Args:
        method: Async method of an object exposing ``session_manager``.

    Returns:
        The wrapped method.

    Raises:
        TypeError: If applied to a repository method or the owner has no
            ``session_manager`` with a ``session()`` context manager.
        ScopeOwnershipError: Inside a scope this task does not own (a
            ``gather`` child).
        ForeignSessionError: Inside a scope where the owner's store is
            enlisted with another session manager.
        UndeclaredStoreWrite: Inside a scope where the owner's store is not
            enlisted, or the running execution did not declare it.
        PostCommitError: If a hook failed after the commit.
    """

    @wraps(method)
    async def wrapper(self: Any, *args: Any, **kwargs: Any) -> T:
        _reject_repository_owner(self)
        session_manager = _require_session_manager(self)
        scope = current_scope()
        if scope is not None and not scope.closed:
            _require_enlisted(scope, session_manager)
            _log.debug(
                "TransactionalSessionReused",
                owner=self.__class__.__name__,
                method=method.__name__,
            )
            return await method(self, *args, **kwargs)

        channel = PostCommitChannel()
        channel_token = bind_channel(channel)
        try:
            result = await _run_in_own_invocation(
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


def _require_enlisted(scope: InvocationScope, session_manager: SessionManager) -> None:
    """Join the participant of ``session_manager``'s store, or refuse.

    Raises:
        ScopeOwnershipError: Outside the task that owns ``scope``.
        ForeignSessionError: When another session manager's unit of work
            holds the store.
        UndeclaredStoreWrite: When the store is not enlisted in ``scope``, or
            the running execution did not declare it.
    """
    scope.require_owner()
    # Without a frame the checkpoint refuses every write; a store not enlisted is named here.
    store = session_manager.store
    checked = scope.frames or store in scope.participants
    if not checked or scope.participant_for(store, session_manager, write=True) is None:
        enlisted = ", ".join(scope.participants)
        raise UndeclaredStoreWrite(
            f"{ForeignSessionError.side(session_manager.store, session_manager.target)} "
            "cannot join this invocation, "
            + (f"which enlisted only store {enlisted}" if enlisted else "with no store enlisted")
        )


def _reject_repository_owner(owner: Any) -> None:
    # Local import: ``repository`` imports this module for ``record_mutation``.
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


async def _run_in_own_invocation(
    owner: Any,
    method: Callable[..., Awaitable[T]],
    session_manager: SessionManager,
    channel: PostCommitChannel,
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> T:
    """Run ``method`` in an invocation of its store alone, then enqueue the hooks on ``channel``."""
    # Local import: ``uow`` imports this module for the mutation list.
    from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork

    try:
        async with single_store(session_manager.store, SQLAlchemyUnitOfWork(session_manager)):
            result = await method(owner, *args, **kwargs)
            pending = get_pending_mutations()
    except Exception:
        _log.exception(
            "TransactionRolledBack",
            owner=owner.__class__.__name__,
            method=method.__name__,
        )
        raise
    _log.info(
        "TransactionCommitted",
        owner=owner.__class__.__name__,
        method=method.__name__,
        mutation_count=len(pending),
    )
    _enqueue_post_commit_hooks(owner, pending, channel)
    return result


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
