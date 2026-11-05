from __future__ import annotations

from collections.abc import Awaitable, Callable
from functools import partial, wraps
from typing import (
    Any,
    Concatenate,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
    runtime_checkable,
)

from loom.core.engine.post_commit import PostCommitChannel, PostCommitError
from loom.core.errors.errors import ForeignSessionError
from loom.core.logger import get_logger
from loom.core.repository.mutation import MutationEvent
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.core.repository.sqlalchemy.uow import SQLAlchemyUnitOfWork
from loom.core.uow.alone import single_store
from loom.core.uow.scope import (
    InvocationScope,
    StoresNotCommitted,
    UndeclaredStoreWrite,
    current_scope,
)


@runtime_checkable
class SupportsPostCommit(Protocol):
    """Protocol for objects that react to committed transactions."""

    async def on_transaction_committed(self, events: tuple[MutationEvent, ...]) -> None: ...


T = TypeVar("T")
P = ParamSpec("P")

_log = get_logger(__name__).bind(component="transactional")


def record_mutation(event: MutationEvent) -> None:
    """Record ``event`` on the invocation running in this task, for the ``@transactional`` hooks.

    Discarded unless this task owns an invocation collecting them.

    Args:
        event: The mutation event to record.
    """
    scope = current_scope()
    if scope is not None and scope.mutations is not None and scope.owned_here():
        scope.mutations.append(event)


def transactional(
    method: Callable[Concatenate[Any, P], Awaitable[T]],
) -> Callable[Concatenate[Any, P], Awaitable[T]]:
    """Create a single transaction boundary for service/orchestrator use cases.

    Inside an open invocation scope (a use case the executor runs, an outer
    ``@transactional`` call) the method joins the participant of its owner's
    store and nothing else happens: the invocation commits it and runs the
    post-commit actions. Outside one, the method runs in an invocation of
    its owner's store alone (:func:`~loom.core.uow.alone.single_store`),
    which commits on return and rolls back on any exception; once it
    committed, ``on_transaction_committed(pending)`` runs on the owner and on
    its :class:`SupportsPostCommit` attributes, after the cache invalidations.

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

        return await _run_in_own_invocation(self, method, session_manager, args, kwargs)

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
    store = session_manager.store
    # Without a frame the checkpoint refuses every write; a store not enlisted is named here.
    if not (scope.frames or store in scope.participants):
        scope.require_owner()
    elif scope.participant_for(store, session_manager, write=True) is not None:
        return
    enlisted = ", ".join(scope.participants)
    raise UndeclaredStoreWrite(
        f"{ForeignSessionError.side(store, session_manager.target)} cannot join this invocation, "
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
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> T:
    """Run ``method`` in an invocation of its store alone; the hooks run only if it commits."""
    hooks = PostCommitChannel()
    unit_of_work = SQLAlchemyUnitOfWork(session_manager)
    try:
        async with single_store(session_manager.store, unit_of_work, post_commit=hooks) as scope:
            scope.mutations = []
            result = await method(owner, *args, **kwargs)
            pending = tuple(scope.mutations)
            _enqueue_post_commit_hooks(owner, pending, hooks)
    except (PostCommitError, StoresNotCommitted):
        raise
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
