"""A unit of work run as the only participant of an invocation of its own."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from types import TracebackType
from typing import Self, cast

from loom.core.engine.post_commit import (
    PostCommitChannel,
    bind_channel,
    drain_after_failure,
    reset_channel,
)
from loom.core.persistence.stores import StoreKey
from loom.core.uow.abc import UnitOfWork
from loom.core.uow.coordinator import OrderedCommits
from loom.core.uow.scope import (
    Frame,
    InvocationScope,
    invocation,
    invocation_error,
    refuse_inside_open_scope,
)


@asynccontextmanager
async def single_store(
    key: StoreKey, unit_of_work: UnitOfWork, *, post_commit: PostCommitChannel | None = None
) -> AsyncIterator[InvocationScope]:
    """Run ``unit_of_work`` as the only participant of an invocation of its own.

    What ``async with uow:`` and ``@transactional`` do outside a scope: the
    repositories of ``key`` join the unit of work through the scope, and on
    exit the coordinator commits it, or rolls it back if the block raised.
    The post-commit actions queued in the block, a nested use case's
    included, run once the store has ended, by its outcome
    (:meth:`~loom.core.uow.scope.InvocationScope.settle_post_commit`).

    Args:
        key: Store the unit of work belongs to.
        unit_of_work: Not yet begun; it is begun on entry.
        post_commit: The channel bound in the block, whose actions run only
            if the invocation succeeded; a new one when omitted.

    Yields:
        The open scope.

    Raises:
        StoresNotCommitted: When the commit's result is unknown.
        PostCommitError: When a post-commit action failed after the commit.
        Exception: What the commit failed with, when the block did not raise.
    """
    coordinator = OrderedCommits({key: _Given(unit_of_work)})
    unowned, settled = post_commit or PostCommitChannel(), PostCommitChannel()
    error: BaseException | None = None
    token = bind_channel(unowned)
    try:
        async with invocation(coordinator) as scope:
            scope.post_commit = unowned
            async with scope.frame(Frame(key)):
                try:
                    yield scope
                except BaseException as exc:  # NOSONAR S5754 - raised below, once the scope ended
                    error = exc
            try:
                outcome = await coordinator.finish(scope, error)
            finally:
                scope.settle_post_commit(settled, error)
        failure = invocation_error(outcome, error or scope.poisoned)
    except BaseException as exc:  # NOSONAR S5754 - raised below, once the drain ran
        failure = exc
    finally:
        reset_channel(token)
    if failure is None:
        await settled.drain(committed=True)
        return
    await drain_after_failure(settled)
    raise failure


class EntersAlone:
    """Mixin: ``async with uow:`` runs the unit of work as the only participant of an invocation.

    What :func:`single_store` does, for a phased unit of work entered by
    hand. The class sets ``_store``, the key of its store, in ``__init__``.
    """

    _store: StoreKey
    _alone: AbstractAsyncContextManager[InvocationScope] | None = None

    async def __aenter__(self) -> Self:
        """Begin the unit of work as the only participant of an invocation of its own.

        Returns:
            This instance.

        Raises:
            UnitOfWorkInsideScope: Inside an open invocation scope, where
                this transaction would commit on its own, outside the invocation.
        """
        refuse_inside_open_scope(self)
        alone = single_store(self._store, cast("UnitOfWork", self))
        await alone.__aenter__()
        self._alone = alone
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """End the invocation ``__aenter__`` opened: commit on clean exit, roll back otherwise.

        The coordinator rolls back a failed commit, and the unit of work
        always closes; see :mod:`loom.core.uow.coordinator` for how
        cancellation is handled.
        """
        alone, self._alone = self._alone, None
        if alone is not None:
            traceback = exc_tb if isinstance(exc_tb, TracebackType) else None
            await alone.__aexit__(exc_type, exc_val, traceback)


@dataclass(frozen=True, slots=True)
class _Given:
    """A factory that hands out one unit of work already built."""

    unit_of_work: UnitOfWork

    def create(self) -> UnitOfWork:
        return self.unit_of_work
